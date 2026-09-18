import hmac
import hashlib
import json
import urllib.parse
import asyncio
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, CommandObject, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from motor.motor_asyncio import AsyncIOMotorClient
import config

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
mongo_client = AsyncIOMotorClient(config.MONGO_URI)
db = mongo_client["p2p_vault"]
users_col = db["users"]

# Memoria volátil de señalización: {room_id: {user_id: {"ws": WebSocketResponse, "name": str}}}
rooms: dict[str, dict[str, dict]] = {}


def validate_init_data(init_data: str, bot_token: str) -> dict | None:
    if not init_data:
        return None
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        if "hash" not in parsed:
            return None
        received_hash = parsed.pop("hash")
        check_str = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        calc_hash = hmac.new(secret_key, check_str.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(calc_hash, received_hash):
            return json.loads(parsed.get("user", "{}"))
        return None
    except Exception:
        return None


# --- Comandos aiogram 3 ---

@dp.message(CommandStart(deep_link=True))
async def cmd_start_deeplink(message: types.Message, command: CommandObject):
    room_code = command.args.strip().upper()
    direct_url = f"{config.WEBAPP_URL}?room={room_code}"
    
    kb = InlineKeyboardBuilder()
    kb.button(text=f"🚀 Unirse a la Sala {room_code}", web_app=types.WebAppInfo(url=direct_url))
    
    await message.answer(
        f"🔗 **Transferencia P2P Directa**\n\n"
        f"Sala asignada: `{room_code}`.\n\n"
        f"Pulsa el botón para conectarte de forma segura y directa:",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown"
    )


@dp.message(CommandStart())
async def cmd_start_default(message: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="⚡ Abrir Bóveda P2P", web_app=types.WebAppInfo(url=config.WEBAPP_URL))
    await message.answer(
        "🔒 **Bóveda Multimedia P2P**\n\n"
        "Transfiere fotos, videos y documentos sin límite de tamaño directamente entre dispositivos.\n"
        "Cero almacenamiento en servidores de Telegram o de la aplicación.",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown"
    )


@dp.message(Command("sala"))
async def cmd_join_room(message: types.Message, command: CommandObject):
    if not command.args:
        await message.answer("ℹ️ Uso: `/sala CODIGO`", parse_mode="Markdown")
        return
    room_code = command.args.strip().upper()
    direct_url = f"{config.WEBAPP_URL}?room={room_code}"
    kb = InlineKeyboardBuilder()
    kb.button(text=f"🔑 Entrar a Sala {room_code}", web_app=types.WebAppInfo(url=direct_url))
    await message.answer(f"Acceso a la sala `{room_code}`:", reply_markup=kb.as_markup(), parse_mode="Markdown")


# --- Servidor de Señalización WebSocket ---

async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=25.0)
    await ws.prepare(request)

    current_room = None
    current_user_id = None

    try:
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue

            payload = json.loads(msg.data)
            action = payload.get("action")

            if action == "join":
                init_data = payload.get("init_data")
                user = validate_init_data(init_data, config.BOT_TOKEN)
                current_user_id = str(user["id"]) if user else f"guest_{payload.get('user_seed', 'anon')}"
                current_room = payload.get("room_id", "").strip().upper()

                if not current_room:
                    continue

                if current_room not in rooms:
                    rooms[current_room] = {}

                rooms[current_room][current_user_id] = {
                    "ws": ws,
                    "name": user.get("first_name", "Usuario") if user else "Invitado"
                }

                await ws.send_json({"type": "joined_success", "room_id": current_room})

                for peer_id, peer_data in rooms[current_room].items():
                    if peer_id != current_user_id:
                        await peer_data["ws"].send_json({
                            "type": "peer_joined",
                            "peer_id": current_user_id,
                            "initiator": True
                        })
                        await ws.send_json({
                            "type": "peer_joined",
                            "peer_id": peer_id,
                            "initiator": False
                        })

            elif action in ("offer", "answer", "candidate"):
                target_id = payload.get("target_id")
                if current_room in rooms and target_id in rooms[current_room]:
                    target_ws = rooms[current_room][target_id]["ws"]
                    await target_ws.send_json({
                        "type": action,
                        "sender_id": current_user_id,
                        "data": payload.get("data")
                    })

    finally:
        if current_room and current_room in rooms:
            rooms[current_room].pop(current_user_id, None)
            if not rooms[current_room]:
                del rooms[current_room]
            else:
                for peer_data in rooms[current_room].values():
                    await peer_data["ws"].send_json({
                        "type": "peer_left",
                        "peer_id": current_user_id
                    })

    return ws


async def handle_transfer_complete(request: web.Request) -> web.Response:
    data = await request.json()
    sender_id = data.get("sender_id")
    file_id = data.get("file_id")

    if sender_id and file_id and not str(sender_id).startswith("guest_"):
        await users_col.find_one_and_update(
            {"telegram_id": int(sender_id)},
            {
                "$inc": {"reputation": 1, "completed_transfers": 1},
                "$addToSet": {"transferred_files": file_id}
            },
            upsert=True
        )
    return web.json_response({"status": "ok"})


async def index_handler(request: web.Request) -> web.FileResponse:
    return web.FileResponse("./public/index.html")


async def on_startup(app: web.Application):
    asyncio.create_task(dp.start_polling(bot))


async def on_cleanup(app: web.Application):
    await bot.session.close()
    mongo_client.close()


def create_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    app.router.add_get("/ws/signal", websocket_handler)
    app.router.add_post("/api/transfer-complete", handle_transfer_complete)
    app.router.add_get("/", index_handler)
    app.router.add_static("/", path="./public", name="public", show_index=False)
    return app


if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host=config.HOST, port=config.PORT)
