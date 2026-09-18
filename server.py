import hmac
import hashlib
import json
import urllib.parse
import asyncio
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
from motor.motor_asyncio import AsyncIOMotorClient
import config

# Inicialización de servicios
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
mongo_client = AsyncIOMotorClient(config.MONGO_URI)
db = mongo_client["p2p_vault"]
users_col = db["users"]

# Memoria volátil de señalización: {room_id: {user_id: {"ws": WebSocketResponse, "first_name": str}}}
rooms: dict[str, dict[str, dict]] = {}


def validate_init_data(init_data: str, bot_token: str) -> dict | None:
    """Valida la firma HMAC-SHA256 generada por Telegram WebApp."""
    if not init_data:
        return None
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        if "hash" not in parsed:
            return None
        
        received_hash = parsed.pop("hash")
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        if hmac.compare_digest(calculated_hash, received_hash):
            return json.loads(parsed.get("user", "{}"))
        return None
    except Exception:
        return None


# --- Rutas de aiogram 3 ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await users_col.update_one(
        {"telegram_id": message.from_user.id},
        {"$setOnInsert": {"telegram_id": message.from_user.id, "reputation": 0}},
        upsert=True
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(
        text="⚡ Abrir Bóveda P2P",
        web_app=types.WebAppInfo(url=config.WEBAPP_URL)
    )
    await message.answer(
        "🔒 **Bóveda Multimedia P2P**\n\n"
        "Transfiere fotos y videos encriptados directamente entre dispositivos sin pasar por los servidores de Telegram.\n\n"
        "Pulsa el botón de abajo para empezar:",
        reply_markup=kb.as_markup(),
        parse_mode="Markdown"
    )


# --- Handlers de aiohttp: Señalización y API ---

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
                if not user:
                    await ws.send_json({"type": "error", "message": "Firma inválida"})
                    await ws.close()
                    return ws

                current_user_id = str(user["id"])
                current_room = payload.get("room_id")

                if current_room not in rooms:
                    rooms[current_room] = {}

                rooms[current_room][current_user_id] = {
                    "ws": ws,
                    "first_name": user.get("first_name", "Usuario")
                }

                # Notificar a los integrantes de la sala
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
    user = validate_init_data(data.get("init_data"), config.BOT_TOKEN)
    if not user:
        return web.json_response({"error": "No autorizado"}, status=401)

    sender_id = data.get("sender_id")
    file_id = data.get("file_id")

    if sender_id and file_id:
        await users_col.find_one_and_update(
            {"telegram_id": int(sender_id)},
            {
                "$inc": {"reputation": 1, "completed_transfers": 1},
                "$addToSet": {"transferred_files": file_id}
            },
            upsert=True
        )

    return web.json_response({"status": "ok"})


async def on_startup(app: web.Application):
    asyncio.create_task(dp.start_polling(bot))


async def on_cleanup(app: web.Application):
    await bot.session.close()
    mongo_client.close()


async def index_handler(request: web.Request) -> web.FileResponse:
    """Sirve directamente el archivo HTML principal al entrar a la raíz."""
    return web.FileResponse("./public/index.html")


def create_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # 1. Rutas API y WebSockets
    app.router.add_get("/ws/signal", websocket_handler)
    app.router.add_post("/api/transfer-complete", handle_transfer_complete)

    # 2. Servir index.html de forma explícita en la raíz "/" (Debe ir ANTES de add_static)
    app.router.add_get("/", index_handler)

    # 3. Servir el resto de archivos estáticos (JS, CSS, etc.) sin listar carpetas
    app.router.add_static("/", path="./public", name="public", show_index=False)
    
    return app


if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host=config.HOST, port=config.PORT)
