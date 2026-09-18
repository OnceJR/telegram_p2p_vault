import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "TU_TELEGRAM_BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://user:pass@cluster.mongodb.net/p2p_vault?retryWrites=true&w=majority")
PORT = int(os.getenv("PORT", 8080))
HOST = "0.0.0.0"
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://tu-app-en-render.onrender.com")
