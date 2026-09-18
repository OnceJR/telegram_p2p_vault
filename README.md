# Telegram P2P Media Vault

Sistema de intercambio multimedia Peer-to-Peer (WebRTC DataChannel) para Telegram Mini Apps con backend en Python (aiogram 3 + aiohttp + motor).

## Características
- **Cero multimedia en servidores:** Los archivos nunca se envían al bot ni al servidor web, eliminando riesgos de ToS y costos de almacenamiento.
- **Persistencia local (IndexedDB):** Los archivos residen de forma privada en el almacenamiento del cliente.
- **Señalización WebSockets en aiohttp:** Emparejamiento de salas y traspaso de SDP/ICE candidates con validación criptográfica HMAC-SHA256 (`initData`).
- **Control de backpressure:** Chunks de 16 KB con monitoreo de `bufferedAmount` para transferencias fluidas sin saturación de memoria.

## Despliegue en Render
1. Conecta este repositorio en **Render** como un **Web Service**.
2. **Build Command:** `pip install -r requirements.txt`
3. **Start Command:** `python server.py`
4. Configura las variables de entorno (`BOT_TOKEN`, `MONGO_URI`, `WEBAPP_URL`).
