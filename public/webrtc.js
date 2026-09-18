const CHUNK_SIZE = 16384; // 16 KB (tamaño estándar SCTP)
const BUFFER_CEILING = 1024 * 1024; // 1 MB backpressure threshold

export class P2PTransport {
  constructor(callbacks) {
    this.roomId = null;
    this.callbacks = callbacks;

    this.pc = null;
    this.dc = null;
    this.ws = null;
    this.targetPeerId = null;
    this.iceCandidateQueue = [];

    this.userSeed = Math.random().toString(36).substring(2, 9);

    // Sistema de escritura en streaming
    this.incomingMeta = null;
    this.opfsFileHandle = null;
    this.opfsWritable = null;
    this.receivedBytes = 0;
    this.fallbackChunks = [];
  }

  isChannelReady() {
    return this.dc && this.dc.readyState === 'open';
  }

  connectSignaling(wsUrl, roomId) {
    this.roomId = roomId.trim().toUpperCase();
    if (this.ws) {
      this.ws.close();
    }

    this.ws = new WebSocket(wsUrl);

    this.ws.onopen = () => {
      this.callbacks.onStatus?.('Conectando a la sala...', false);
      this.ws.send(JSON.stringify({
        action: 'join',
        room_id: this.roomId,
        user_seed: this.userSeed,
        init_data: window.Telegram?.WebApp?.initData || ''
      }));
    };

    this.ws.onmessage = async (event) => {
      const msg = JSON.parse(event.data);

      if (msg.type === 'joined_success') {
        this.callbacks.onRoomJoined?.(msg.room_id);
      } else if (msg.type === 'peer_joined') {
        this.targetPeerId = msg.peer_id;
        this.callbacks.onStatus?.('Dispositivo detectado. Negociando conexión...', false);
        this.initPeer(msg.initiator);
      } else if (msg.type === 'offer') {
        await this.handleOffer(msg.data, msg.sender_id);
      } else if (msg.type === 'answer') {
        await this.handleAnswer(msg.data);
      } else if (msg.type === 'candidate') {
        await this.handleCandidate(msg.data);
      } else if (msg.type === 'peer_left') {
        this.callbacks.onStatus?.('El otro dispositivo se ha desconectado.', false);
        this.cleanup();
      }
    };

    this.ws.onclose = () => this.callbacks.onStatus?.('Desconectado de señalización.', false);
  }

  initPeer(isInitiator) {
    this.cleanupPeerConnection();

    this.pc = new RTCPeerConnection({
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
      ]
    });

    this.pc.onicecandidate = (e) => {
      if (e.candidate && this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({
          action: 'candidate',
          target_id: this.targetPeerId,
          data: e.candidate
        }));
      }
    };

    if (isInitiator) {
      this.dc = this.pc.createDataChannel('p2p_channel', { ordered: true });
      this.setupDataChannel(this.dc);

      this.pc.createOffer()
        .then((offer) => this.pc.setLocalDescription(offer))
        .then(() => {
          this.ws.send(JSON.stringify({
            action: 'offer',
            target_id: this.targetPeerId,
            data: this.pc.localDescription
          }));
        });
    } else {
      this.pc.ondatachannel = (e) => {
        this.dc = e.channel;
        this.setupDataChannel(this.dc);
      };
    }
  }

  async handleOffer(offerData, senderId) {
    this.targetPeerId = senderId;
    this.initPeer(false);
    await this.pc.setRemoteDescription(new RTCSessionDescription(offerData));
    await this.processIceQueue();

    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);

    this.ws.send(JSON.stringify({
      action: 'answer',
      target_id: this.targetPeerId,
      data: answer
    }));
  }

  async handleAnswer(answerData) {
    await this.pc.setRemoteDescription(new RTCSessionDescription(answerData));
    await this.processIceQueue();
  }

  async handleCandidate(candidateData) {
    const candidate = new RTCIceCandidate(candidateData);
    if (!this.pc || !this.pc.remoteDescription) {
      this.iceCandidateQueue.push(candidate);
    } else {
      await this.pc.addIceCandidate(candidate);
    }
  }

  async processIceQueue() {
    while (this.iceCandidateQueue.length > 0) {
      const cand = this.iceCandidateQueue.shift();
      try {
        await this.pc.addIceCandidate(cand);
      } catch (e) {
        console.error('Error aplicando ICE candidate:', e);
      }
    }
  }

  setupDataChannel(dc) {
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = BUFFER_CEILING / 2;

    dc.onopen = () => {
      this.callbacks.onStatus?.('⚡ Enlace P2P Directo Establecido', true);
      this.callbacks.onChannelReady?.();
    };

    dc.onclose = () => this.callbacks.onStatus?.('Canal directo cerrado.', false);

    dc.onmessage = async (e) => {
      if (typeof e.data === 'string') {
        const payload = JSON.parse(e.data);

        if (payload.event === 'START') {
          this.incomingMeta = payload.meta;
          this.receivedBytes = 0;
          this.fallbackChunks = [];

          // Inicializar escritura en disco mediante OPFS para uso mínimo de RAM
          if (navigator.storage && navigator.storage.getDirectory) {
            try {
              const root = await navigator.storage.getDirectory();
              this.opfsFileHandle = await root.getFileHandle(`recv_${this.incomingMeta.id}.tmp`, { create: true });
              this.opfsWritable = await this.opfsFileHandle.createWritable();
            } catch (err) {
              console.warn('OPFS no disponible, usando fallback en memoria:', err);
              this.opfsWritable = null;
            }
          }

          this.callbacks.onStatus?.(`Recibiendo: ${payload.meta.name}`, true);
        } else if (payload.event === 'COMPLETE') {
          let finalFile = null;

          if (this.opfsWritable) {
            await this.opfsWritable.close();
            const diskFile = await this.opfsFileHandle.getFile();
            finalFile = new File([diskFile], this.incomingMeta.name, { type: this.incomingMeta.type });
          } else {
            const blob = new Blob(this.fallbackChunks, { type: this.incomingMeta.type });
            finalFile = new File([blob], this.incomingMeta.name, { type: this.incomingMeta.type });
            this.fallbackChunks = [];
          }

          // Notificar reputación al backend
          fetch('/api/transfer-complete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              init_data: window.Telegram?.WebApp?.initData || '',
              sender_id: this.targetPeerId,
              file_id: this.incomingMeta.id
            })
          }).catch(() => {});

          this.callbacks.onReceived?.(finalFile, this.incomingMeta);
          this.callbacks.onStatus?.('✅ Transferencia Completa', true);
        }
      } else {
        // Recepción del fragmento binario
        if (this.opfsWritable) {
          await this.opfsWritable.write(e.data);
        } else {
          this.fallbackChunks.push(e.data);
        }

        this.receivedBytes += e.data.byteLength;
        if (this.incomingMeta?.size) {
          this.callbacks.onProgress?.((this.receivedBytes / this.incomingMeta.size) * 100);
        }
      }
    };
  }

  // Envío en streaming: Lectura fraccionada con slice() para archivos de cualquier tamaño
  async sendFileStream(file, onProgress) {
    if (!this.isChannelReady()) return;

    const meta = {
      id: crypto.randomUUID(),
      name: file.name,
      type: file.type || 'application/octet-stream',
      size: file.size
    };

    this.dc.send(JSON.stringify({ event: 'START', meta }));

    let offset = 0;
    const totalSize = file.size;

    const pump = async () => {
      while (offset < totalSize) {
        // Control de flujo: si el buffer está lleno, pausar y esperar evento
        if (this.dc.bufferedAmount > BUFFER_CEILING) {
          this.dc.onbufferedamountlow = () => {
            this.dc.onbufferedamountlow = null;
            pump();
          };
          return;
        }

        // Cortar y leer únicamente 16 KB en RAM por iteración
        const slice = file.slice(offset, offset + CHUNK_SIZE);
        const chunkBuffer = await slice.arrayBuffer();

        this.dc.send(chunkBuffer);
        offset += chunkBuffer.byteLength;

        onProgress?.((offset / totalSize) * 100);
      }

      this.dc.send(JSON.stringify({ event: 'COMPLETE' }));
      this.callbacks.onStatus?.('✅ Archivo enviado con éxito', true);
    };

    await pump();
  }

  cleanupPeerConnection() {
    if (this.dc) { try { this.dc.close(); } catch (e) {} this.dc = null; }
    if (this.pc) { try { this.pc.close(); } catch (e) {} this.pc = null; }
    this.iceCandidateQueue = [];
  }

  cleanup() {
    this.cleanupPeerConnection();
  }
}
