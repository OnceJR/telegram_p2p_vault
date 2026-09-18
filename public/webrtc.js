import { saveMedia, getMedia } from './indexedDB.js';

const CHUNK_SIZE = 16384;
const BUFFER_CEILING = 64 * 1024;

export class P2PTransport {
  constructor(callbacks) {
    this.roomId = null;
    this.callbacks = callbacks;

    this.pc = null;
    this.dc = null;
    this.ws = null;
    this.targetPeerId = null;
    this.iceCandidateQueue = [];

    this.incomingMeta = null;
    this.receivedChunks = [];
    this.receivedSize = 0;
    this.userSeed = Math.random().toString(36).substring(2, 9);
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
        this.callbacks.onStatus?.('Par encontrado. Negociando WebRTC...', false);
        this.initPeer(msg.initiator);
      } else if (msg.type === 'offer') {
        await this.handleOffer(msg.data, msg.sender_id);
      } else if (msg.type === 'answer') {
        await this.handleAnswer(msg.data);
      } else if (msg.type === 'candidate') {
        await this.handleCandidate(msg.data);
      } else if (msg.type === 'peer_left') {
        this.callbacks.onStatus?.('El otro dispositivo se desconectó.', false);
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
      this.dc = this.pc.createDataChannel('p2p_transfer', { ordered: true });
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
      const candidate = this.iceCandidateQueue.shift();
      try {
        await this.pc.addIceCandidate(candidate);
      } catch (err) {
        console.error('Error aplicando ICE candidate de cola:', err);
      }
    }
  }

  setupDataChannel(dc) {
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = BUFFER_CEILING;

    dc.onopen = () => {
      this.callbacks.onStatus?.('⚡ Conectado directo P2P', true);
      this.callbacks.onChannelReady?.();
    };

    dc.onclose = () => this.callbacks.onStatus?.('Canal directo cerrado.', false);

    dc.onmessage = async (e) => {
      if (typeof e.data === 'string') {
        const payload = JSON.parse(e.data);
        if (payload.event === 'START') {
          this.incomingMeta = payload.meta;
          this.receivedChunks = [];
          this.receivedSize = 0;
          this.callbacks.onStatus?.(`Recibiendo: ${payload.meta.name}...`, true);
        } else if (payload.event === 'COMPLETE') {
          const blob = new Blob(this.receivedChunks, { type: this.incomingMeta.type });
          await saveMedia(this.incomingMeta.id, blob, this.incomingMeta);

          fetch('/api/transfer-complete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              init_data: window.Telegram?.WebApp?.initData || '',
              sender_id: this.targetPeerId,
              file_id: this.incomingMeta.id
            })
          });

          this.callbacks.onReceived?.(blob, this.incomingMeta);
          this.callbacks.onStatus?.('✅ Archivo recibido correctamente', true);
        }
      } else {
        this.receivedChunks.push(e.data);
        this.receivedSize += e.data.byteLength;
        if (this.incomingMeta?.size) {
          this.callbacks.onProgress?.((this.receivedSize / this.incomingMeta.size) * 100);
        }
      }
    };
  }

  async sendFile(fileId) {
    const record = await getMedia(fileId);
    if (!record || !this.isChannelReady()) return;

    this.dc.send(JSON.stringify({
      event: 'START',
      meta: { id: record.id, name: record.name, type: record.type, size: record.size }
    }));

    const buffer = await record.blob.arrayBuffer();
    let offset = 0;

    const pump = () => {
      while (offset < buffer.byteLength) {
        if (this.dc.bufferedAmount > BUFFER_CEILING) {
          this.dc.onbufferedamountlow = () => {
            this.dc.onbufferedamountlow = null;
            pump();
          };
          return;
        }

        const chunk = buffer.slice(offset, offset + CHUNK_SIZE);
        this.dc.send(chunk);
        offset += chunk.byteLength;
        this.callbacks.onProgress?.((offset / buffer.byteLength) * 100);
      }

      this.dc.send(JSON.stringify({ event: 'COMPLETE' }));
      this.callbacks.onStatus?.('✅ Archivo enviado con éxito', true);
    };

    pump();
  }

  cleanupPeerConnection() {
    if (this.dc) { try { this.dc.close(); } catch(e) {} this.dc = null; }
    if (this.pc) { try { this.pc.close(); } catch(e) {} this.pc = null; }
    this.iceCandidateQueue = [];
  }

  cleanup() {
    this.cleanupPeerConnection();
  }
}
