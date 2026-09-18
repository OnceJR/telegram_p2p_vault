import { saveMedia, getMedia } from './indexedDB.js';

const CHUNK_SIZE = 16384; // 16 KB
const BUFFER_CEILING = 64 * 1024; // 64 KB de backpressure

export class P2PTransport {
  constructor(roomId, callbacks) {
    this.roomId = roomId;
    this.onStatus = callbacks.onStatus || (() => {});
    this.onProgress = callbacks.onProgress || (() => {});
    this.onReceived = callbacks.onReceived || (() => {});
    this.onReadyToSend = callbacks.onReadyToSend || (() => {});

    this.pc = null;
    this.dc = null;
    this.ws = null;
    this.targetPeerId = null;

    this.incomingMeta = null;
    this.receivedChunks = [];
    this.receivedSize = 0;
  }

  isChannelReady() {
    return this.dc && this.dc.readyState === 'open';
  }

  connect(wsUrl) {
    this.ws = new WebSocket(wsUrl);

    this.ws.onopen = () => {
      this.onStatus('Esperando par en la sala...', false);
      this.ws.send(JSON.stringify({
        action: 'join',
        room_id: this.roomId,
        init_data: window.Telegram?.WebApp?.initData || ''
      }));
    };

    this.ws.onmessage = async (event) => {
      const msg = JSON.parse(event.data);

      if (msg.type === 'peer_joined') {
        this.targetPeerId = msg.peer_id;
        this.onStatus('Par detectado. Negociando WebRTC...', false);
        this.initPeer(msg.initiator);
      } else if (msg.type === 'offer') {
        await this.handleOffer(msg.data, msg.sender_id);
      } else if (msg.type === 'answer') {
        await this.pc.setRemoteDescription(new RTCSessionDescription(msg.data));
      } else if (msg.type === 'candidate' && this.pc) {
        await this.pc.addIceCandidate(new RTCIceCandidate(msg.data));
      } else if (msg.type === 'peer_left') {
        this.onStatus('El par se ha desconectado.', false);
        this.cleanupPC();
      }
    };

    this.ws.onclose = () => this.onStatus('Desconectado', false);
  }

  initPeer(isInitiator) {
    this.pc = new RTCPeerConnection({
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
      ]
    });

    this.pc.onicecandidate = (e) => {
      if (e.candidate) {
        this.ws.send(JSON.stringify({
          action: 'candidate',
          target_id: this.targetPeerId,
          data: e.candidate
        }));
      }
    };

    if (isInitiator) {
      this.dc = this.pc.createDataChannel('p2p_channel', { ordered: true });
      this.bindDataChannel(this.dc);

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
        this.bindDataChannel(this.dc);
      };
    }
  }

  async handleOffer(offerData, senderId) {
    this.targetPeerId = senderId;
    this.initPeer(false);
    await this.pc.setRemoteDescription(new RTCSessionDescription(offerData));
    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);

    this.ws.send(JSON.stringify({
      action: 'answer',
      target_id: this.targetPeerId,
      data: answer
    }));
  }

  bindDataChannel(dc) {
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = BUFFER_CEILING;

    dc.onopen = () => {
      this.onStatus('P2P Conectado', true);
      this.onReadyToSend();
    };

    dc.onclose = () => this.onStatus('Canal P2P cerrado', false);

    dc.onmessage = async (e) => {
      if (typeof e.data === 'string') {
        const payload = JSON.parse(e.data);
        if (payload.event === 'START') {
          this.incomingMeta = payload.meta;
          this.receivedChunks = [];
          this.receivedSize = 0;
          this.onStatus(`Recibiendo: ${payload.meta.name}`, true);
        } else if (payload.event === 'COMPLETE') {
          const completeBlob = new Blob(this.receivedChunks, { type: this.incomingMeta.type });
          await saveMedia(this.incomingMeta.id, completeBlob, this.incomingMeta);

          // Notificar al backend para reputación
          fetch('/api/transfer-complete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              init_data: window.Telegram?.WebApp?.initData || '',
              sender_id: this.targetPeerId,
              file_id: this.incomingMeta.id
            })
          });

          this.onReceived(completeBlob, this.incomingMeta);
          this.onStatus('Completado', true);
        }
      } else {
        this.receivedChunks.push(e.data);
        this.receivedSize += e.data.byteLength;
        if (this.incomingMeta?.size) {
          this.onProgress((this.receivedSize / this.incomingMeta.size) * 100);
        }
      }
    };
  }

  async sendFile(fileId) {
    const record = await getMedia(fileId);
    if (!record || !this.isChannelReady()) return;

    const meta = {
      id: record.id,
      name: record.name,
      type: record.type,
      size: record.size
    };

    this.dc.send(JSON.stringify({ event: 'START', meta }));

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
        this.onProgress((offset / buffer.byteLength) * 100);
      }

      this.dc.send(JSON.stringify({ event: 'COMPLETE' }));
      this.onStatus('Enviado con éxito', true);
    };

    pump();
  }

  cleanupPC() {
    if (this.dc) this.dc.close();
    if (this.pc) this.pc.close();
    this.dc = null;
    this.pc = null;
  }
}
