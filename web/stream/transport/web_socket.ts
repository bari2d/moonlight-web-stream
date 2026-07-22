import { TransportChannelId } from "../../api_bindings.js";
import { ByteBuffer } from "../buffer.js";
import { Logger } from "../log.js";
import { StatValue } from "../stats.js";
import { allVideoCodecs, VideoCodecSupport } from "../video.js";
import { DataTransportChannel, Transport, TransportAudioSetup, TransportChannel, TransportChannelIdKey, TransportChannelIdValue, TransportShutdown, TransportVideoSetup } from "./index.js";

// WebSocket is a reliable ordered stream, so allowing a large browser-side
// send buffer only converts congestion into delayed input replay.
const WEBSOCKET_SEND_BUFFER_HIGH_WATER_MARK = 64 * 1024
// A reliable input transition must never be silently discarded. If the
// browser has accumulated this much outgoing data, tear down the stream and
// let Stream establish a fresh control socket instead of risking stuck keys or
// buttons. Replaceable snapshots are shed much earlier at the high-water mark.
const WEBSOCKET_SEND_BUFFER_FATAL_LIMIT = 256 * 1024

type ChannelSender = (id: TransportChannelIdValue, message: ArrayBuffer) => void
type BufferedBytesReader = () => number | null

export class WebSocketTransport implements Transport {
    readonly implementationName: string = "web_socket"

    private logger: Logger | null = null
    private ws: WebSocket
    private active = true
    private sendBlockedLogged = false

    private channels: Array<WebSocketDataTransportChannel> = []

    private readonly wsMessageListener = (event: MessageEvent) => this.onWsMessage(event)
    private readonly wsCloseListener = (event: CloseEvent) => this.onWsClose(event)
    private readonly channelSender: ChannelSender = (id, message) => this.sendChannelMessage(id, message)
    private readonly bufferedBytesReader: BufferedBytesReader = () => this.active ? this.ws.bufferedAmount : null

    constructor(ws: WebSocket, _buffer: ByteBuffer, logger: Logger | null) {
        if (logger) {
            this.logger = logger
        }

        this.ws = ws

        // Very important, set the binary type to arraybuffer.
        this.ws.binaryType = "arraybuffer"
        this.ws.addEventListener("message", this.wsMessageListener)
        this.ws.addEventListener("close", this.wsCloseListener)

        for (const keyRaw in TransportChannelId) {
            const key = keyRaw as TransportChannelIdKey
            const id = TransportChannelId[key]

            this.channels[id] = new WebSocketDataTransportChannel(
                id,
                this.channelSender,
                this.bufferedBytesReader,
            )
        }
    }

    getChannel(id: TransportChannelIdValue): TransportChannel {
        return this.channels[id]
    }

    async setupHostVideo(setup: TransportVideoSetup): Promise<VideoCodecSupport> {
        if (setup.type.indexOf("data") == -1) {
            this.logger?.debug("Cannot use Web Socket Transport: Found no supported video pipeline")
            throw "Cannot use Web Socket Transport: Found no supported video pipeline"
        }

        return allVideoCodecs()
    }
    async setupHostAudio(setup: TransportAudioSetup): Promise<void> {
        if (setup.type.indexOf("data") == -1) {
            this.logger?.debug("Cannot use Web Socket Transport: Found no supported audio pipeline")
            throw "Cannot use Web Socket Transport: Found no supported audio pipeline"
        }
    }

    onclose: ((shutdown: TransportShutdown) => void) | null = null

    private onWsMessage(event: MessageEvent) {
        if (!this.active) {
            return
        }

        const data = event.data
        if (!(data instanceof ArrayBuffer) || data.byteLength < 1) {
            return
        }

        const id = new Uint8Array(data, 0, 1)[0] as TransportChannelIdValue
        // Channel IDs are dense, so this is an O(1) lookup. The selected
        // channel performs the sole payload copy only when it has listeners.
        this.channels[id]?.receiveFrame(data)
    }

    private onWsClose(event: CloseEvent) {
        if (!this.active) {
            return
        }

        this.detachWsListeners()
        if (this.onclose) {
            this.onclose(event.wasClean ? "disconnect" : "failed")
        }
    }

    private sendChannelMessage(id: TransportChannelIdValue, message: ArrayBuffer): void {
        if (!this.active) {
            return
        }
        if (this.ws.readyState != WebSocket.OPEN) {
            this.failTransport("control socket is not open")
            return
        }

        const frameBytes = message.byteLength + 1
        const projectedBufferedAmount = this.ws.bufferedAmount + frameBytes
        if (!Number.isSafeInteger(frameBytes) || projectedBufferedAmount > WEBSOCKET_SEND_BUFFER_FATAL_LIMIT) {
            this.failTransport("outgoing reliable buffer exceeded its safety limit")
            return
        }

        if (
            projectedBufferedAmount > WEBSOCKET_SEND_BUFFER_HIGH_WATER_MARK &&
            this.isReplaceableSnapshot(id, message)
        ) {
            // Do not retain an application queue here: replaying old input
            // after congestion clears is worse than dropping it at the edge.
            this.logBlockedSendOnce("send buffer reached its bound")
            return
        }

        let frame: Uint8Array
        try {
            frame = new Uint8Array(frameBytes)
            frame[0] = id
            frame.set(new Uint8Array(message), 1)
            this.ws.send(frame.buffer)
            this.sendBlockedLogged = false
        } catch (_error) {
            // readyState may race with send(). A reliable transition may be in
            // this frame, so replace the session instead of silently losing it.
            this.failTransport("control socket rejected an outgoing frame")
        }
    }

    private isReplaceableSnapshot(id: TransportChannelIdValue, message: ArrayBuffer): boolean {
        if (
            id == TransportChannelId.MOUSE_ABSOLUTE ||
            (id >= TransportChannelId.CONTROLLER0 && id <= TransportChannelId.CONTROLLER15)
        ) {
            return true
        }

        // Touch move packets are replaceable while down/up/cancel packets must
        // retain their reliable ordered semantics.
        return id == TransportChannelId.TOUCH &&
            message.byteLength > 0 &&
            new Uint8Array(message, 0, 1)[0] == 1
    }

    private failTransport(reason: string): void {
        if (!this.active) {
            return
        }

        this.logger?.debug(`Web Socket transport failed: ${reason}`)
        this.detachWsListeners()
        try {
            // This WebSocket also owns the authenticated control session. It
            // must be replaced together with the binary transport so the
            // server cannot continue an input stream whose reliable tail was
            // rejected locally.
            this.ws.close(1011, "transport send buffer exceeded")
        } catch (_error) { }
        this.onclose?.("failed")
    }

    private logBlockedSendOnce(reason: string): void {
        if (this.sendBlockedLogged) {
            return
        }
        this.sendBlockedLogged = true
        this.logger?.debug(`Web Socket transport ${reason}; dropping outgoing data`)
    }

    private detachWsListeners(): void {
        if (!this.active) {
            return
        }
        this.active = false
        this.ws.removeEventListener("message", this.wsMessageListener)
        this.ws.removeEventListener("close", this.wsCloseListener)
    }

    async close(): Promise<void> {
        // We do not own this socket, but we do own our listeners and must stop
        // old channel objects from sending after a transport replacement.
        this.detachWsListeners()
        this.logger?.debug("Web Socket transport close called, leaving the shared Web Socket open")
    }
    async getStats(): Promise<Record<string, StatValue>> {
        return {
            websocketBufferedBytes: this.ws.bufferedAmount,
        }
    }
}

class WebSocketDataTransportChannel implements DataTransportChannel {
    readonly type: "data" = "data"

    private id: TransportChannelIdValue
    private sendFrame: ChannelSender
    private readBufferedBytes: BufferedBytesReader

    constructor(id: TransportChannelIdValue, sendFrame: ChannelSender, readBufferedBytes: BufferedBytesReader) {
        this.id = id
        this.sendFrame = sendFrame
        this.readBufferedBytes = readBufferedBytes
    }

    canReceive: boolean = true
    canSend: boolean = true

    private receiveListeners: Array<(data: ArrayBuffer) => void> = []
    addReceiveListener(listener: (data: ArrayBuffer) => void): void {
        this.receiveListeners.push(listener)
    }
    removeReceiveListener(listener: (data: ArrayBuffer) => void): void {
        const index = this.receiveListeners.indexOf(listener)
        if (index != -1) {
            this.receiveListeners.splice(index, 1)
        }
    }

    receiveFrame(frame: ArrayBuffer): void {
        if (this.receiveListeners.length == 0) {
            return
        }

        const payload = frame.slice(1)
        for (const listener of this.receiveListeners) {
            listener(payload)
        }
    }

    send(message: ArrayBuffer): void {
        this.sendFrame(this.id, message)
    }

    estimatedBufferedBytes(): number | null {
        return this.readBufferedBytes()
    }
}
