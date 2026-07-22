import { TransportChannelId } from "../../api_bindings.js"
import { Logger } from "../log.js"
import { StatValue } from "../stats.js"
import { allVideoCodecs, VideoCodecSupport } from "../video.js"
import {
    DataTransportChannel,
    Transport,
    TransportAudioSetup,
    TransportChannel,
    TransportChannelIdKey,
    TransportChannelIdValue,
    TransportShutdown,
    TransportVideoSetup,
} from "./index.js"

const DEFAULT_CONNECT_TIMEOUT_MS = 3000
const MAX_RELIABLE_FRAME_BYTES = 128 * 1024
const MAX_RELIABLE_QUEUED_BYTES = 128 * 1024
const MAX_RELIABLE_QUEUED_FRAMES = 64
const COMBINED_RELIABLE_WRITE_MAX_BYTES = 4 * 1024
const MAX_MEDIA_FRAME_BYTES = 16 * 1024 * 1024
const MAX_INCOMING_STREAM_TASKS = 16
const MAX_INCOMING_ALLOCATED_BYTES = 64 * 1024 * 1024
const MAX_PENDING_VIDEO_FRAMES = 4
const MAX_PENDING_VIDEO_BYTES = 32 * 1024 * 1024
const MAX_VIDEO_SEQUENCE_GAP = 32
const VIDEO_REORDER_DEADLINE_MS = 50
const VIDEO_RECOVERY_RETRY_MS = 250
const CLOSE_DRAIN_TIMEOUT_MS = 250
const PROTOCOL_VERSION = "3"

const LANE_VIDEO_FRAME = 1
const LANE_AUDIO = 2
const LANE_OTHER = 3
const LANE_CLIENT_RELIABLE = 4
const DATAGRAM_MAGIC = 0xf0
const DATAGRAM_SNAPSHOT = 2
const DATAGRAM_SNAPSHOT_HEADER_BYTES = 9

type Lifecycle = "new" | "connecting" | "connected" | "closing" | "closed" | "failed"
type ChannelSender = (id: TransportChannelIdValue, message: ArrayBuffer) => void
type BufferedBytesReader = () => number | null

type ReliableFrame = {
    id: number
    payload: ArrayBuffer
    bufferedBytes: number
    enqueuedAt: number
    snapshotId: TransportChannelIdValue | null
}

type PendingDatagram = {
    data: Uint8Array
    id: TransportChannelIdValue
    sequence: number
    payload: ArrayBuffer
}

type PendingVideoFrame = {
    id: number
    payload: ArrayBuffer
    bufferedBytes: number
}

/**
 * WebTransport protocol v3.
 *
 * Server video uses one independently resettable unidirectional stream per
 * frame: lane byte 1, u32-BE wrapping sequence, u32-BE frame length, then the
 * existing channel-prefixed frame. Audio (lane 2) and other reliable data
 * (lane 3) remain ordered persistent lanes with repeated length-prefixed
 * frames. The client opens lane 4. Relative motion and clicks share that lane
 * so input barriers retain their ordering. Replaceable controller snapshots
 * use QUIC datagrams beginning with DATAGRAM_MAGIC when the browser exposes
 * them. Absolute mouse snapshots stay on lane 4 so they cannot overtake the
 * reliable position + click barrier used by point-and-drag input.
 */
export class WebTransportTransport implements Transport {
    readonly implementationName = "web_transport"

    readonly url: string
    private readonly logger: Logger | null
    private lifecycle: Lifecycle = "new"
    private transport: WebTransport | null = null

    private readonly channels: Array<WebTransportDataTransportChannel> = []
    private readonly channelSender: ChannelSender = (id, message) => this.sendChannelMessage(id, message)
    private readonly bufferedBytesReader: BufferedBytesReader = () => this.bufferedBytes()

    private incomingStreamsReader: ReadableStreamDefaultReader<ReadableStream<Uint8Array>> | null = null
    private readonly incomingLaneReaders = new Set<ReadableStreamDefaultReader<Uint8Array>>()
    private readonly activeVideoReaders = new Map<number, ReadableStreamDefaultReader<Uint8Array>>()
    private reliableWriter: WritableStreamDefaultWriter<Uint8Array> | null = null
    private datagramWriter: WritableStreamDefaultWriter<Uint8Array> | null = null

    private readonly reliableQueue: Array<ReliableFrame> = []
    private readonly pendingReliableSnapshots = new Map<TransportChannelIdValue, ReliableFrame>()
    private reliableQueuedBytes = 0
    private reliableInFlightBytes = 0
    private reliablePumpRunning = false

    private incomingAllocatedBytes = 0
    private maxIncomingAllocatedBytes = 0
    private readonly pendingVideoFrames = new Map<number, PendingVideoFrame>()
    private pendingVideoBytes = 0
    private videoExpectedSequence: number | null = null
    private videoLastDeliveredSequence: number | null = null
    private videoAwaitingIdr = true
    private videoIdrRequested = false
    private videoReorderTimer: number | null = null
    private videoReorderExpectedSequence: number | null = null
    private videoRecoveryRetryTimer: number | null = null

    // Each key contains only the newest cumulative value or snapshot. No old
    // replaceable state is replayed after congestion clears.
    private readonly pendingDatagrams = new Map<number, PendingDatagram>()
    private datagramPumpRunning = false
    private epoch = 0
    private readonly snapshotSequences = new Map<number, number>()

    private reliableFramesSent = 0
    private reliableBytesSent = 0
    private reliableQueueOverflows = 0
    private reliableSnapshotsReplaced = 0
    private reliableMotionCoalesced = 0
    private maxReliableQueueAgeMs = 0
    private datagramsSent = 0
    private datagramBytesSent = 0
    private datagramsReplaced = 0
    private datagramFallbacks = 0
    private incomingFrames = 0
    private incomingBytes = 0
    private incomingStreams = 0
    private incomingStreamErrors = 0
    private incomingStreamBackpressureEvents = 0
    private videoFramesReceived = 0
    private videoFramesDropped = 0
    private videoFramesReordered = 0
    private videoReorderTimeouts = 0
    private videoRecoveryRequests = 0
    private maxReliableQueuedBytes = 0
    private closeNotified = false
    private blockedSendLogged = false
    private datagramMode = "uninitialized"

    onclose: ((shutdown: TransportShutdown) => void) | null = null

    constructor(url: string | URL, logger?: Logger) {
        this.url = url.toString()
        this.logger = logger ?? null

        for (const keyRaw in TransportChannelId) {
            const key = keyRaw as TransportChannelIdKey
            const id = TransportChannelId[key]
            this.channels[id] = new WebTransportDataTransportChannel(
                id,
                this.channelSender,
                this.bufferedBytesReader,
            )
        }
    }

    static isSupported(): boolean {
        return window.isSecureContext && "WebTransport" in window && typeof WebTransport == "function"
    }

    async connect(timeoutMs: number = DEFAULT_CONNECT_TIMEOUT_MS): Promise<void> {
        if (this.lifecycle != "new") {
            throw new Error(`Cannot connect WebTransport while it is ${this.lifecycle}`)
        }
        if (!WebTransportTransport.isSupported()) {
            throw new Error("WebTransport requires a secure context and browser WebTransport support")
        }
        if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
            throw new Error("WebTransport connection timeout must be greater than zero")
        }
        if (new URL(this.url).searchParams.get("v") != PROTOCOL_VERSION) {
            throw new Error("Unsupported Moonlight WebTransport protocol version")
        }

        this.lifecycle = "connecting"
        this.epoch = this.newEpoch()

        let transport: WebTransport
        let connectStage = "construct"
        try {
            transport = new WebTransport(this.url)
            this.transport = transport
            connectStage = "ready"
            await this.withTimeout(transport.ready, timeoutMs, "WebTransport connection timed out")

            connectStage = "datagrams"
            try {
                const datagrams = transport.datagrams
                if (datagrams?.writable) {
                    // Keep the browser's hidden datagram queue as shallow and
                    // fresh as possible; our pending map already preserves the
                    // latest state.
                    try {
                        datagrams.outgoingHighWaterMark = 1
                        datagrams.outgoingMaxAge = 15
                        datagrams.incomingHighWaterMark = 1
                        datagrams.incomingMaxAge = 15
                    } catch (_error) {
                        // These tuning setters are optional in older implementations.
                    }

                    connectStage = "datagram-writer"
                    this.datagramWriter = datagrams.writable.getWriter() as WritableStreamDefaultWriter<Uint8Array>
                    this.datagramMode = "native"
                }
            } catch (error) {
                this.logger?.debug(
                    `WebTransport datagrams unavailable; using reliable snapshot fallback: ${error instanceof Error ? error.message : String(error)}`,
                )
            }
            if (!this.datagramWriter) {
                this.datagramMode = "reliable-fallback"
                this.logger?.debug("WebTransport datagrams unavailable; using reliable snapshot fallback")
            }

            connectStage = "incoming-reader"
            this.incomingStreamsReader = transport.incomingUnidirectionalStreams.getReader() as ReadableStreamDefaultReader<ReadableStream<Uint8Array>>

            connectStage = "create-reliable-lane"
            const outgoing = await this.withTimeout(
                transport.createUnidirectionalStream(),
                timeoutMs,
                "WebTransport reliable lane creation timed out",
            )
            connectStage = "reliable-writer"
            this.reliableWriter = outgoing.getWriter() as WritableStreamDefaultWriter<Uint8Array>
            connectStage = "write-lane-marker"
            await this.withTimeout(
                this.reliableWriter.write(new Uint8Array([LANE_CLIENT_RELIABLE])),
                timeoutMs,
                "WebTransport reliable lane setup timed out",
            )

            connectStage = "finalize"
            if (this.lifecycle != "connecting") {
                throw new Error("WebTransport was closed while connecting")
            }
            this.lifecycle = "connected"
            this.blockedSendLogged = false

            void this.acceptIncomingStreams()
            void this.pumpReliable()
            void this.pumpDatagrams()
            void transport.closed.then(
                info => this.handleTransportClosed(info),
                error => this.handleTransportFailure(error),
            )
        } catch (error) {
            this.fail(error, "failednoconnect", `transport failure at ${connectStage}`)
            throw error
        }
    }

    getChannel(id: TransportChannelIdValue): TransportChannel {
        const channel = this.channels[id]
        if (!channel) {
            throw new Error(`Unknown WebTransport channel ${id}`)
        }
        return channel
    }

    async setupHostVideo(setup: TransportVideoSetup): Promise<VideoCodecSupport> {
        if (setup.type.indexOf("data") == -1) {
            throw new Error("WebTransport requires a data video pipeline")
        }
        return allVideoCodecs()
    }

    async setupHostAudio(setup: TransportAudioSetup): Promise<void> {
        if (setup.type.indexOf("data") == -1) {
            throw new Error("WebTransport requires a data audio pipeline")
        }
    }

    private async withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
        let timer: number | null = null
        const timeout = new Promise<T>((_resolve, reject) => {
            timer = window.setTimeout(() => reject(new Error(message)), timeoutMs)
        })
        try {
            return await Promise.race([promise, timeout])
        } finally {
            if (timer != null) {
                window.clearTimeout(timer)
            }
        }
    }

    private newEpoch(): number {
        const value = new Uint16Array(1)
        crypto.getRandomValues(value)
        return value[0]
    }

    private sendChannelMessage(id: TransportChannelIdValue, message: ArrayBuffer): void {
        if (this.lifecycle == "closed" || this.lifecycle == "closing" || this.lifecycle == "failed") {
            this.logBlockedSendOnce()
            return
        }

        if (id == TransportChannelId.MOUSE_ABSOLUTE) {
            const sequence = ((this.snapshotSequences.get(id) ?? 0) + 1) >>> 0
            this.snapshotSequences.set(id, sequence)
            this.queueReliableSnapshot(id, sequence, message)
            return
        }

        if (id >= TransportChannelId.CONTROLLER0 && id <= TransportChannelId.CONTROLLER15) {
            const sequence = ((this.snapshotSequences.get(id) ?? 0) + 1) >>> 0
            this.snapshotSequences.set(id, sequence)
            if (this.queueSnapshotDatagram(id, sequence, message)) {
                return
            }
            this.datagramFallbacks++
            this.queueReliableSnapshot(id, sequence, message)
            return
        }

        this.queueReliable(id, message)
    }

    private queueSnapshotDatagram(id: TransportChannelIdValue, sequence: number, payload: ArrayBuffer): boolean {
        if (!this.datagramWriter) {
            return false
        }
        const size = DATAGRAM_SNAPSHOT_HEADER_BYTES + payload.byteLength
        const maxDatagramSize = this.transport?.datagrams?.maxDatagramSize ?? 0
        if (maxDatagramSize <= 0 || size > maxDatagramSize) {
            return false
        }

        const data = this.encodeSnapshotEnvelope(id, sequence, payload)
        this.putLatestDatagram(id + DATAGRAM_SNAPSHOT_HEADER_BYTES, data, id, sequence, payload)
        return true
    }

    private encodeSnapshotEnvelope(
        id: TransportChannelIdValue,
        sequence: number,
        payload: ArrayBuffer,
    ): Uint8Array {
        const data = new Uint8Array(DATAGRAM_SNAPSHOT_HEADER_BYTES + payload.byteLength)
        const view = new DataView(data.buffer)
        view.setUint8(0, DATAGRAM_MAGIC)
        view.setUint8(1, DATAGRAM_SNAPSHOT)
        view.setUint8(2, id)
        view.setUint16(3, this.epoch, false)
        view.setUint32(5, sequence, false)
        data.set(new Uint8Array(payload), DATAGRAM_SNAPSHOT_HEADER_BYTES)
        return data
    }

    private queueReliableSnapshot(
        id: TransportChannelIdValue,
        sequence: number,
        payload: ArrayBuffer,
    ): void {
        // Send the same epoch/sequence envelope on the reliable lane. The
        // server shares ordering state between both delivery paths, so a
        // delayed older datagram cannot overwrite this newer snapshot.
        const envelope = this.encodeSnapshotEnvelope(id, sequence, payload)
        this.queueReliable(DATAGRAM_MAGIC, envelope.slice(1).buffer, id)
    }

    private putLatestDatagram(
        key: number,
        data: Uint8Array,
        id: TransportChannelIdValue,
        sequence: number,
        payload: ArrayBuffer,
    ): void {
        if (this.pendingDatagrams.has(key)) {
            this.datagramsReplaced++
        }
        this.pendingDatagrams.set(key, { data, id, sequence, payload })
        void this.pumpDatagrams()
    }

    private async pumpDatagrams(): Promise<void> {
        if (this.datagramPumpRunning || !this.datagramWriter || this.lifecycle != "connected") {
            return
        }
        this.datagramPumpRunning = true
        try {
            while (this.lifecycle == "connected" && this.pendingDatagrams.size > 0) {
                const writer = this.datagramWriter
                if (!writer) {
                    return
                }
                await writer.ready

                // Select only after backpressure clears, ensuring that the
                // datagram written is the newest value currently available.
                const next = this.pendingDatagrams.entries().next()
                if (next.done) {
                    continue
                }
                const [key, packet] = next.value
                await writer.write(packet.data)
                // A newer snapshot may have replaced this entry while the
                // write was backpressured. Remove only the value we wrote.
                if (this.pendingDatagrams.get(key) == packet) {
                    this.pendingDatagrams.delete(key)
                }
                this.datagramsSent++
                this.datagramBytesSent += packet.data.byteLength
            }
        } catch (error) {
            if (this.lifecycle == "connected") {
                // QUIC datagrams are an optional fast path for replaceable
                // snapshots. If that path is unavailable, retain the session
                // and transparently use the ordered reliable lane instead.
                const writer = this.datagramWriter
                const fallbackSnapshots = [...this.pendingDatagrams.values()]
                this.datagramWriter = null
                this.pendingDatagrams.clear()
                this.datagramMode = "reliable-fallback"
                this.logger?.debug(
                    `WebTransport datagrams disabled: ${error instanceof Error ? error.message : String(error)}`,
                )
                try {
                    writer?.releaseLock()
                } catch (_releaseError) { }
                for (const packet of fallbackSnapshots) {
                    this.datagramFallbacks++
                    this.queueReliableSnapshot(packet.id, packet.sequence, packet.payload)
                }
            }
        } finally {
            this.datagramPumpRunning = false
            if (this.lifecycle == "connected" && this.pendingDatagrams.size > 0) {
                void this.pumpDatagrams()
            }
        }
    }

    private queueReliable(
        id: number,
        payload: ArrayBuffer,
        replaceableSnapshotId: TransportChannelIdValue | null = null,
    ): void {
        const bufferedBytes = 5 + payload.byteLength
        if (payload.byteLength + 1 > MAX_RELIABLE_FRAME_BYTES) {
            this.reliableQueueOverflows++
            this.fail(new Error("WebTransport reliable frame exceeded its bound"), this.lifecycle == "connected" ? "failed" : "failednoconnect")
            return
        }

        if (replaceableSnapshotId == null) {
            // Never replace a snapshot across a reliable key/button/control
            // barrier. A later snapshot starts a fresh replaceable segment.
            this.pendingReliableSnapshots.clear()

            // Mouse motion and high-resolution scroll are additive. Merge
            // only adjacent packets, so key/button/control messages remain
            // strict ordering barriers while backpressure cannot build a long
            // replay of tiny stale movements.
            const previous = this.reliableQueue[this.reliableQueue.length - 1]
            const combinedMotion = previous
                ? this.combineAdjacentMouseMotion(previous, id, payload)
                : null
            if (combinedMotion) {
                previous.payload = combinedMotion
                previous.bufferedBytes = 5 + combinedMotion.byteLength
                this.reliableMotionCoalesced++
                return
            }
        } else {
            const pending = this.pendingReliableSnapshots.get(replaceableSnapshotId)
            if (pending) {
                const nextQueuedBytes = this.reliableQueuedBytes - pending.bufferedBytes + bufferedBytes
                if (nextQueuedBytes + this.reliableInFlightBytes > MAX_RELIABLE_QUEUED_BYTES) {
                    this.reliableQueueOverflows++
                    this.fail(new Error("WebTransport reliable queue exceeded its bound"), this.lifecycle == "connected" ? "failed" : "failednoconnect")
                    return
                }
                pending.payload = payload
                pending.bufferedBytes = bufferedBytes
                this.reliableQueuedBytes = nextQueuedBytes
                this.maxReliableQueuedBytes = Math.max(this.maxReliableQueuedBytes, this.reliableQueuedBytes)
                this.reliableSnapshotsReplaced++
                return
            }
        }

        if (
            this.reliableQueue.length >= MAX_RELIABLE_QUEUED_FRAMES ||
            this.reliableQueuedBytes + this.reliableInFlightBytes + bufferedBytes > MAX_RELIABLE_QUEUED_BYTES
        ) {
            this.reliableQueueOverflows++
            this.fail(new Error("WebTransport reliable queue exceeded its bound"), this.lifecycle == "connected" ? "failed" : "failednoconnect")
            return
        }

        const frame = {
            id,
            payload,
            bufferedBytes,
            enqueuedAt: performance.now(),
            snapshotId: replaceableSnapshotId,
        }
        // Decoder recovery must not wait behind a backlog of user input. It is
        // independent of input ordering and is idempotently latched upstream.
        if (id == TransportChannelId.HOST_VIDEO) {
            this.reliableQueue.unshift(frame)
        } else {
            this.reliableQueue.push(frame)
        }
        if (replaceableSnapshotId != null) {
            this.pendingReliableSnapshots.set(replaceableSnapshotId, frame)
        }
        this.reliableQueuedBytes += bufferedBytes
        this.maxReliableQueuedBytes = Math.max(this.maxReliableQueuedBytes, this.reliableQueuedBytes)
        void this.pumpReliable()
    }

    private combineAdjacentMouseMotion(
        previous: ReliableFrame,
        id: TransportChannelIdValue,
        payload: ArrayBuffer,
    ): ArrayBuffer | null {
        if (
            id != TransportChannelId.MOUSE_RELATIVE || previous.id != id ||
            previous.payload.byteLength != 5 || payload.byteLength != 5
        ) {
            return null
        }

        const previousView = new DataView(previous.payload)
        const nextView = new DataView(payload)
        const packetType = nextView.getUint8(0)
        if (packetType != previousView.getUint8(0) || (packetType != 0 && packetType != 3)) {
            return null
        }

        const x = previousView.getInt16(1, false) + nextView.getInt16(1, false)
        const y = previousView.getInt16(3, false) + nextView.getInt16(3, false)
        if (x < -0x8000 || x > 0x7fff || y < -0x8000 || y > 0x7fff) {
            return null
        }

        const combined = new ArrayBuffer(5)
        const combinedView = new DataView(combined)
        combinedView.setUint8(0, packetType)
        combinedView.setInt16(1, x, false)
        combinedView.setInt16(3, y, false)
        return combined
    }

    private async pumpReliable(): Promise<void> {
        if (this.reliablePumpRunning || !this.reliableWriter || this.lifecycle != "connected") {
            return
        }
        this.reliablePumpRunning = true
        try {
            while (this.lifecycle == "connected" && this.reliableQueue.length > 0) {
                const writer = this.reliableWriter
                if (!writer) {
                    return
                }
                // Keep replaceable snapshots in the queue until QUIC is
                // writable so input arriving under backpressure can still
                // replace them with the newest state.
                await writer.ready
                const frame = this.reliableQueue.shift()
                if (!frame) {
                    continue
                }
                if (
                    frame.snapshotId != null &&
                    this.pendingReliableSnapshots.get(frame.snapshotId) == frame
                ) {
                    this.pendingReliableSnapshots.delete(frame.snapshotId)
                }
                this.reliableQueuedBytes -= frame.bufferedBytes
                this.reliableInFlightBytes = frame.bufferedBytes
                this.maxReliableQueueAgeMs = Math.max(
                    this.maxReliableQueueAgeMs,
                    performance.now() - frame.enqueuedAt,
                )

                if (frame.payload.byteLength <= COMBINED_RELIABLE_WRITE_MAX_BYTES) {
                    // Browser-to-host reliable traffic is normally tiny input.
                    // One small copy avoids a second stream write and promise
                    // boundary on the latency-sensitive path.
                    const packet = new Uint8Array(frame.bufferedBytes)
                    const view = new DataView(packet.buffer)
                    view.setUint32(0, frame.payload.byteLength + 1, false)
                    view.setUint8(4, frame.id)
                    packet.set(new Uint8Array(frame.payload), 5)
                    await writer.write(packet)
                } else {
                    // Retain split writes for unusually large payloads so they
                    // do not require another full-size allocation and copy.
                    const header = new Uint8Array(5)
                    const view = new DataView(header.buffer)
                    view.setUint32(0, frame.payload.byteLength + 1, false)
                    view.setUint8(4, frame.id)
                    await writer.write(header)
                    await writer.write(new Uint8Array(frame.payload))
                }
                this.reliableFramesSent++
                this.reliableBytesSent += frame.bufferedBytes
                this.reliableInFlightBytes = 0
            }
        } catch (error) {
            this.reliableInFlightBytes = 0
            if (this.lifecycle == "connected") {
                this.fail(error, "failed")
            }
        } finally {
            this.reliablePumpRunning = false
            if (this.lifecycle == "connected" && this.reliableQueue.length > 0) {
                void this.pumpReliable()
            }
        }
    }

    private async acceptIncomingStreams(): Promise<void> {
        const reader = this.incomingStreamsReader
        if (!reader) {
            return
        }
        try {
            while (this.lifecycle == "connected") {
                const result = await reader.read()
                if (this.lifecycle != "connected") {
                    break
                }
                if (result.done) {
                    break
                }
                if (this.incomingLaneReaders.size >= MAX_INCOMING_STREAM_TASKS) {
                    // Apply natural stream backpressure instead of issuing
                    // STOP_SENDING, which can race the peer's FIN and used to
                    // tear down an otherwise healthy WebTransport session.
                    this.incomingStreamBackpressureEvents++
                    this.incomingStreams++
                    await this.consumeIncomingLane(result.value)
                    continue
                }
                this.incomingStreams++
                void this.consumeIncomingLane(result.value)
            }
        } catch (error) {
            if (this.lifecycle == "connected") {
                this.fail(error, "failed")
            }
        }
    }

    private async consumeIncomingLane(stream: ReadableStream<Uint8Array>): Promise<void> {
        const reader = stream.getReader()
        this.incomingLaneReaders.add(reader)
        const decoder = new IncomingLaneDecoder(
            (id, payload) => this.dispatchIncoming(id, payload),
            (sequence, id, payload) => this.receiveVideoFrame(sequence, id, payload),
            bytes => this.reserveIncomingBytes(bytes),
            bytes => this.releaseIncomingBytes(bytes),
        )
        try {
            while (this.lifecycle == "connected") {
                const result = await reader.read()
                if (this.lifecycle != "connected") {
                    decoder.abort()
                    break
                }
                if (result.done) {
                    decoder.finish()
                    break
                }
                decoder.push(result.value)
                const sequence = decoder.videoSequence
                if (decoder.lane == LANE_VIDEO_FRAME && sequence != null) {
                    const existing = this.activeVideoReaders.get(sequence)
                    if (existing && existing != reader) {
                        throw new WireProtocolError(`Duplicate active WebTransport video sequence ${sequence}`)
                    }
                    this.activeVideoReaders.set(sequence, reader)
                }
            }
        } catch (error) {
            if (this.lifecycle == "connected") {
                this.incomingStreamErrors++
            }
            decoder.abort()
            if (
                this.lifecycle == "connected" &&
                decoder.lane == LANE_VIDEO_FRAME &&
                !(error instanceof WireProtocolError) &&
                !decoder.videoFrameComplete
            ) {
                this.handleVideoStreamFailure(decoder.videoSequence)
            } else if (this.lifecycle == "connected") {
                if (error instanceof WireProtocolError) {
                    this.fail(error, "failed")
                } else if (!decoder.videoFrameComplete) {
                    this.fail(error, "failed")
                }
            }
        } finally {
            this.incomingLaneReaders.delete(reader)
            const sequence = decoder.videoSequence
            if (sequence != null && this.activeVideoReaders.get(sequence) == reader) {
                this.activeVideoReaders.delete(sequence)
            }
            try {
                reader.releaseLock()
            } catch (_error) { }
        }
    }

    private reserveIncomingBytes(bytes: number): boolean {
        if (bytes < 0 || this.incomingAllocatedBytes + bytes > MAX_INCOMING_ALLOCATED_BYTES) {
            return false
        }
        this.incomingAllocatedBytes += bytes
        this.maxIncomingAllocatedBytes = Math.max(this.maxIncomingAllocatedBytes, this.incomingAllocatedBytes)
        return true
    }

    private releaseIncomingBytes(bytes: number): void {
        this.incomingAllocatedBytes = Math.max(0, this.incomingAllocatedBytes - bytes)
    }

    private receiveVideoFrame(sequence: number, id: number, payload: ArrayBuffer): void {
        this.videoFramesReceived++
        if (id != TransportChannelId.HOST_VIDEO || payload.byteLength < 1) {
            this.fail(new WireProtocolError("Invalid WebTransport video frame channel or payload"), "failed")
            return
        }

        const isIdr = new Uint8Array(payload, 0, 1)[0] == 1
        if (this.videoAwaitingIdr || this.videoExpectedSequence == null) {
            if (!this.videoSequenceIsFresh(sequence)) {
                this.videoFramesDropped++
                return
            }
            if (!isIdr) {
                // This delta may have been encoded after an IDR whose stream
                // is merely slower. Retain it within strict bounds; once the
                // IDR arrives, sequence order proves whether it is usable.
                if (!this.bufferPendingVideoFrame(sequence, id, payload, false)) {
                    this.videoFramesDropped++
                }
                this.requestVideoRecovery()
                return
            }
            this.acceptVideoIdr(sequence, id, payload)
            return
        }

        const expected = this.videoExpectedSequence
        if (sequence == expected) {
            this.deliverVideoFrame(sequence, id, payload)
            this.flushPendingVideoFrames()
            return
        }
        if (!isNewerU32(sequence, expected)) {
            this.videoFramesDropped++
            return
        }

        // An IDR has no dependency on the skipped frames, so advance to it
        // immediately. This is monotonic, but never decodes a delta out of
        // order or lets a lost delta hold an entire GOP behind it.
        if (isIdr) {
            this.acceptVideoIdr(sequence, id, payload)
            return
        }

        if (!this.bufferPendingVideoFrame(sequence, id, payload, true)) {
            this.videoFramesDropped++
            this.enterVideoRecovery()
        }
    }

    private acceptVideoIdr(sequence: number, id: number, payload: ArrayBuffer): void {
        // Older streams are obsolete after an independently decodable frame,
        // but drain them instead of cancelling: STOP_SENDING can race a peer
        // FIN/retransmission and violate QUIC final-size invariants.
        this.discardPendingVideoFramesNotNewerThan(sequence)
        this.clearVideoReorderTimer()
        this.clearVideoRecoveryRetryTimer()
        this.videoAwaitingIdr = false
        this.videoIdrRequested = false
        this.deliverVideoFrame(sequence, id, payload)
        this.flushPendingVideoFrames()
    }

    private bufferPendingVideoFrame(
        sequence: number,
        id: number,
        payload: ArrayBuffer,
        scheduleDeadline: boolean,
    ): boolean {
        if (this.pendingVideoFrames.has(sequence)) {
            return false
        }
        if (
            this.videoExpectedSequence != null &&
            sequenceDistance(sequence, this.videoExpectedSequence) > MAX_VIDEO_SEQUENCE_GAP
        ) {
            return false
        }
        const bufferedBytes = payload.byteLength + 1
        if (
            this.pendingVideoFrames.size >= MAX_PENDING_VIDEO_FRAMES ||
            this.pendingVideoBytes + bufferedBytes > MAX_PENDING_VIDEO_BYTES
        ) {
            return false
        }

        this.pendingVideoFrames.set(sequence, { id, payload, bufferedBytes })
        this.pendingVideoBytes += bufferedBytes
        this.videoFramesReordered++
        if (scheduleDeadline) {
            this.scheduleVideoReorderDeadline()
        }
        return true
    }

    private discardPendingVideoFramesNotNewerThan(sequence: number): void {
        for (const [pendingSequence, frame] of this.pendingVideoFrames) {
            if (!isNewerU32(pendingSequence, sequence)) {
                this.pendingVideoFrames.delete(pendingSequence)
                this.pendingVideoBytes -= frame.bufferedBytes
                this.videoFramesDropped++
            }
        }
    }

    private deliverVideoFrame(sequence: number, id: number, payload: ArrayBuffer): void {
        this.videoLastDeliveredSequence = sequence
        this.videoExpectedSequence = (sequence + 1) >>> 0
        this.dispatchIncoming(id, payload)
    }

    private flushPendingVideoFrames(): void {
        while (this.videoExpectedSequence != null) {
            const frame = this.pendingVideoFrames.get(this.videoExpectedSequence)
            if (!frame) {
                break
            }
            const sequence = this.videoExpectedSequence
            this.pendingVideoFrames.delete(sequence)
            this.pendingVideoBytes -= frame.bufferedBytes
            this.deliverVideoFrame(sequence, frame.id, frame.payload)
        }
        if (this.pendingVideoFrames.size == 0) {
            this.clearVideoReorderTimer()
        } else {
            this.scheduleVideoReorderDeadline()
        }
    }

    private handleVideoStreamFailure(sequence: number | null): void {
        if (sequence != null && this.videoExpectedSequence != null) {
            if (sequence == this.videoExpectedSequence) {
                this.enterVideoRecovery()
            } else if (isNewerU32(sequence, this.videoExpectedSequence)) {
                // Earlier streams can still form a decodable prefix. Ask for
                // an IDR now, but do not withhold that useful prefix merely
                // because a future independent stream failed first.
                this.requestVideoRecovery()
            }
            return
        }
        this.enterVideoRecovery()
    }

    private videoSequenceIsFresh(sequence: number): boolean {
        return this.videoLastDeliveredSequence == null || isNewerU32(sequence, this.videoLastDeliveredSequence)
    }

    private scheduleVideoReorderDeadline(): void {
        const expected = this.videoExpectedSequence
        if (expected == null || this.pendingVideoFrames.size == 0) {
            return
        }
        if (this.videoReorderTimer != null && this.videoReorderExpectedSequence == expected) {
            return
        }
        this.clearVideoReorderTimer()
        this.videoReorderExpectedSequence = expected
        this.videoReorderTimer = window.setTimeout(() => {
            this.videoReorderTimer = null
            this.videoReorderExpectedSequence = null
            if (
                this.lifecycle == "connected" &&
                this.pendingVideoFrames.size > 0 &&
                this.videoExpectedSequence == expected
            ) {
                // A reorder timeout is evidence of loss, not proof. Ask for
                // an IDR while still allowing the missing delta to arrive.
                this.videoReorderTimeouts++
                this.requestVideoRecovery()
            }
        }, VIDEO_REORDER_DEADLINE_MS)
    }

    private enterVideoRecovery(): void {
        this.clearVideoReorderTimer()
        this.videoAwaitingIdr = true
        this.requestVideoRecovery()
    }

    private requestVideoRecovery(): void {
        if (this.videoIdrRequested || this.lifecycle != "connected") {
            return
        }
        this.videoIdrRequested = true
        this.videoRecoveryRequests++
        this.queueReliable(TransportChannelId.HOST_VIDEO, new Uint8Array([0]).buffer)
        this.clearVideoRecoveryRetryTimer()
        this.videoRecoveryRetryTimer = window.setTimeout(() => {
            this.videoRecoveryRetryTimer = null
            if (this.lifecycle != "connected" || !this.videoIdrRequested) {
                return
            }
            this.videoIdrRequested = false
            if (this.videoAwaitingIdr || this.pendingVideoFrames.size > 0) {
                this.requestVideoRecovery()
            }
        }, VIDEO_RECOVERY_RETRY_MS)
    }

    private clearVideoReorderTimer(): void {
        if (this.videoReorderTimer != null) {
            window.clearTimeout(this.videoReorderTimer)
            this.videoReorderTimer = null
        }
        this.videoReorderExpectedSequence = null
    }

    private clearVideoRecoveryRetryTimer(): void {
        if (this.videoRecoveryRetryTimer != null) {
            window.clearTimeout(this.videoRecoveryRetryTimer)
            this.videoRecoveryRetryTimer = null
        }
    }

    private clearPendingVideoFrames(): void {
        this.clearVideoReorderTimer()
        this.clearVideoRecoveryRetryTimer()
        this.pendingVideoFrames.clear()
        this.pendingVideoBytes = 0
    }

    private dispatchIncoming(id: number, payload: ArrayBuffer): void {
        if (this.lifecycle != "connected") {
            return
        }
        const channel = this.channels[id]
        if (!channel) {
            this.logger?.debug(`Ignoring WebTransport frame for unknown channel ${id}`)
            return
        }
        this.incomingFrames++
        this.incomingBytes += payload.byteLength + 1
        channel.receive(payload)
    }

    private bufferedBytes(): number | null {
        if (this.lifecycle == "closed" || this.lifecycle == "failed") {
            return null
        }
        let datagramBytes = 0
        for (const packet of this.pendingDatagrams.values()) {
            datagramBytes += packet.data.byteLength
        }
        return this.reliableQueuedBytes + this.reliableInFlightBytes + datagramBytes
    }

    private handleTransportClosed(info: WebTransportCloseInfo): void {
        if (this.lifecycle == "closing" || this.lifecycle == "closed") {
            return
        }
        const wasConnected = this.lifecycle == "connected"
        this.logger?.debug(`WebTransport closed (${info.closeCode}): ${info.reason}`)
        this.lifecycle = "closed"
        this.clearQueues()
        // Intentional local close changes lifecycle to closing before the
        // browser's closed promise settles and is handled above. Any close
        // observed here is therefore remote/unexpected and should trigger a
        // fresh WebSocket fallback even if the peer used close code zero.
        this.notifyClose(wasConnected ? "failed" : "failednoconnect")
    }

    private handleTransportFailure(error: unknown): void {
        if (this.lifecycle == "closing" || this.lifecycle == "closed" || this.lifecycle == "failed") {
            return
        }
        this.fail(error, this.lifecycle == "connected" ? "failed" : "failednoconnect")
    }

    private fail(
        error: unknown,
        shutdown: TransportShutdown,
        closeReason: string = "transport failure",
    ): void {
        if (this.lifecycle == "failed" || this.lifecycle == "closed" || this.lifecycle == "closing") {
            return
        }
        this.lifecycle = "failed"
        this.logger?.debug(`WebTransport failed: ${error instanceof Error ? error.message : String(error)}`)
        this.clearQueues()
        try {
            this.transport?.close({ closeCode: 1, reason: closeReason })
        } catch (_closeError) { }
        this.notifyClose(shutdown)
    }

    private notifyClose(shutdown: TransportShutdown): void {
        if (this.closeNotified) {
            return
        }
        this.closeNotified = true
        this.onclose?.(shutdown)
    }

    private logBlockedSendOnce(): void {
        if (!this.blockedSendLogged) {
            this.blockedSendLogged = true
            this.logger?.debug("WebTransport is closed; dropping outgoing data")
        }
    }

    private clearQueues(): void {
        this.reliableQueue.length = 0
        this.pendingReliableSnapshots.clear()
        this.reliableQueuedBytes = 0
        this.reliableInFlightBytes = 0
        this.pendingDatagrams.clear()
        this.clearPendingVideoFrames()
    }

    async close(): Promise<void> {
        if (this.lifecycle == "closed" || this.lifecycle == "closing") {
            return
        }
        this.lifecycle = "closing"
        this.clearQueues()

        const closed = this.transport?.closed.catch(() => undefined)
        try {
            this.transport?.close({ closeCode: 0, reason: "client closed" })
        } catch (_error) { }
        if (closed) {
            await Promise.race([
                closed,
                new Promise<void>(resolve => window.setTimeout(resolve, CLOSE_DRAIN_TIMEOUT_MS)),
            ])
        }
        this.lifecycle = "closed"
    }

    async getStats(): Promise<Record<string, StatValue>> {
        let datagramBufferedBytes = 0
        for (const packet of this.pendingDatagrams.values()) {
            datagramBufferedBytes += packet.data.byteLength
        }
        return {
            webTransportState: this.lifecycle,
            webTransportReliableQueuedBytes: this.reliableQueuedBytes + this.reliableInFlightBytes,
            webTransportReliableQueuedFrames: this.reliableQueue.length + (this.reliableInFlightBytes > 0 ? 1 : 0),
            webTransportReliableMaxQueuedBytes: this.maxReliableQueuedBytes,
            webTransportReliableFramesSent: this.reliableFramesSent,
            webTransportReliableBytesSent: this.reliableBytesSent,
            webTransportReliableQueueOverflows: this.reliableQueueOverflows,
            webTransportReliableSnapshotsReplaced: this.reliableSnapshotsReplaced,
            webTransportReliableMotionCoalesced: this.reliableMotionCoalesced,
            webTransportReliableOldestQueuedAgeMs: this.reliableQueue.length > 0
                ? performance.now() - this.reliableQueue[0].enqueuedAt
                : 0,
            webTransportReliableMaxQueueAgeMs: this.maxReliableQueueAgeMs,
            webTransportDatagramMode: this.datagramMode,
            webTransportDatagramBufferedBytes: datagramBufferedBytes,
            webTransportDatagramPending: this.pendingDatagrams.size,
            webTransportDatagramsSent: this.datagramsSent,
            webTransportDatagramBytesSent: this.datagramBytesSent,
            webTransportDatagramsReplaced: this.datagramsReplaced,
            webTransportDatagramFallbacks: this.datagramFallbacks,
            webTransportIncomingStreams: this.incomingStreams,
            webTransportIncomingFrames: this.incomingFrames,
            webTransportIncomingBytes: this.incomingBytes,
            webTransportIncomingStreamErrors: this.incomingStreamErrors,
            webTransportIncomingStreamBackpressureEvents: this.incomingStreamBackpressureEvents,
            webTransportIncomingAllocatedBytes: this.incomingAllocatedBytes,
            webTransportIncomingMaxAllocatedBytes: this.maxIncomingAllocatedBytes,
            webTransportVideoPendingFrames: this.pendingVideoFrames.size,
            webTransportVideoPendingBytes: this.pendingVideoBytes,
            webTransportVideoFramesReceived: this.videoFramesReceived,
            webTransportVideoFramesDropped: this.videoFramesDropped,
            webTransportVideoFramesReordered: this.videoFramesReordered,
            webTransportVideoReorderTimeouts: this.videoReorderTimeouts,
            webTransportVideoRecoveryRequests: this.videoRecoveryRequests,
            webTransportVideoAwaitingIdr: this.videoAwaitingIdr ? "true" : "false",
        }
    }
}

class WebTransportDataTransportChannel implements DataTransportChannel {
    readonly type: "data" = "data"
    readonly canReceive = true
    readonly canSend = true

    private readonly listeners: Array<(data: ArrayBuffer) => void> = []

    constructor(
        private readonly id: TransportChannelIdValue,
        private readonly sendFrame: ChannelSender,
        private readonly readBufferedBytes: BufferedBytesReader,
    ) { }

    addReceiveListener(listener: (data: ArrayBuffer) => void): void {
        this.listeners.push(listener)
    }

    removeReceiveListener(listener: (data: ArrayBuffer) => void): void {
        const index = this.listeners.indexOf(listener)
        if (index != -1) {
            this.listeners.splice(index, 1)
        }
    }

    receive(payload: ArrayBuffer): void {
        for (const listener of this.listeners) {
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

class WireProtocolError extends Error { }

class IncomingLaneDecoder {
    private laneValue: number | null = null
    private readonly sequenceBytes = new Uint8Array(4)
    private sequenceBytesUsed = 0
    private sequenceValue: number | null = null
    private readonly lengthBytes = new Uint8Array(4)
    private lengthBytesUsed = 0
    private payload: Uint8Array | null = null
    private payloadUsed = 0
    private channelId: number | null = null
    private reservedBytes = 0
    private videoFrameDispatched = false

    constructor(
        private readonly onFrame: (id: number, payload: ArrayBuffer) => void,
        private readonly onVideoFrame: (sequence: number, id: number, payload: ArrayBuffer) => void,
        private readonly reserveBytes: (bytes: number) => boolean,
        private readonly releaseBytes: (bytes: number) => void,
    ) { }

    get lane(): number | null {
        return this.laneValue
    }

    get videoSequence(): number | null {
        return this.sequenceValue
    }

    get videoFrameComplete(): boolean {
        return this.videoFrameDispatched
    }

    push(chunk: Uint8Array): void {
        let offset = 0
        if (this.laneValue == null) {
            if (chunk.byteLength == 0) {
                return
            }
            this.laneValue = chunk[0]
            if (
                this.laneValue != LANE_VIDEO_FRAME &&
                this.laneValue != LANE_AUDIO &&
                this.laneValue != LANE_OTHER
            ) {
                throw new WireProtocolError(`Unknown WebTransport incoming lane ${this.laneValue}`)
            }
            offset = 1
        }

        while (offset < chunk.byteLength) {
            if (this.videoFrameDispatched) {
                throw new WireProtocolError("WebTransport video stream contains trailing data")
            }

            if (this.laneValue == LANE_VIDEO_FRAME && this.sequenceBytesUsed < 4) {
                while (this.sequenceBytesUsed < 4 && offset < chunk.byteLength) {
                    this.sequenceBytes[this.sequenceBytesUsed++] = chunk[offset++]
                }
                if (this.sequenceBytesUsed < 4) {
                    return
                }
                this.sequenceValue = new DataView(this.sequenceBytes.buffer).getUint32(0, false)
            }

            if (this.payload == null) {
                while (this.lengthBytesUsed < 4 && offset < chunk.byteLength) {
                    this.lengthBytes[this.lengthBytesUsed++] = chunk[offset++]
                }
                if (this.lengthBytesUsed < 4) {
                    return
                }
                const length = new DataView(this.lengthBytes.buffer).getUint32(0, false)
                this.lengthBytesUsed = 0
                if (length < 1 || length > MAX_MEDIA_FRAME_BYTES) {
                    throw new WireProtocolError(`Invalid WebTransport frame length ${length}`)
                }
                const payloadBytes = length - 1
                if (!this.reserveBytes(payloadBytes)) {
                    throw new WireProtocolError("WebTransport incoming allocation limit exceeded")
                }
                this.reservedBytes = payloadBytes
                this.payload = new Uint8Array(payloadBytes)
                this.payloadUsed = 0
                this.channelId = null
            }

            if (this.channelId == null) {
                if (offset >= chunk.byteLength) {
                    return
                }
                this.channelId = chunk[offset++]
            }

            const payload = this.payload
            const remaining = payload.byteLength - this.payloadUsed
            const available = chunk.byteLength - offset
            const copyLength = Math.min(remaining, available)
            if (copyLength > 0) {
                payload.set(chunk.subarray(offset, offset + copyLength), this.payloadUsed)
                this.payloadUsed += copyLength
                offset += copyLength
            }

            if (this.payloadUsed == payload.byteLength) {
                const channelId = this.channelId
                this.payload = null
                this.channelId = null
                this.payloadUsed = 0
                if (channelId == null) {
                    throw new WireProtocolError("WebTransport frame has no channel")
                }

                if (this.laneValue == LANE_VIDEO_FRAME) {
                    if (offset < chunk.byteLength) {
                        throw new WireProtocolError("WebTransport video stream contains trailing data")
                    }
                    if (this.sequenceValue == null) {
                        throw new WireProtocolError("WebTransport video stream has no sequence")
                    }
                    // Payload length is authoritative. Dispatch immediately
                    // and merely drain/validate FIN afterward so a delayed or
                    // retransmitted FIN cannot hold a complete video frame.
                    this.videoFrameDispatched = true
                    this.releasePayloadReservation()
                    this.onVideoFrame(this.sequenceValue, channelId, payload.buffer as ArrayBuffer)
                } else {
                    this.releasePayloadReservation()
                    this.onFrame(channelId, payload.buffer as ArrayBuffer)
                }
            }
        }
    }

    finish(): void {
        if (
            this.laneValue == null ||
            this.lengthBytesUsed != 0 ||
            this.payload != null ||
            (this.laneValue == LANE_VIDEO_FRAME && this.sequenceBytesUsed != 4)
        ) {
            throw new WireProtocolError("WebTransport incoming lane ended with an incomplete frame")
        }
        if (this.laneValue == LANE_VIDEO_FRAME) {
            if (this.sequenceValue == null || !this.videoFrameDispatched) {
                throw new WireProtocolError("WebTransport video stream ended without exactly one frame")
            }
        }
    }

    abort(): void {
        this.payload = null
        this.releasePayloadReservation()
    }

    private releasePayloadReservation(): void {
        if (this.reservedBytes > 0) {
            this.releaseBytes(this.reservedBytes)
            this.reservedBytes = 0
        }
    }
}

function sequenceDistance(current: number, previous: number): number {
    return (current - previous) >>> 0
}

function isNewerU32(current: number, previous: number): boolean {
    const distance = sequenceDistance(current, previous)
    return distance != 0 && distance < 0x80000000
}
