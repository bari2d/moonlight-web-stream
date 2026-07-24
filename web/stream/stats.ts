import { StreamerStatsUpdate, TransportChannelId } from "../api_bindings.js"
import { BIG_BUFFER, ByteBuffer } from "./buffer.js"
import { Logger } from "./log.js"
import { Pipe } from "./pipeline/index.js"
import { DataTransportChannel, Transport } from "./transport/index.js"

export type StatValue = string | number
export type StreamStatsMode = "off" | "compact" | "advanced"

export type StreamStatsData = {
    transportImplementation: string | null
    videoCodec: string | null
    videoWidth: number | null
    videoHeight: number | null
    videoFps: number | null
    videoPipeline: string | null
    audioPipeline: string | null
    hdrEnabled: boolean | null
    streamerRttMs: number | null
    streamerRttVarianceMs: number | null
    minHostProcessingLatencyMs: number | null
    maxHostProcessingLatencyMs: number | null
    avgHostProcessingLatencyMs: number | null
    minStreamerProcessingTimeMs: number | null
    maxStreamerProcessingTimeMs: number | null
    avgStreamerProcessingTimeMs: number | null
    browserRtt: number | null
    transport: Record<string, StatValue>
    video: Record<string, StatValue>
    audio: Record<string, StatValue>
}

function num(value: number | null | undefined, suffix?: string): string | null {
    if (value == null) {
        return null
    } else {
        return `${value.toFixed(2)}${suffix ?? ""}`
    }
}

function finiteStat(record: Record<string, StatValue>, key: string): number | null {
    const value = record[key]
    return typeof value == "number" && Number.isFinite(value) ? value : null
}

function firstFinite(...values: Array<number | null>): number | null {
    return values.find(value => value != null) ?? null
}

function compactNumber(value: number, digits: number): string {
    return value.toFixed(digits).replace(/\.0+$/, "").replace(/(\.\d*?)0+$/, "$1")
}

function friendlyTransportName(implementationName: string): string {
    const knownNames: Record<string, string> = {
        web_transport: "WebTransport",
        web_socket: "WebSocket",
        webrtc: "WebRTC",
    }
    return knownNames[implementationName] ?? implementationName.replace(/_/g, " ")
}

export function streamStatsToCompactText(statsData: StreamStatsData): string {
    const lines = ["Stream stats — Compact (press Stats for Advanced)"]

    if (statsData.transportImplementation) {
        lines.push(`Transport: ${friendlyTransportName(statsData.transportImplementation)}`)
    }

    const videoParts: Array<string> = []
    if (statsData.videoCodec) {
        videoParts.push(statsData.videoCodec)
    }
    if (statsData.videoWidth != null && statsData.videoHeight != null) {
        videoParts.push(`${statsData.videoWidth}×${statsData.videoHeight}`)
    }
    if (statsData.videoFps != null) {
        videoParts.push(`${compactNumber(statsData.videoFps, 1)} configured FPS`)
    }
    if (videoParts.length > 0) {
        lines.push(`Video: ${videoParts.join(" · ")}`)
    }

    if (statsData.streamerRttMs != null) {
        const variance = statsData.streamerRttVarianceMs == null
            ? ""
            : ` · variation ${compactNumber(statsData.streamerRttVarianceMs, 1)} ms`
        lines.push(`Host ↔ streamer RTT: ${compactNumber(statsData.streamerRttMs, 1)} ms${variance}`)
    }
    if (statsData.browserRtt != null) {
        lines.push(`Streamer ↔ browser RTT: ${compactNumber(statsData.browserRtt, 1)} ms`)
    }
    if (statsData.avgHostProcessingLatencyMs != null) {
        lines.push(`Average host processing: ${compactNumber(statsData.avgHostProcessingLatencyMs, 1)} ms`)
    }
    if (statsData.avgStreamerProcessingTimeMs != null) {
        lines.push(`Average streamer processing: ${compactNumber(statsData.avgStreamerProcessingTimeMs, 1)} ms`)
    }

    const presentedFps = firstFinite(
        finiteStat(statsData.video, "videoElementPresentedFps"),
        finiteStat(statsData.transport, "webrtcRenderFps"),
        finiteStat(statsData.video, "videoElementCallbackFps"),
        finiteStat(statsData.transport, "webrtcFps"),
    )
    const displayDropPercent = finiteStat(statsData.video, "videoElementDropPercent")
    let deliveryDropPercent: number | null = null
    if (displayDropPercent == null) {
        deliveryDropPercent = finiteStat(statsData.transport, "webTransportVideoDropPercent")
    }
    if (displayDropPercent == null && deliveryDropPercent == null) {
        const droppedFps = finiteStat(statsData.transport, "webrtcDroppedFps")
        const receivedFps = finiteStat(statsData.transport, "webrtcReceiveFps")
        if (droppedFps != null && receivedFps != null && receivedFps > 0) {
            deliveryDropPercent = droppedFps * 100 / receivedFps
        }
    }
    if (presentedFps != null || displayDropPercent != null) {
        const displayParts: Array<string> = []
        if (presentedFps != null) {
            displayParts.push(`${compactNumber(presentedFps, 1)} presented FPS`)
        }
        if (displayDropPercent != null) {
            displayParts.push(`${compactNumber(displayDropPercent, 1)}% dropped frames`)
        }
        lines.push(`Display: ${displayParts.join(" · ")}`)
    }
    if (deliveryDropPercent != null) {
        lines.push(`Video delivery: ${compactNumber(deliveryDropPercent, 1)}% dropped frames`)
    }

    const receiveMbps = firstFinite(
        finiteStat(statsData.transport, "webrtcReceiveMbps"),
        finiteStat(statsData.transport, "webTransportReceiveMbps"),
    )
    const packetLossPercent = finiteStat(statsData.transport, "webrtcPacketLossPercent")
    if (receiveMbps != null || packetLossPercent != null) {
        const receiveParts: Array<string> = []
        if (receiveMbps != null) {
            receiveParts.push(`${compactNumber(receiveMbps, 2)} Mbps`)
        }
        if (packetLossPercent != null) {
            receiveParts.push(`${compactNumber(packetLossPercent, 2)}% packet loss`)
        }
        lines.push(`Receive: ${receiveParts.join(" · ")}`)
    }

    return `${lines.join("\n")}\n`
}

export function streamStatsToText(statsData: StreamStatsData, mode: StreamStatsMode = "advanced"): string {
    if (mode == "off") {
        return ""
    }
    if (mode == "compact") {
        return streamStatsToCompactText(statsData)
    }

    let text = `Stream stats — Advanced (press Stats to turn Off)
stats:
transport implementation: ${statsData.transportImplementation}
video information: ${statsData.videoCodec}, ${statsData.videoWidth}x${statsData.videoHeight}, ${statsData.videoFps} fps
HDR: ${statsData.hdrEnabled === true ? "Enabled" : statsData.hdrEnabled === false ? "Disabled" : "Unknown"}
video pipeline: ${statsData.videoPipeline}
audio pipeline: ${statsData.audioPipeline}
streamer round trip time: ${num(statsData.streamerRttMs, "ms")} (variance: ${num(statsData.streamerRttVarianceMs, "ms")})
host processing latency min/max/avg: ${num(statsData.minHostProcessingLatencyMs, "ms")} / ${num(statsData.maxHostProcessingLatencyMs, "ms")} / ${num(statsData.avgHostProcessingLatencyMs, "ms")}
streamer processing latency min/max/avg: ${num(statsData.minStreamerProcessingTimeMs, "ms")} / ${num(statsData.maxStreamerProcessingTimeMs, "ms")} / ${num(statsData.avgStreamerProcessingTimeMs, "ms")}
streamer to browser rtt (ws only): ${num(statsData.browserRtt, "ms")}
`
    for (const key in statsData.transport) {
        const value = statsData.transport[key]
        let valuePretty = value

        if (typeof value == "number" && key.endsWith("Ms")) {
            valuePretty = `${num(value, "ms")}`
        }

        text += `${key}: ${valuePretty}\n`
    }

    for (const key in statsData.video) {
        const value = statsData.video[key]
        let valuePretty = value

        if (typeof value == "number" && key.endsWith("Ms")) {
            valuePretty = `${num(value, "ms")}`
        }

        text += `${key}: ${valuePretty}\n`
    }

    for (const key in statsData.audio) {
        const value = statsData.audio[key]
        let valuePretty = value

        if (typeof value == "number" && key.endsWith("Ms")) {
            valuePretty = `${num(value, "ms")}`
        }

        text += `${key}: ${valuePretty}\n`
    }

    return text
}

export class StreamStats {

    private logger: Logger | null = null

    private mode: StreamStatsMode = "off"
    private transport: Transport | null = null
    private statsChannel: DataTransportChannel | null = null
    private updateIntervalId: number | null = null
    private localStatsUpdateRunning = false
    private readonly rawDataListener = (data: ArrayBuffer) => this.onRawData(data)
    private readonly updateLocalStatsListener = () => { void this.updateLocalStats() }

    private videoPipe: Pipe | null = null
    private audioPipe: Pipe | null = null
    private previousWebTransportReceiveSample: {
        transport: Transport
        measuredAtMs: number
        bytes: number
        receivedFrames: number | null
        droppedFrames: number | null
    } | null = null
    private statsData: StreamStatsData = {
        transportImplementation: null,
        videoCodec: null,
        videoWidth: null,
        videoHeight: null,
        videoFps: null,
        videoPipeline: null,
        audioPipeline: null,
        hdrEnabled: null,
        streamerRttMs: null,
        streamerRttVarianceMs: null,
        minHostProcessingLatencyMs: null,
        maxHostProcessingLatencyMs: null,
        avgHostProcessingLatencyMs: null,
        minStreamerProcessingTimeMs: null,
        maxStreamerProcessingTimeMs: null,
        avgStreamerProcessingTimeMs: null,
        browserRtt: null,
        transport: {},
        video: {},
        audio: {}
    }

    constructor(logger?: Logger) {
        if (logger) {
            this.logger = logger
        }
    }

    setTransport(transport: Transport) {
        this.transport = transport
        this.statsData.transportImplementation = transport.implementationName
        this.statsData.transport = {}
        this.statsData.streamerRttMs = null
        this.statsData.streamerRttVarianceMs = null
        this.statsData.minHostProcessingLatencyMs = null
        this.statsData.maxHostProcessingLatencyMs = null
        this.statsData.avgHostProcessingLatencyMs = null
        this.statsData.minStreamerProcessingTimeMs = null
        this.statsData.maxStreamerProcessingTimeMs = null
        this.statsData.avgStreamerProcessingTimeMs = null
        this.statsData.browserRtt = null
        this.previousWebTransportReceiveSample = null

        this.checkEnabled()
    }
    private checkEnabled() {
        if (this.isEnabled()) {
            if (this.statsChannel) {
                this.statsChannel.removeReceiveListener(this.rawDataListener)
                this.statsChannel = null
            }

            if (!this.statsChannel && this.transport) {
                const channel = this.transport.getChannel(TransportChannelId.STATS)
                if (channel.type != "data") {
                    this.logger?.debug(`Failed initialize debug transport channel because type is "${channel.type}" and not "data"`)
                    return
                }
                channel.addReceiveListener(this.rawDataListener)
                this.statsChannel = channel
            }
            if (this.updateIntervalId == null) {
                this.updateIntervalId = setInterval(this.updateLocalStatsListener, 1000)
            }
        } else {
            if (this.statsChannel) {
                this.statsChannel.removeReceiveListener(this.rawDataListener)
                this.statsChannel = null
            }
            if (this.updateIntervalId != null) {
                clearInterval(this.updateIntervalId)
                this.updateIntervalId = null
            }
        }
    }

    setEnabled(enabled: boolean) {
        if (enabled) {
            if (!this.isEnabled()) {
                this.setMode("compact")
            }
        } else {
            this.setMode("off")
        }
    }
    setMode(mode: StreamStatsMode) {
        if (this.mode == mode) {
            return
        }
        this.mode = mode

        this.checkEnabled()
    }
    isEnabled(): boolean {
        return this.mode != "off"
    }
    getMode(): StreamStatsMode {
        return this.mode
    }
    toggle(): StreamStatsMode {
        const nextMode: Record<StreamStatsMode, StreamStatsMode> = {
            off: "compact",
            compact: "advanced",
            advanced: "off",
        }
        this.setMode(nextMode[this.mode])
        return this.mode
    }

    private buffer: ByteBuffer = BIG_BUFFER
    private onRawData(data: ArrayBuffer) {
        this.buffer.reset()
        this.buffer.putU8Array(new Uint8Array(data))

        this.buffer.flip()

        const textLength = this.buffer.getU16()
        const text = this.buffer.getUtf8Raw(textLength)

        const json: StreamerStatsUpdate = JSON.parse(text)
        this.onMessage(json)
    }
    private onMessage(msg: StreamerStatsUpdate) {
        if ("Rtt" in msg) {
            this.statsData.streamerRttMs = msg.Rtt.rtt_ms
            this.statsData.streamerRttVarianceMs = msg.Rtt.rtt_variance_ms
        } else if ("Video" in msg) {
            if (msg.Video.host_processing_latency) {
                this.statsData.minHostProcessingLatencyMs = msg.Video.host_processing_latency.min_host_processing_latency_ms
                this.statsData.maxHostProcessingLatencyMs = msg.Video.host_processing_latency.max_host_processing_latency_ms
                this.statsData.avgHostProcessingLatencyMs = msg.Video.host_processing_latency.avg_host_processing_latency_ms
            } else {
                this.statsData.minHostProcessingLatencyMs = null
                this.statsData.maxHostProcessingLatencyMs = null
                this.statsData.avgHostProcessingLatencyMs = null
            }

            this.statsData.minStreamerProcessingTimeMs = msg.Video.min_streamer_processing_time_ms
            this.statsData.maxStreamerProcessingTimeMs = msg.Video.max_streamer_processing_time_ms
            this.statsData.avgStreamerProcessingTimeMs = msg.Video.avg_streamer_processing_time_ms
        } else if ("BrowserRtt" in msg) {
            this.statsData.browserRtt = msg.BrowserRtt.rtt_ms
        }
    }

    private async updateLocalStats() {
        if (this.localStatsUpdateRunning) {
            return
        }

        this.localStatsUpdateRunning = true
        try {
            await Promise.all([
                this.updateTransportStats(),
                this.updateVideoStats(),
                this.updateAudioStats(),
            ])
        } catch (error) {
            this.logger?.debug(`Failed to collect local stream statistics: ${String(error)}`)
        } finally {
            this.localStatsUpdateRunning = false
        }
    }
    private async updateTransportStats() {
        const transport = this.transport
        if (!transport) {
            console.debug("Cannot query stats without transport")
            return
        }

        const stats = await transport.getStats()
        if (transport != this.transport) {
            return
        }

        const measuredAtMs = performance.now()
        const incomingBytes = finiteStat(stats, "webTransportIncomingBytes")
        const receivedFrames = finiteStat(stats, "webTransportVideoFramesReceived")
        const droppedFrames = finiteStat(stats, "webTransportVideoFramesDropped")
        const previousSample = this.previousWebTransportReceiveSample
        if (
            incomingBytes != null && previousSample?.transport == transport &&
            incomingBytes >= previousSample.bytes && measuredAtMs > previousSample.measuredAtMs
        ) {
            const elapsedSeconds = (measuredAtMs - previousSample.measuredAtMs) / 1000
            stats.webTransportReceiveMbps = (incomingBytes - previousSample.bytes) * 8 / elapsedSeconds / 1_000_000
        }
        if (
            receivedFrames != null && droppedFrames != null &&
            previousSample?.transport == transport &&
            previousSample.receivedFrames != null && previousSample.droppedFrames != null &&
            receivedFrames >= previousSample.receivedFrames && droppedFrames >= previousSample.droppedFrames
        ) {
            const receivedDelta = receivedFrames - previousSample.receivedFrames
            const droppedDelta = droppedFrames - previousSample.droppedFrames
            if (receivedDelta > 0) {
                stats.webTransportVideoDropPercent = Math.min(100, droppedDelta * 100 / receivedDelta)
            }
        }
        this.previousWebTransportReceiveSample = incomingBytes == null
            ? null
            : { transport, measuredAtMs, bytes: incomingBytes, receivedFrames, droppedFrames }

        this.statsData.transport = stats
    }
    private async updateVideoStats() {
        const stats = {}

        if (this.videoPipe && this.videoPipe.reportStats) {
            await this.videoPipe.reportStats(stats)
        }

        this.statsData.video = stats
    }
    private async updateAudioStats() {
        const stats = {}

        if (this.audioPipe && this.audioPipe.reportStats) {
            await this.audioPipe.reportStats(stats)
        }

        this.statsData.audio = stats
    }

    setVideoInfo(codec: string, width: number, height: number, fps: number) {
        this.statsData.videoCodec = codec
        this.statsData.videoWidth = width
        this.statsData.videoHeight = height
        this.statsData.videoFps = fps
    }
    setVideoPipeline(name: string, pipe: Pipe | null) {
        this.statsData.videoPipeline = name
        this.videoPipe = pipe
    }
    setAudioPipeline(name: string, pipe: Pipe | null) {
        this.statsData.audioPipeline = name
        this.audioPipe = pipe
    }
    setHdrEnabled(enabled: boolean) {
        this.statsData.hdrEnabled = enabled
    }

    getCurrentStats(): StreamStatsData {
        return {
            ...this.statsData,
            transport: { ...this.statsData.transport },
            video: { ...this.statsData.video },
            audio: { ...this.statsData.audio },
        }
    }
}
