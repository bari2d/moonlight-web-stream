import { Api } from "../api.js"
import { App, ConnectionStatus, GeneralClientMessage, GeneralServerMessage, StreamCapabilities, StreamClientMessage, StreamPermissions, StreamServerMessage, StreamSettings, TransportChannelId } from "../api_bindings.js"
import { showNotification } from "../component/notification.js"
import { Component } from "../component/index.js"
import { Settings, TransportType } from "../component/settings_menu.js"
import { AudioPlayer } from "./audio/index.js"
import { buildAudioPipeline } from "./audio/pipeline.js"
import { BIG_BUFFER, ByteBuffer } from "./buffer.js"
import { defaultStreamInputConfig, StreamInput } from "./input.js"
import { Logger, LogMessageInfo } from "./log.js"
import { gatherPipeInfo } from "./pipeline/index.js"
import { StreamStats } from "./stats.js"
import { DataTransportChannel, Transport, TransportShutdown } from "./transport/index.js"
import { WebSocketTransport } from "./transport/web_socket.js"
import { WebTransportTransport } from "./transport/web_transport.js"
import { filterTransportCandidates, raceCandidates, transportCandidates } from "./transport/candidates.js"
import { WebRTCTransport } from "./transport/webrtc.js"
import { allVideoCodecs, andVideoCodecs, createSupportedVideoFormatsBits, emptyVideoCodecs, getSelectedVideoCodec, hasAnyCodec, VideoCodecSupport } from "./video.js"
import { VideoRenderer } from "./video/index.js"
import { buildVideoPipeline, VideoPipelineOptions } from "./video/pipeline.js"

export type ExecutionEnvironment = {
    main: boolean
    worker: boolean
}

export type InfoEvent = CustomEvent<
    { type: "app", app: App } |
    { type: "serverMessage", message: string } |
    { type: "connectionComplete", capabilities: StreamCapabilities } |
    { type: "videoReady" } |
    { type: "connectionStatus", status: ConnectionStatus } |
    { type: "addDebugLine", line: string, additional?: LogMessageInfo }
>
export type InfoEventListener = (event: InfoEvent) => void

export function getStreamerSize(settings: Settings, viewerScreenSize: [number, number]): [number, number] {
    let width, height
    if (settings.videoSize == "720p") {
        width = 1280
        height = 720
    } else if (settings.videoSize == "1080p") {
        width = 1920
        height = 1080
    } else if (settings.videoSize == "1440p") {
        width = 2560
        height = 1440
    } else if (settings.videoSize == "4k") {
        width = 3840
        height = 2160
    } else if (settings.videoSize == "custom") {
        width = settings.videoSizeCustom.width
        height = settings.videoSizeCustom.height
    } else { // native
        width = viewerScreenSize[0]
        height = viewerScreenSize[1]
    }
    return [width, height]
}

function getVideoCodecHint(settings: Settings): VideoCodecSupport {
    let videoCodecHint = emptyVideoCodecs()
    if (settings.videoCodec == "h264") {
        videoCodecHint.H264 = true
        videoCodecHint.H264_HIGH8_444 = true
    } else if (settings.videoCodec == "h265") {
        videoCodecHint.H265 = true
        videoCodecHint.H265_MAIN10 = true
        videoCodecHint.H265_REXT8_444 = true
        videoCodecHint.H265_REXT10_444 = true
    } else if (settings.videoCodec == "av1") {
        videoCodecHint.AV1_MAIN8 = true
        videoCodecHint.AV1_MAIN10 = true
        videoCodecHint.AV1_HIGH8_444 = true
        videoCodecHint.AV1_HIGH10_444 = true
    } else if (settings.videoCodec == "auto") {
        videoCodecHint = allVideoCodecs()
    }

    if (isFirefox()) {
        videoCodecHint.AV1_MAIN10 = false
        videoCodecHint.AV1_HIGH10_444 = false
    }

    return videoCodecHint
}

function isFirefox(): boolean {
    return navigator.userAgent.includes("Firefox/")
}

const WEBRTC_CONNECT_TIMEOUT_MS = 15000
const WEBTRANSPORT_CONNECT_TIMEOUT_MS = 3000
const WEBTRANSPORT_ESTABLISHED_RETRY_LIMIT = 2
const WEBTRANSPORT_STABILITY_RESET_MS = 30000
const WEBTRANSPORT_REPROBE_COOLDOWN_MS = 30000
const CONTROL_SOCKET_CLOSE_TIMEOUT_MS = 1000
const MAX_STREAM_CONTROL_JSON_BYTES = 64 * 1024
const STREAM_SERVER_CONTROL_KEYS = new Set([
    "Setup",
    "WebRtc",
    "WebTransportSetup",
    "UpdateApp",
    "DebugLog",
    "ConnectionComplete",
    "ConnectionTerminated",
])

type WebTransportReprobeState = "idle" | "pending" | "probing" | "spent"

export class Stream implements Component {
    private logger: Logger = new Logger()

    private api: Api

    private hostId: number
    private appId: number

    private permissions: StreamPermissions
    private settings: Settings

    private divElement = document.createElement("div")
    private eventTarget = new EventTarget()

    private ws: WebSocket
    private controlGeneration = 0
    private controlSocketGeneration = 0
    private startConnectionTask: { generation: number, promise: Promise<void> } | null = null
    private controlRestartTask: Promise<void> | null = null
    private readonly retiringControlSockets = new WeakSet<WebSocket>()
    private controlRecoveryGeneration: number | null = null
    private stopping = false
    private iceServers: Array<RTCIceServer> | null = null
    private webTransportUrls: string[] = []
    private webTransportAttempt: AbortController | null = null
    private transportOverride: TransportType | null = null
    private webTransportEstablishedRetryCount = 0
    private webTransportStabilityTimer: number | null = null
    private webTransportStabilityTransport: WebTransportTransport | null = null
    private webTransportReprobeTimer: number | null = null
    private webTransportReprobeTransport: WebSocketTransport | null = null
    private webTransportReprobeDeadline: number | null = null
    private webTransportReprobeState: WebTransportReprobeState = "idle"
    private webTransportControl: {
        transport: WebTransportTransport,
        channel: DataTransportChannel,
        generation: number,
        listener: (data: ArrayBuffer) => void,
    } | null = null
    private readonly streamControlEncoder = new TextEncoder()
    private readonly streamControlDecoder = new TextDecoder("utf-8", { fatal: true })

    private videoRenderer: VideoRenderer | null = null
    private audioPlayer: AudioPlayer | null = null

    private input: StreamInput
    private stats: StreamStats

    private streamerSize: [number, number]
    private hasConnectionComplete = false
    private connectionCompleteEpoch = 0
    private connectionCompleteSetup: Promise<void> = Promise.resolve()
    // Media setup that last completed, so a replacement native connection
    // (adaptive bitrate) with identical parameters can tolerate a re-setup
    // failure instead of tearing the whole transport down.
    private lastMediaSetupKey: string | null = null
    // Bitrate the host settled on after adaptive reductions. Later
    // (re)connections in this tab start here instead of the full setting.
    private adaptiveBitrateCapKbps: number | null = readAdaptiveBitrateCap()
    private hasVideoReady = false
    private hasDispatchedVideoReady = false

    constructor(api: Api, hostId: number, appId: number, settings: Settings, viewerScreenSize: [number, number], permissions: StreamPermissions) {
        this.logger.addInfoListener((info, type) => {
            this.debugLog(info, { type: type ?? undefined })
        })

        this.api = api

        this.hostId = hostId
        this.appId = appId

        this.permissions = permissions
        this.settings = settings

        this.streamerSize = getStreamerSize(settings, viewerScreenSize)

        this.ws = this.createControlWebSocket(this.controlGeneration)
        this.sendInitMessage(this.controlGeneration)

        // Stream Input
        const streamInputConfig = defaultStreamInputConfig()
        Object.assign(streamInputConfig, {
            mouseMode: this.settings.mouseMode,
            mouseScrollMode: this.settings.mouseScrollMode,
            touchMode: this.settings.touchMode,
            localCursorSensitivity: this.settings.localCursorSensitivity,
            controllerConfig: this.settings.controllerConfig
        })
        this.input = new StreamInput(streamInputConfig)

        // Stream Stats
        this.stats = new StreamStats(this.logger)
    }

    private debugLog(message: string, additional?: LogMessageInfo) {
        for (const line of message.split("\n")) {
            const event: InfoEvent = new CustomEvent("stream-info", {
                detail: { type: "addDebugLine", line, additional }
            })

            this.eventTarget.dispatchEvent(event)
        }
    }
    private resetVideoReadyState() {
        this.lastMediaSetupKey = null
        this.connectionCompleteEpoch++
        this.hasConnectionComplete = false
        this.hasVideoReady = false
        this.hasDispatchedVideoReady = false
    }
    private markConnectionComplete() {
        this.hasConnectionComplete = true
        if (this.transport instanceof WebSocketTransport) {
            this.armWebTransportReprobeAfterWebSocketStability(this.transport, this.controlGeneration)
        }
        this.tryDispatchVideoReady()
    }
    private markVideoReady() {
        this.hasVideoReady = true
        this.tryDispatchVideoReady()
    }
    private tryDispatchVideoReady() {
        if (!this.hasConnectionComplete || !this.hasVideoReady || this.hasDispatchedVideoReady) {
            return
        }

        this.hasDispatchedVideoReady = true
        const event: InfoEvent = new CustomEvent("stream-info", {
            detail: { type: "videoReady" }
        })
        this.eventTarget.dispatchEvent(event)
    }

    private isCurrentControlGeneration(generation: number): boolean {
        return !this.stopping && this.controlGeneration == generation
    }

    private async onMessage(message: StreamServerMessage, generation: number) {
        if (!this.isCurrentControlGeneration(generation)) {
            return
        }

        if ("DebugLog" in message) {
            const debugLog = message.DebugLog
            // Sent by the streamer (restart_with_adaptive_bitrate) after a
            // successful reduction.
            const adaptive = /^Adaptive bitrate adjusted the stream from \d+ to (\d+) Kbps/.exec(debugLog.message)
            if (adaptive) {
                this.adaptiveBitrateCapKbps = Number.parseInt(adaptive[1])
                writeAdaptiveBitrateCap(this.adaptiveBitrateCapKbps)
            }

            this.debugLog(debugLog.message, {
                type: debugLog.ty ?? undefined
            })
        } else if ("UpdateApp" in message) {
            const event: InfoEvent = new CustomEvent("stream-info", {
                detail: { type: "app", app: message.UpdateApp.app }
            })

            this.eventTarget.dispatchEvent(event)
        } else if ("ConnectionComplete" in message) {
            // Adaptive bitrate replaces only the native Moonlight connection,
            // so the browser transport stays up and can receive another
            // ConnectionComplete for the same Stream instance.
            const completionEpoch = this.connectionCompleteEpoch
            const capabilities = message.ConnectionComplete.capabilities
            const formatRaw = message.ConnectionComplete.format
            const width = message.ConnectionComplete.width
            const height = message.ConnectionComplete.height
            const fps = message.ConnectionComplete.fps

            const audioSampleRate = message.ConnectionComplete.audio_sample_rate
            const audioChannelCount = message.ConnectionComplete.audio_channel_count
            const audioStreams = message.ConnectionComplete.audio_streams
            const audioCoupledStreams = message.ConnectionComplete.audio_coupled_streams
            const audioSamplesPerFrame = message.ConnectionComplete.audio_samples_per_frame
            const audioMapping = message.ConnectionComplete.audio_mapping

            const format = getSelectedVideoCodec(formatRaw)
            if (format == null) {
                this.debugLog(`Video Format ${formatRaw} was not found! Couldn't start stream!`, { type: "fatal" })
                return
            }

            const event: InfoEvent = new CustomEvent("stream-info", {
                detail: { type: "connectionComplete", capabilities }
            })

            this.eventTarget.dispatchEvent(event)

            this.input.onStreamStart(capabilities, [width, height])

            this.stats.setVideoInfo(format ?? "Unknown", width, height, fps)
            // HDR state will be set when server sends HdrModeUpdate message
            // Don't initialize from settings.hdr because that's just the user's preference,
            // not the actual HDR state (which depends on host support, display, and codec)
            if (this.settings.hdr) {
                this.debugLog("HDR requested by user, waiting for host confirmation...")
            }

            // we should allow streaming without audio
            if (!this.audioPlayer) {
                showNotification("Failed to find supported audio player -> audio is missing.")
            }

            if (!this.videoRenderer || !this.audioPlayer) {
                throw "Video renderer or audio player not initialized!"
            }
            const videoRenderer = this.videoRenderer
            const audioPlayer = this.audioPlayer

            // WebSocket message callbacks are not awaited by the browser. Keep
            // repeated native-connection setups in receipt order so a slow TV
            // cannot finish the initial probe after a reconnect setup and reset
            // the decoder a second time behind the recovery IDR.
            const setupResult = this.connectionCompleteSetup.then(async () => {
                if (!this.isCurrentControlGeneration(generation)) {
                    return
                }
                const setupKey = JSON.stringify([
                    format, fps, width, height,
                    audioSampleRate, audioChannelCount, audioStreams,
                    audioCoupledStreams, audioSamplesPerFrame, audioMapping,
                ])
                // Run each setup (sync or async) and capture its failure
                // instead of letting the first one reject the whole batch.
                const settle = (run: () => void | Promise<void>) =>
                    Promise.resolve().then(run).then(() => null, (error: unknown) => String(error))
                const results = await Promise.all([
                    settle(() => videoRenderer.setup({
                        codec: format,
                        fps,
                        width,
                        height,
                    })),
                    settle(() => audioPlayer.setup({
                        sampleRate: audioSampleRate,
                        channels: audioChannelCount,
                        streams: audioStreams,
                        coupledStreams: audioCoupledStreams,
                        samplesPerFrame: audioSamplesPerFrame,
                        mapping: audioMapping,
                    })),
                ])
                const failures: string[] = []
                if (results[0] != null) failures.push(`video: ${results[0]}`)
                if (results[1] != null) failures.push(`audio: ${results[1]}`)
                if (failures.length > 0) {
                    if (this.lastMediaSetupKey != setupKey) {
                        throw new Error(`Media setup failed: ${failures.join("; ")}`)
                    }
                    // Same codec/size/audio layout as the running pipeline
                    // (e.g. an adaptive bitrate reconnect). Keep it; the IDR
                    // requested below resynchronizes the decoder. iOS
                    // Safari's worker pipeline rejects re-setup of a live
                    // pipeline, which used to kill the whole transport.
                    this.debugLog(`Media re-setup failed for an unchanged stream; keeping the existing pipeline (${failures.join("; ")})`)
                }
                this.lastMediaSetupKey = setupKey
            })
            this.connectionCompleteSetup = setupResult.catch(() => { })
            await setupResult
            if (
                !this.isCurrentControlGeneration(generation) ||
                this.connectionCompleteEpoch != completionEpoch
            ) {
                // A transport fallback replaced this connection while its
                // decoder setup was pending. Never let the stale completion
                // mark the fresh transport ready or request on its channel.
                return
            }

            // setup() resets the data decoder. On both initial startup and a
            // replacement connection, the host's startup IDR may pass through
            // while setup is still pending. Request one after every completed
            // setup through WebTransport's deduplicated, retrying recovery path.
            const video = this.transport?.getChannel(TransportChannelId.HOST_VIDEO)
            if (video?.type == "data") {
                this.requestHostVideoIdr(video)
            }

            this.markConnectionComplete()
        } else if ("ConnectionTerminated" in message) {
            const code = message.ConnectionTerminated.error_code

            this.debugLog(`ConnectionTerminated with code ${code}`, { type: "fatalDescription" })
        }
        // The authenticated control socket provides a one-time WebTransport URL
        // before the streamer Setup message triggers transport selection.
        else if ("WebTransportSetup" in message) {
            const candidates = transportCandidates(message.WebTransportSetup.url, message.WebTransportSetup.urls)
            this.webTransportUrls = filterTransportCandidates(
                candidates,
                this.settings.webTransportHost,
                this.settings.webTransportPort,
            )
            this.debugLog(
                `WebTransport route preference selected ${this.webTransportUrls.length}/${candidates.length} endpoint(s): host=${this.settings.webTransportHost}, port=${this.settings.webTransportPort}`,
            )
        }
        // -- WebRTC Config
        else if ("Setup" in message) {
            const iceServers = message.Setup.ice_servers

            this.iceServers = iceServers

            this.debugLog(`window.isSecureContext: ${window.isSecureContext}`)
            this.debugLog(`Using WebRTC Ice Servers: ${createPrettyList(
                iceServers.map(server => server.urls).reduce((list, url) => list.concat(url), [])
            )}`)

            await this.startConnection(generation)
        }
        // -- WebRTC
        else if ("WebRtc" in message) {
            const webrtcMessage = message.WebRtc
            if (this.transport instanceof WebRTCTransport) {
                this.transport.onReceiveMessage(webrtcMessage)
            } else {
                this.debugLog(`Received WebRTC message but transport is currently ${this.transport?.implementationName}`)
            }
        }
    }

    async startConnection(generation = this.controlGeneration): Promise<void> {
        if (!this.isCurrentControlGeneration(generation)) {
            return
        }

        const active = this.startConnectionTask
        if (active) {
            try {
                await active.promise
            } catch (error) {
                if (active.generation == generation) {
                    throw error
                }
            }
            if (active.generation == generation || !this.isCurrentControlGeneration(generation)) {
                return
            }
            return this.startConnection(generation)
        }

        const promise = this.runStartConnection(generation)
        this.startConnectionTask = { generation, promise }
        try {
            await promise
        } finally {
            if (this.startConnectionTask?.promise == promise) {
                this.startConnectionTask = null
            }
        }
    }

    private async runStartConnection(generation: number): Promise<void> {
        if (!this.isCurrentControlGeneration(generation)) {
            return
        }
        this.debugLog(`Permissions: ${JSON.stringify(this.permissions)}`)

        const desiredTransport = this.transportOverride ?? this.settings.dataTransport
        this.debugLog(`Using transport: ${desiredTransport}`)

        if (desiredTransport == "auto") {
            const shutdownReason = await this.tryWebTransport(generation)

            if (!this.isCurrentControlGeneration(generation)) {
                return
            }

            if (shutdownReason == "failednoconnect" && this.webTransportEstablishedRetryCount == 0) {
                this.debugLog(
                    `Initial WebTransport connection was unavailable. Falling back immediately to WebSocket transport; established-session retry budget remains 0/${WEBTRANSPORT_ESTABLISHED_RETRY_LIMIT}.`,
                    { type: "ifErrorDescription" },
                )
                await this.restartWithFreshTransportFallback("websocket", generation)
                return
            }
            await this.retryWebTransportOrFallback(shutdownReason, generation)
            return
        } else if (desiredTransport == "webtransport") {
            const shutdownReason = await this.tryWebTransport(generation)
            if (!this.isCurrentControlGeneration(generation)) {
                return
            }
            if (
                shutdownReason == "failednoconnect" ||
                shutdownReason == "failed" ||
                shutdownReason == "disconnect"
            ) {
                await this.retryWebTransportOrFallback(shutdownReason, generation)
                return
            }
        } else if (desiredTransport == "webrtc") {
            await this.tryWebRTCTransport(generation)
            if (!this.isCurrentControlGeneration(generation)) {
                return
            }
        } else if (desiredTransport == "websocket") {
            const shutdownReason = await this.tryWebSocketTransport(generation)
            if (!this.isCurrentControlGeneration(generation)) {
                return
            }
            if (shutdownReason == "failed" || shutdownReason == "disconnect") {
                const reconnectTransport = this.selectWebSocketReconnectTransport()
                this.debugLog(
                    `WebSocket transport ${shutdownReason == "failed" ? "failed" : "disconnected"}. Reconnecting with a fresh control socket using ${reconnectTransport}.`,
                    { type: "ifErrorDescription" },
                )
                await this.restartWithFreshTransportFallback(reconnectTransport, generation)
                return
            }
        }

        this.debugLog("Tried all configured transport options but no connection was possible", { type: "fatal" })
    }

    private transport: Transport | null = null

    private isCurrentControlSocket(ws: WebSocket, generation: number): boolean {
        return (
            this.isCurrentControlGeneration(generation) &&
            this.controlSocketGeneration == generation &&
            this.ws === ws
        )
    }

    private createControlWebSocket(generation: number): WebSocket {
        const wsApiHost = this.api.host_url.replace(/^http(s)?:/, "ws$1:")
        const ws = new WebSocket(`${wsApiHost}/host/stream`)

        ws.addEventListener("error", (event) => {
            if (!this.isCurrentControlSocket(ws, generation)) {
                return
            }
            if (this.hasActiveWebTransportControl(generation)) {
                return
            }
            this.onError(event)
        })
        ws.addEventListener("open", () => {
            if (!this.isCurrentControlSocket(ws, generation)) {
                return
            }
            this.onWsOpen(ws, generation)
        })
        ws.addEventListener("close", (event) => {
            if (this.retiringControlSockets.delete(ws)) {
                return
            }
            if (!this.isCurrentControlSocket(ws, generation)) {
                return
            }
            this.onWsClose(ws, generation, event)
        })
        ws.addEventListener("message", (event) => {
            if (!this.isCurrentControlSocket(ws, generation)) {
                return
            }
            this.onRawWsMessage(event, generation)
        })

        return ws
    }
    private sendInitMessage(generation: number) {
        this.sendWsMessage({
            Init: {
                host_id: this.hostId,
                app_id: this.appId,
                video_frame_queue_size: this.settings.videoFrameQueueSize,
                audio_sample_queue_size: this.settings.audioSampleQueueSize,
            }
        }, generation)
    }
    private async retryWebTransportOrFallback(
        reason: TransportShutdown,
        generation: number,
    ): Promise<void> {
        if (!this.isCurrentControlGeneration(generation)) {
            return
        }

        if (this.webTransportReprobeState == "probing") {
            this.webTransportReprobeState = "spent"
            this.debugLog(
                `Controlled WebTransport re-probe ended after ${reason}; returning to WebSocket without scheduling another automatic probe.`,
                { type: "ifErrorDescription" },
            )
            await this.restartWithFreshTransportFallback("websocket", generation)
            return
        }

        if (this.webTransportEstablishedRetryCount < WEBTRANSPORT_ESTABLISHED_RETRY_LIMIT) {
            this.webTransportEstablishedRetryCount++
            this.debugLog(
                `WebTransport recovery retry ${this.webTransportEstablishedRetryCount}/${WEBTRANSPORT_ESTABLISHED_RETRY_LIMIT} after ${reason}; reconnecting with a fresh authenticated control WebSocket and WebTransport token.`,
                { type: "ifErrorDescription" },
            )
            await this.restartWithFreshTransportFallback("webtransport", generation)
            return
        }

        if (this.webTransportReprobeState == "idle") {
            this.webTransportReprobeState = "pending"
        }
        this.debugLog(
            `WebTransport recovery retry budget exhausted (${this.webTransportEstablishedRetryCount}/${WEBTRANSPORT_ESTABLISHED_RETRY_LIMIT}) after ${reason}; falling back to WebSocket${this.webTransportReprobeState == "pending" ? " with one controlled re-probe after stability" : ""}.`,
            { type: "ifErrorDescription" },
        )
        await this.restartWithFreshTransportFallback("websocket", generation)
    }
    private armWebTransportStabilityReset(
        transport: WebTransportTransport,
        generation: number,
    ): void {
        this.clearWebTransportStabilityTimer()
        this.webTransportStabilityTransport = transport
        this.webTransportStabilityTimer = window.setTimeout(() => {
            if (
                !this.isCurrentControlGeneration(generation) ||
                this.webTransportStabilityTransport != transport
            ) {
                return
            }
            this.webTransportStabilityTimer = null
            this.webTransportStabilityTransport = null
            if (this.transport != transport) {
                return
            }

            const previousRetryCount = this.webTransportEstablishedRetryCount
            const previousReprobeState = this.webTransportReprobeState
            this.webTransportEstablishedRetryCount = 0
            this.webTransportReprobeState = "idle"
            this.clearWebTransportReprobeTimer()
            this.debugLog(
                `WebTransport remained stable for ${WEBTRANSPORT_STABILITY_RESET_MS}ms; retry budget reset from ${previousRetryCount}/${WEBTRANSPORT_ESTABLISHED_RETRY_LIMIT} to 0/${WEBTRANSPORT_ESTABLISHED_RETRY_LIMIT} and re-probe state reset from ${previousReprobeState} to idle.`,
            )
        }, WEBTRANSPORT_STABILITY_RESET_MS)
    }
    private clearWebTransportStabilityTimer(transport?: WebTransportTransport): void {
        if (transport && this.webTransportStabilityTransport != transport) {
            return
        }
        if (this.webTransportStabilityTimer != null) {
            window.clearTimeout(this.webTransportStabilityTimer)
            this.webTransportStabilityTimer = null
        }
        this.webTransportStabilityTransport = null
    }
    private armWebTransportReprobeAfterWebSocketStability(
        transport: WebSocketTransport,
        generation: number,
    ): void {
        if (
            this.webTransportReprobeState != "pending" ||
            this.transportOverride != "websocket" ||
            (
                this.webTransportReprobeTransport == transport &&
                this.webTransportReprobeTimer != null
            )
        ) {
            return
        }

        this.clearWebTransportReprobeTimer()
        this.webTransportReprobeTransport = transport
        this.webTransportReprobeDeadline = performance.now() + WEBTRANSPORT_REPROBE_COOLDOWN_MS
        this.webTransportReprobeTimer = window.setTimeout(() => {
            if (
                !this.isCurrentControlGeneration(generation) ||
                this.transport != transport ||
                this.webTransportReprobeTransport != transport ||
                this.transportOverride != "websocket" ||
                this.webTransportReprobeState != "pending"
            ) {
                return
            }

            this.webTransportReprobeTimer = null
            this.webTransportReprobeTransport = null
            this.webTransportReprobeDeadline = null
            const notifyShutdown = transport.onclose
            if (!notifyShutdown) {
                return
            }
            this.webTransportReprobeState = "probing"
            const probeTransport = this.settings.dataTransport == "webtransport"
                ? "webtransport"
                : "auto"
            this.debugLog(
                `WebSocket fallback remained stable for ${WEBTRANSPORT_REPROBE_COOLDOWN_MS}ms; starting the one controlled ${probeTransport} re-probe.`,
            )
            // Resolve the existing WebSocket connection task instead of
            // starting a competing restart. Its single-flight shutdown path
            // consumes the probing state and owns the fresh control generation.
            notifyShutdown("disconnect")
        }, WEBTRANSPORT_REPROBE_COOLDOWN_MS)
    }
    private clearWebTransportReprobeTimer(): void {
        if (this.webTransportReprobeTimer != null) {
            window.clearTimeout(this.webTransportReprobeTimer)
            this.webTransportReprobeTimer = null
        }
        this.webTransportReprobeTransport = null
        this.webTransportReprobeDeadline = null
    }
    private selectWebSocketReconnectTransport(): TransportType {
        if (
            this.webTransportReprobeState == "pending" &&
            this.webTransportReprobeDeadline != null &&
            performance.now() >= this.webTransportReprobeDeadline
        ) {
            this.webTransportReprobeState = "probing"
            this.webTransportReprobeDeadline = null
            this.debugLog("The stable-WebSocket re-probe deadline elapsed while browser timers were delayed; probing WebTransport on this reconnect.")
        }
        if (this.settings.dataTransport == "webtransport") {
            this.webTransportReprobeState = "probing"
            this.debugLog("Forced WebTransport mode: probing WebTransport on this natural WebSocket reconnect.")
            return "webtransport"
        }
        if (this.settings.dataTransport == "auto" && this.webTransportReprobeState == "probing") {
            this.debugLog("Auto transport is consuming its controlled WebTransport re-probe.")
            return "auto"
        }
        return "websocket"
    }
    private restartWithFreshTransportFallback(
        transport: TransportType,
        sourceGeneration = this.controlGeneration,
    ): Promise<void> {
        if (this.stopping) {
            return Promise.resolve()
        }
        if (!this.isCurrentControlGeneration(sourceGeneration)) {
            return Promise.resolve()
        }
        if (this.controlRestartTask) {
            return this.controlRestartTask
        }

        const task = this.performRestartWithFreshTransportFallback(transport, sourceGeneration)
        this.controlRestartTask = task
        return this.waitForControlRestart(task)
    }

    private async waitForControlRestart(task: Promise<void>): Promise<void> {
        try {
            await task
        } finally {
            if (this.controlRestartTask == task) {
                this.controlRestartTask = null
            }
        }
    }

    private async performRestartWithFreshTransportFallback(
        transport: TransportType,
        sourceGeneration: number,
    ): Promise<void> {
        if (!this.isCurrentControlGeneration(sourceGeneration)) {
            return
        }

        // Reserve the replacement generation synchronously, before any close
        // or reconnect await. Every callback and continuation belonging to
        // the old control socket becomes stale at this point.
        const replacementGeneration = sourceGeneration + 1
        this.controlGeneration = replacementGeneration
        this.transportOverride = transport
        this.resetVideoReadyState()
        this.clearWebTransportStabilityTimer()
        this.clearWebTransportReprobeTimer()
        this.connectionCompleteSetup = Promise.resolve()
        // WebTransport URLs are one-use authenticated tokens. Retire the old
        // socket's setup state at the same instant as its generation.
        this.webTransportAttempt?.abort()
        this.webTransportUrls = []
        this.iceServers = null
        this.wsSendBuffer.length = 0
        this.clearWebTransportControl()

        const oldWs = this.ws
        this.retiringControlSockets.add(oldWs)

        if (this.transport) {
            await this.transport.close()
            this.transport = null
        }

        await this.retireControlSocket(oldWs)

        if (!this.isCurrentControlGeneration(replacementGeneration)) {
            return
        }

        this.ws = this.createControlWebSocket(replacementGeneration)
        this.controlSocketGeneration = replacementGeneration
        this.sendInitMessage(replacementGeneration)
    }

    private async retireControlSocket(ws: WebSocket): Promise<void> {
        if (ws.readyState == WebSocket.CLOSED) {
            return
        }

        const closeObserved = await new Promise<boolean>((resolve) => {
            let settled = false
            let timeout: number | null = null
            const finish = (observed: boolean) => {
                if (settled) {
                    return
                }
                settled = true
                if (timeout != null) {
                    window.clearTimeout(timeout)
                }
                ws.removeEventListener("close", onClose)
                resolve(observed)
            }
            const onClose = () => finish(true)

            // Register before close() so even an unusually fast implementation
            // cannot force every reconnect through the timeout path.
            ws.addEventListener("close", onClose, { once: true })
            timeout = window.setTimeout(
                () => finish(false),
                CONTROL_SOCKET_CLOSE_TIMEOUT_MS,
            )
            try {
                if (ws.readyState == WebSocket.OPEN || ws.readyState == WebSocket.CONNECTING) {
                    ws.close()
                }
            } catch (_error) { }
            if (ws.readyState == WebSocket.CLOSED) {
                finish(true)
            }
        })

        if (!closeObserved) {
            this.debugLog(
                `Old control WebSocket did not close within ${CONTROL_SOCKET_CLOSE_TIMEOUT_MS}ms; continuing with the fresh generation.`,
                { type: "ifErrorDescription" },
            )
        }
    }

    private setTransport(transport: Transport) {
        if (this.transport) {
            this.transport.close()
        }

        this.transport = transport
        if (this.webTransportControl?.transport != transport) {
            this.clearWebTransportControl()
        }

        this.input.setTransport(this.transport)
        this.stats.setTransport(this.transport)

        const rtt = this.transport.getChannel(TransportChannelId.RTT)
        if (rtt.type == "data") {
            rtt.addReceiveListener((data) => {
                const buffer = new ByteBuffer(data.byteLength)
                buffer.putU8Array(new Uint8Array(data))
                buffer.flip()

                const ty = buffer.getU8()
                if (ty == 0) {
                    rtt.send(data)
                }
            })
        } else {
            this.debugLog("Failed to get rtt as data transport channel. Cannot respond to rtt packets")
        }

        // Setup GENERAL channel listener for HDR mode updates
        const generalChannel = this.transport.getChannel(TransportChannelId.GENERAL)
        this.debugLog(`[GENERAL] Setting up GENERAL channel listener, type=${generalChannel.type}`)
        if (generalChannel.type === "data") {
            generalChannel.addReceiveListener((data: ArrayBuffer) => {
                this.onGeneralChannelMessage(data)
            })
            this.debugLog(`[GENERAL] GENERAL channel listener registered`)
        } else {
            this.debugLog(`[GENERAL] Cannot register listener, channel type is not 'data'`)
        }
    }

    private bindWebTransportControl(
        transport: WebTransportTransport,
        generation: number,
    ): boolean {
        this.clearWebTransportControl()

        const channel = transport.getChannel(TransportChannelId.STREAM_CONTROL)
        if (channel.type != "data") {
            this.debugLog("WebTransport STREAM_CONTROL is not a data channel", { type: "fatalDescription" })
            return false
        }

        const listener = (data: ArrayBuffer) => {
            const active = this.webTransportControl
            if (
                !active ||
                active.transport != transport ||
                active.channel != channel ||
                active.listener != listener ||
                active.generation != generation ||
                !this.isCurrentControlGeneration(generation) ||
                this.transport != transport
            ) {
                return
            }
            if (data.byteLength == 0 || data.byteLength > MAX_STREAM_CONTROL_JSON_BYTES) {
                throw new Error("Invalid WebTransport STREAM_CONTROL payload size")
            }

            const raw = this.streamControlDecoder.decode(new Uint8Array(data))
            const parsed: unknown = JSON.parse(raw)
            if (typeof parsed != "object" || parsed == null || Array.isArray(parsed)) {
                throw new Error("Invalid WebTransport STREAM_CONTROL JSON value")
            }
            const keys = Object.keys(parsed)
            if (keys.length != 1 || !STREAM_SERVER_CONTROL_KEYS.has(keys[0])) {
                throw new Error("Disallowed WebTransport STREAM_CONTROL message")
            }

            void this.onMessage(parsed as StreamServerMessage, generation).catch((error) => {
                const current = this.webTransportControl
                if (
                    current?.transport == transport &&
                    current.channel == channel &&
                    current.listener == listener &&
                    current.generation == generation &&
                    this.transport == transport
                ) {
                    // Synchronous framing/JSON failures already escape through
                    // the receive loop. Do the same for shape errors discovered
                    // after onMessage awaits decoder/audio setup so an invalid
                    // control frame cannot leave an apparently live, stuck QUIC
                    // session after the setup WebSocket has been retired.
                    transport.failStreamControl(error)
                }
            })
        }
        channel.addReceiveListener(listener)
        this.webTransportControl = { transport, channel, generation, listener }
        return true
    }

    private clearWebTransportControl(transport?: WebTransportTransport): void {
        const active = this.webTransportControl
        if (!active || (transport && active.transport != transport)) {
            return
        }
        active.channel.removeReceiveListener(active.listener)
        this.webTransportControl = null
    }

    private hasActiveWebTransportControl(generation: number): boolean {
        const active = this.webTransportControl
        return (
            !!active &&
            active.generation == generation &&
            active.transport == this.transport &&
            this.isCurrentControlGeneration(generation)
        )
    }

    private sendWebTransportControl(
        transport: WebTransportTransport,
        message: StreamClientMessage,
        generation: number,
    ): boolean {
        const active = this.webTransportControl
        if (
            !active ||
            active.transport != transport ||
            active.transport != this.transport ||
            active.generation != generation ||
            !this.isCurrentControlGeneration(generation)
        ) {
            return false
        }

        const payload = this.streamControlEncoder.encode(JSON.stringify(message))
        if (payload.byteLength == 0 || payload.byteLength > MAX_STREAM_CONTROL_JSON_BYTES) {
            this.debugLog("Refusing an invalid WebTransport STREAM_CONTROL payload", { type: "fatalDescription" })
            return false
        }

        // STREAM_CONTROL uses the ordinary reliable lane, where control frames
        // are strict ordering barriers and are never snapshot-coalesced.
        active.channel.send(payload.buffer)
        return true
    }

    private sendStreamControl(
        message: StreamClientMessage,
        generation: number,
    ): boolean {
        if (this.transport instanceof WebTransportTransport) {
            return this.sendWebTransportControl(this.transport, message, generation)
        }
        this.sendWsMessage(message, generation)
        return true
    }

    private onGeneralChannelMessage(data: ArrayBuffer) {
        this.debugLog(`[GENERAL] Received message on GENERAL channel, size=${data.byteLength}`)
        const buffer = new Uint8Array(data)
        if (buffer.length < 2) {
            this.debugLog(`[GENERAL] Message too short: ${buffer.length} bytes`)
            return
        }

        const textLength = (buffer[0] << 8) | buffer[1]
        if (buffer.length < 2 + textLength) {
            this.debugLog(`[GENERAL] Message incomplete: expected ${2 + textLength} bytes, got ${buffer.length}`)
            return
        }

        const text = new TextDecoder().decode(buffer.slice(2, 2 + textLength))
        this.debugLog(`[GENERAL] Parsed message: ${text}`)
        try {
            const message: GeneralServerMessage = JSON.parse(text)
            this.handleGeneralMessage(message)
        } catch (err) {
            this.debugLog(`Failed to parse general message: ${err}`)
        }
    }

    private handleGeneralMessage(message: GeneralServerMessage) {
        if ("HdrModeUpdate" in message) {
            const hdrUpdate = message.HdrModeUpdate
            if (hdrUpdate) {
                const enabled = hdrUpdate.enabled
                this.debugLog(`HDR mode ${enabled ? "enabled" : "disabled"}`)
                this.setHdrMode(enabled)
            }
        } else if ("ConnectionStatusUpdate" in message) {
            const statusUpdate = message.ConnectionStatusUpdate
            if (statusUpdate) {
                const status = statusUpdate.status
                const event: InfoEvent = new CustomEvent("stream-info", {
                    detail: { type: "connectionStatus", status }
                })
                this.eventTarget.dispatchEvent(event)
            }
        }
    }

    private setHdrMode(enabled: boolean) {
        this.stats.setHdrEnabled(enabled)
        if (this.videoRenderer) {
            if ("setHdrMode" in this.videoRenderer && typeof this.videoRenderer.setHdrMode === "function") {
                this.videoRenderer.setHdrMode(enabled)
            }
        }
    }

    private sendGeneralMessage(message: GeneralClientMessage): boolean {
        const general = this.transport?.getChannel(TransportChannelId.GENERAL)

        if (!general || general.type != "data") {
            return false
        }

        const text = JSON.stringify(message)

        const buffer = BIG_BUFFER
        buffer.reset()
        buffer.putU16(text.length)
        buffer.putUtf8Raw(text)
        buffer.flip()

        general.send(buffer.getRemainingBuffer().buffer)

        return true
    }

    private async tryWebRTCTransport(generation: number): Promise<TransportShutdown> {
        if (!this.permissions.allow_transport_webrtc) {
            this.debugLog("Not trying WebRTC transport because permissions disallow it")
            return "failednoconnect"
        }

        this.debugLog("Trying WebRTC transport")

        this.sendWsMessage({
            SetTransport: "WebRTC"
        }, generation)

        if (!this.iceServers) {
            this.debugLog(`Failed to try WebRTC Transport: no ice servers available`)
            return "failednoconnect"
        }

        const transport = new WebRTCTransport(this.logger)
        transport.onsendmessage = (message) => this.sendWsMessage({ WebRtc: message }, generation)

        transport.initPeer({
            iceServers: this.iceServers
        })
        this.setTransport(transport)

        const videoCodecSupport = await this.createPipelines()
        if (!this.isCurrentControlGeneration(generation)) {
            await transport.close()
            return "disconnect"
        }
        if (!videoCodecSupport) {
            this.debugLog("No video pipeline was found for the codec that was specified. If you're unsure which codecs are supported use H264.", { type: "fatalDescription" })

            await transport.close()
            return "failednoconnect"
        }

        // Starting the stream will start negotiation
        await this.startStream(videoCodecSupport, generation)

        // Wait for negotiation, but don't let a stuck ICE check block fallback forever.
        const result = await new Promise<boolean>((resolve) => {
            const timeout = window.setTimeout(async () => {
                this.debugLog(`WebRTC negotiation timed out after ${WEBRTC_CONNECT_TIMEOUT_MS}ms`)
                transport.onconnect = null
                transport.onclose = null
                await transport.close()
                resolve(false)
            }, WEBRTC_CONNECT_TIMEOUT_MS)

            transport.onconnect = () => {
                window.clearTimeout(timeout)
                resolve(true)
            }
            transport.onclose = () => {
                window.clearTimeout(timeout)
                resolve(false)
            }
        })
        if (!this.isCurrentControlGeneration(generation)) {
            await transport.close()
            return "disconnect"
        }
        this.debugLog(`WebRTC negotiation success: ${result}`)

        if (!result) {
            return "failednoconnect"
        }

        return new Promise((resolve) => {
            transport.onclose = (shutdown) => {
                resolve(shutdown)
            }
        })
    }
    private async tryWebTransport(generation: number): Promise<TransportShutdown> {
        if (!this.permissions.allow_transport_websockets) {
            this.debugLog("Not trying WebTransport because permissions disallow browser relay transports")
            return "failednoconnect"
        }
        if (this.webTransportUrls.length == 0) {
            this.debugLog("WebTransport is not enabled on this server")
            return "failednoconnect"
        }
        if (!WebTransportTransport.isSupported()) {
            this.debugLog("This browser does not support WebTransport")
            return "failednoconnect"
        }

        this.debugLog(`Trying ${this.webTransportUrls.length} WebTransport endpoint(s)`)
        this.webTransportAttempt?.abort()
        const attempt = new AbortController()
        this.webTransportAttempt = attempt
        const winner = await raceCandidates(this.webTransportUrls, url => {
            const transport = new WebTransportTransport(url, this.logger)
            const shutdown = new Promise<TransportShutdown>(resolve => {
                transport.onclose = reason => {
                    transport.onclose = null
                    resolve(reason)
                }
            })
            return {
                transport, shutdown,
                connect: (timeout: number) => transport.connect(timeout),
                close: () => transport.close(),
            }
        }, attempt.signal, (url, result) => {
            // Only log host/port: the query contains a one-use bearer token.
            const endpoint = new URL(url)
            this.debugLog(`WebTransport ${endpoint.hostname}:${endpoint.port || "443"}: ${result}`)
        }, WEBTRANSPORT_CONNECT_TIMEOUT_MS)
        if (this.webTransportAttempt === attempt) this.webTransportAttempt = null
        if (!winner) return this.isCurrentControlGeneration(generation) ? "failednoconnect" : "disconnect"
        const { transport, shutdown } = winner

        if (!this.isCurrentControlGeneration(generation)) {
            await transport.close()
            return "disconnect"
        }

        // Bind the inbound control path before selecting QUIC. SetTransport and
        // StartStream then enter the same ordered, reliable lane in that order.
        if (!this.bindWebTransportControl(transport, generation)) {
            await transport.close()
            return "failednoconnect"
        }
        this.setTransport(transport)
        if (!this.sendWebTransportControl(transport, {
            SetTransport: "WebTransport"
        }, generation)) {
            this.clearWebTransportControl(transport)
            await transport.close()
            return "failednoconnect"
        }

        const videoCodecSupport = await this.createPipelines()
        if (!this.isCurrentControlGeneration(generation)) {
            this.clearWebTransportControl(transport)
            await transport.close()
            return "disconnect"
        }
        if (!videoCodecSupport) {
            this.debugLog("Failed to start WebTransport because no supported video pipeline was found", { type: "fatalDescription" })
            this.clearWebTransportControl(transport)
            await transport.close()
            return "failednoconnect"
        }

        if (!await this.startStream(videoCodecSupport, generation)) {
            this.clearWebTransportControl(transport)
            await transport.close()
            return "failednoconnect"
        }
        if (!this.isCurrentControlGeneration(generation)) {
            this.clearWebTransportControl(transport)
            await transport.close()
            return "disconnect"
        }
        this.armWebTransportStabilityReset(transport, generation)
        const shutdownReason = await shutdown
        this.clearWebTransportStabilityTimer(transport)
        this.clearWebTransportControl(transport)
        return shutdownReason
    }
    private async tryWebSocketTransport(generation: number): Promise<TransportShutdown | null> {
        if (!this.permissions.allow_transport_websockets) {
            this.debugLog("Not trying WebSocket transport becaues permissions disallow it")
            return null
        }

        this.debugLog("Trying Web Socket transport")

        this.sendWsMessage({
            SetTransport: "WebSocket"
        }, generation)

        const transport = new WebSocketTransport(this.ws, BIG_BUFFER, this.logger)
        const shutdown = new Promise<TransportShutdown>((resolve) => {
            transport.onclose = (reason) => {
                transport.onclose = null
                resolve(reason)
            }
        })

        this.setTransport(transport)

        const videoCodecSupport = await this.createPipelines()
        if (!this.isCurrentControlGeneration(generation)) {
            await transport.close()
            return "disconnect"
        }
        if (!videoCodecSupport) {
            this.debugLog("Failed to start stream because no video pipeline with support for the specified codec was found!", { type: "fatalDescription" })
            await transport.close()
            return null
        }

        await this.startStream(videoCodecSupport, generation)
        if (!this.isCurrentControlGeneration(generation)) {
            await transport.close()
            return "disconnect"
        }

        return shutdown
    }

    private async createPipelines(): Promise<VideoCodecSupport | null> {
        // Print supported pipes
        const pipesInfo = await gatherPipeInfo()

        this.logger.debug(`Supported Pipes: {`)
        let isFirst = true
        for (const [pipe, info] of pipesInfo) {
            this.logger.debug(`${isFirst ? "" : ","}"${pipe.name}": ${JSON.stringify(info)}`)
            isFirst = false
        }
        this.logger.debug(`}`)

        // Create pipelines
        const [supportedVideoCodecs] = await Promise.all([this.createVideoRenderer(), this.createAudioPlayer()])

        const videoPipelineName = `${this.transport?.getChannel(TransportChannelId.HOST_VIDEO).type} (transport) -> ${this.videoRenderer?.implementationName} (renderer)`
        this.debugLog(`Using video pipeline: ${videoPipelineName}`)

        const audioPipelineName = `${this.transport?.getChannel(TransportChannelId.HOST_AUDIO).type} (transport) -> ${this.audioPlayer?.implementationName} (player)`
        this.debugLog(`Using audio pipeline: ${audioPipelineName}`)

        this.stats.setVideoPipeline(videoPipelineName, this.videoRenderer)
        this.stats.setAudioPipeline(audioPipelineName, this.audioPlayer)

        return supportedVideoCodecs
    }
    private async createVideoRenderer(): Promise<VideoCodecSupport | null> {
        if (this.videoRenderer) {
            this.debugLog("Found an old video renderer -> cleaning it up")

            this.videoRenderer.unmount(this.divElement)
            this.videoRenderer.cleanup()
            this.videoRenderer = null
        }
        if (!this.transport) {
            this.debugLog("Failed to setup video without transport")
            return null
        }

        const codecHint = getVideoCodecHint(this.settings)
        this.debugLog(`Codec Hint by the user: ${JSON.stringify(codecHint)}`)

        if (!hasAnyCodec(codecHint)) {
            this.debugLog("Couldn't find any supported video format. Change the codec option to H264 in the settings if you're unsure which codecs are supported.", { type: "fatalDescription" })
            return null
        }

        const transportCodecSupport = await this.transport.setupHostVideo({
            type: ["videotrack", "data"]
        })
        this.debugLog(`Transport supports these video codecs: ${JSON.stringify(transportCodecSupport)}`)

        const videoSettings: VideoPipelineOptions = {
            supportedVideoCodecs: andVideoCodecs(codecHint, transportCodecSupport),
            canvasRenderer: this.settings.canvasRenderer,
            forceVideoElementRenderer: this.settings.forceVideoElementRenderer,
            canvasVsync: this.settings.canvasVsync,
            framePacing: this.settings.videoFramePacing ?? "balanced",
        }

        let pipelineCodecSupport
        const video = this.transport.getChannel(TransportChannelId.HOST_VIDEO)
        if (video.type == "videotrack") {
            const { videoRenderer, supportedCodecs, error } = await buildVideoPipeline("videotrack", videoSettings, this.logger)

            if (error) {
                return null
            }
            pipelineCodecSupport = supportedCodecs

            videoRenderer.mount(this.divElement)

            video.addTrackListener((track) => {
                this.markVideoReady()
                videoRenderer.setTrack(track)
            })

            this.videoRenderer = videoRenderer
            this.lastMediaSetupKey = null
        } else if (video.type == "data") {
            const { videoRenderer, supportedCodecs, error } = await buildVideoPipeline("data", videoSettings, this.logger)

            if (error) {
                return null
            }
            pipelineCodecSupport = supportedCodecs

            videoRenderer.mount(this.divElement)

            video.addReceiveListener((data) => {
                this.markVideoReady()
                videoRenderer.submitPacket(data)

                // data pipeline support requesting idrs over video channel
                if (videoRenderer.pollRequestIdr()) {
                    this.requestHostVideoIdr(video)
                }
            })

            this.videoRenderer = videoRenderer
            this.lastMediaSetupKey = null
        } else {
            this.debugLog(`Failed to create video pipeline with transport channel of type ${video.type} (${this.transport.implementationName})`)
            return null
        }

        return pipelineCodecSupport
    }
    private requestHostVideoIdr(video: DataTransportChannel) {
        const buffer = new ByteBuffer(1)

        buffer.putU8(0)

        buffer.flip()

        video.send(buffer.getRemainingBuffer().buffer)
    }
    private async createAudioPlayer(): Promise<boolean> {
        if (this.audioPlayer) {
            this.debugLog("Found an old audio player -> cleaning it up")

            this.audioPlayer.unmount(this.divElement)
            this.audioPlayer.cleanup()
            this.audioPlayer = null
        }
        if (!this.transport) {
            this.debugLog("Failed to setup audio without transport")
            return false
        }

        this.transport.setupHostAudio({
            type: ["audiotrack", "data"]
        })

        const audio = this.transport?.getChannel(TransportChannelId.HOST_AUDIO)
        if (audio.type == "audiotrack") {
            const { audioPlayer, error } = await buildAudioPipeline("audiotrack", this.settings, this.logger)

            if (error) {
                return false
            }

            audioPlayer.mount(this.divElement)

            audio.addTrackListener((track) => audioPlayer.setTrack(track))

            this.audioPlayer = audioPlayer
            this.lastMediaSetupKey = null
        } else if (audio.type == "data") {
            const { audioPlayer, error } = await buildAudioPipeline("data", this.settings, this.logger)

            if (error) {
                return false
            }

            audioPlayer.mount(this.divElement)

            audio.addReceiveListener((data) => {
                audioPlayer.submitPacket(data)
            })

            this.audioPlayer = audioPlayer
            this.lastMediaSetupKey = null
        } else {
            this.debugLog(`Cannot find audio pipeline for transport type "${audio.type}"`)
            return false
        }

        return true
    }
    private effectiveStartBitrateKbps(): number {
        const cap = this.adaptiveBitrateCapKbps
        if (!this.settings.adaptiveBitrate || cap == null || cap >= this.settings.bitrate) {
            return this.settings.bitrate
        }
        const bitrate = Math.max(cap, this.settings.minimumBitrate)
        this.debugLog(`Starting at ${bitrate} Kbps, where adaptive bitrate last settled (setting: ${this.settings.bitrate} Kbps)`)
        return bitrate
    }

    private async startStream(
        videoCodecSupport: VideoCodecSupport,
        generation = this.controlGeneration,
    ): Promise<boolean> {
        const settings: StreamSettings = {
            bitrate_kbps: this.effectiveStartBitrateKbps(),
            adaptive_bitrate: this.settings.adaptiveBitrate,
            minimum_bitrate_kbps: this.settings.minimumBitrate,
            fps: this.settings.fps,
            width: this.streamerSize[0],
            height: this.streamerSize[1],
            play_audio_local: this.settings.playAudioLocal,
            encrypt_host_video: this.settings.encryptHostVideo,
            encrypt_host_audio: this.settings.encryptHostAudio,
            supported_codecs: createSupportedVideoFormatsBits(videoCodecSupport),
            hdr: this.settings.hdr ?? false,
        }

        const message: StreamClientMessage = {
            StartStream: {
                settings
            }
        }
        this.debugLog(`Starting stream with info: ${JSON.stringify(message)}`)
        this.debugLog(`Stream video codec info: ${JSON.stringify(videoCodecSupport)}`)

        // Log HDR requirements if HDR is requested
        if (this.settings.hdr) {
            const hasHdrCodec = videoCodecSupport.H265_MAIN10 || videoCodecSupport.AV1_MAIN10
            if (!hasHdrCodec) {
                this.debugLog(`Warning: HDR requested but no 10-bit codec available. HDR requires H265_MAIN10 or AV1_MAIN10 support.`)
            } else {
                this.debugLog(`HDR codec available: H265_MAIN10=${videoCodecSupport.H265_MAIN10}, AV1_MAIN10=${videoCodecSupport.AV1_MAIN10}`)
            }
        }

        return this.sendStreamControl(message, generation)
    }

    mount(parent: HTMLElement): void {
        parent.appendChild(this.divElement)
    }
    unmount(parent: HTMLElement): void {
        parent.removeChild(this.divElement)
    }

    getVideoRenderer(): VideoRenderer | null {
        return this.videoRenderer
    }
    getAudioPlayer(): AudioPlayer | null {
        return this.audioPlayer
    }

    // -- Raw Web Socket stuff
    private wsSendBuffer: Array<{ generation: number, raw: string }> = []

    private onWsOpen(ws: WebSocket, generation: number) {
        this.debugLog(`Web Socket Open`)

        for (const pending of this.wsSendBuffer.splice(0)) {
            if (pending.generation == generation) {
                ws.send(pending.raw)
            }
        }
    }
    private onWsClose(ws: WebSocket, generation: number, event: CloseEvent) {
        if (!this.isCurrentControlSocket(ws, generation)) {
            return
        }
        if (this.stopping) {
            this.debugLog("Control WebSocket closed while the stream was stopping; reconnect suppressed.")
            return
        }
        if (this.hasActiveWebTransportControl(generation)) {
            this.debugLog("Setup WebSocket closed after WebTransport handoff; the active WebTransport remains authoritative.")
            return
        }

        this.debugLog(
            `Control WebSocket closed unexpectedly (code=${event.code}, clean=${event.wasClean}, reason=${event.reason || "none"}).`,
            { type: "ifErrorDescription" },
        )

        // WebSocketTransport owns the same socket. Its close listener resolves
        // the active transport attempt, which then performs the sole guarded
        // restart. Starting another one here would race the two generations.
        if (this.transport instanceof WebSocketTransport) {
            this.debugLog("WebSocket data transport will handle the control socket closure.")
            return
        }
        if (this.controlRecoveryGeneration == generation) {
            return
        }
        this.controlRecoveryGeneration = generation

        // The socket can close before transport selection (for example while
        // waiting for Setup). No transport promise exists to own recovery in
        // that state, so start the same single-flight fresh-generation path.
        const reconnectTransport = this.transportOverride ?? this.settings.dataTransport
        this.debugLog(`Control socket closed before an active transport could handle it; reconnecting with ${reconnectTransport}.`)
        void this.restartWithFreshTransportFallback(reconnectTransport, generation).catch((error) => {
            this.debugLog(
                `Control socket recovery failed: ${error instanceof Error ? error.message : String(error)}`,
                { type: "fatalDescription" },
            )
        })
    }
    private onError(event: Event) {
        this.debugLog(`Web Socket or WebRtcPeer Error`)

        console.error(`Web Socket or WebRtcPeer Error`, event)
    }

    private sendWsMessage(
        message: StreamClientMessage,
        generation = this.controlGeneration,
    ) {
        if (!this.isCurrentControlGeneration(generation)) {
            return
        }

        const raw = JSON.stringify(message)
        if (
            this.controlSocketGeneration == generation &&
            this.ws.readyState == WebSocket.OPEN
        ) {
            this.ws.send(raw)
        } else {
            this.wsSendBuffer.push({ generation, raw })
        }
    }
    private onRawWsMessage(event: MessageEvent, generation: number) {
        if (this.hasActiveWebTransportControl(generation)) {
            return
        }
        const message = event.data
        if (typeof message == "string") {
            const json = JSON.parse(message)

            void this.onMessage(json, generation)
        }
    }

    stop(): Promise<boolean> {
        if (this.stopping) {
            return Promise.resolve(false)
        }
        this.stopping = true
        // The data-channel Stop must be enqueued while the current transport is
        // still usable. Immediately afterward, invalidate every async control
        // continuation before this method can await or return.
        const stopSent = this.sendGeneralMessage("Stop")
        this.controlGeneration++
        this.webTransportAttempt?.abort()
        this.clearWebTransportControl()
        this.clearWebTransportStabilityTimer()
        this.clearWebTransportReprobeTimer()
        this.wsSendBuffer.length = 0
        if (!stopSent) {
            return Promise.resolve(false)
        }

        // Wait for the message to get sent
        return new Promise((resolve, _reject) => {
            setTimeout(() => resolve(true), 100)
        })
    }

    // -- Class Api
    addInfoListener(listener: InfoEventListener) {
        this.eventTarget.addEventListener("stream-info", listener as EventListenerOrEventListenerObject)
    }
    removeInfoListener(listener: InfoEventListener) {
        this.eventTarget.removeEventListener("stream-info", listener as EventListenerOrEventListenerObject)
    }

    getInput(): StreamInput {
        return this.input
    }
    getStats(): StreamStats {
        return this.stats
    }

    getStreamerSize(): [number, number] {
        return this.streamerSize
    }
}

function createPrettyList(list: Array<string>): string {
    return `[${list.join(", ")}]`
}

// The adaptive cap lives in sessionStorage: it survives reconnects and reloads
// within the tab (the network is likely the same) but not a fresh visit.
const ADAPTIVE_CAP_KEY = "mlw-adaptive-bitrate-cap"
const ADAPTIVE_CAP_MAX_AGE_MS = 15 * 60 * 1000
function readAdaptiveBitrateCap(): number | null {
    try {
        const raw = sessionStorage.getItem(ADAPTIVE_CAP_KEY)
        if (!raw) {
            return null
        }
        const { kbps, at } = JSON.parse(raw)
        if (typeof kbps != "number" || typeof at != "number" || Date.now() - at > ADAPTIVE_CAP_MAX_AGE_MS) {
            return null
        }
        return kbps
    } catch (_error) {
        return null
    }
}
function writeAdaptiveBitrateCap(kbps: number) {
    try {
        sessionStorage.setItem(ADAPTIVE_CAP_KEY, JSON.stringify({ kbps, at: Date.now() }))
    } catch (_error) { }
}
