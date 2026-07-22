import { StreamSignalingMessage, TransportChannelId } from "../../api_bindings.js";
import { Logger } from "../log.js";
import { StatValue } from "../stats.js";
import { CAPABILITIES_CODECS, emptyVideoCodecs, maybeVideoCodecs, VideoCodecSupport } from "../video.js";
import { DataTransportChannel, Transport, TransportAudioSetup, TransportChannel, TransportChannelIdValue, TransportVideoSetup, AudioTrackTransportChannel, VideoTrackTransportChannel, TrackTransportChannel, TransportShutdown } from "./index.js";

const EXPECTED_DATA_CHANNELS: ReadonlyArray<readonly [string, TransportChannelIdValue]> = [
    ["general", TransportChannelId.GENERAL],
    ["stats", TransportChannelId.STATS],
    ["mouse_reliable", TransportChannelId.MOUSE_RELIABLE],
    ["mouse_absolute", TransportChannelId.MOUSE_ABSOLUTE],
    ["mouse_relative", TransportChannelId.MOUSE_RELATIVE],
    ["keyboard", TransportChannelId.KEYBOARD],
    ["touch", TransportChannelId.TOUCH],
    ["controllers", TransportChannelId.CONTROLLERS],
    ["controller0", TransportChannelId.CONTROLLER0],
    ["controller1", TransportChannelId.CONTROLLER1],
    ["controller2", TransportChannelId.CONTROLLER2],
    ["controller3", TransportChannelId.CONTROLLER3],
    ["controller4", TransportChannelId.CONTROLLER4],
    ["controller5", TransportChannelId.CONTROLLER5],
    ["controller6", TransportChannelId.CONTROLLER6],
    ["controller7", TransportChannelId.CONTROLLER7],
    ["controller8", TransportChannelId.CONTROLLER8],
    ["controller9", TransportChannelId.CONTROLLER9],
    ["controller10", TransportChannelId.CONTROLLER10],
    ["controller11", TransportChannelId.CONTROLLER11],
    ["controller12", TransportChannelId.CONTROLLER12],
    ["controller13", TransportChannelId.CONTROLLER13],
    ["controller14", TransportChannelId.CONTROLLER14],
    ["controller15", TransportChannelId.CONTROLLER15],
    ["rtt", TransportChannelId.RTT],
]
const EXPECTED_DATA_CHANNEL_IDS = new Map<string, TransportChannelIdValue>(EXPECTED_DATA_CHANNELS)

export class WebRTCTransport implements Transport {
    implementationName: string = "webrtc"

    private logger: Logger | null

    private peer: RTCPeerConnection | null = null
    private previousInboundVideoStats: {
        reportId: string
        counters: Record<string, number>
    } | null = null

    constructor(logger?: Logger) {
        this.logger = logger ?? null
    }

    async initPeer(configuration?: RTCConfiguration) {
        this.logger?.debug(`Creating Client Peer`)

        if (this.peer) {
            this.logger?.debug(`Cannot create Peer because a Peer already exists`)
            return
        }

        // Configure web rtc
        // TODO: use this for signaling instead and extend the protocol so that the client also requests a control channel with name: "control", protocol:"moonlight-control-v1": https://www.ietf.org/archive/id/draft-ietf-wish-whep-02.html
        this.peer = new RTCPeerConnection(configuration)
        this.peer.addEventListener("error", this.onError.bind(this))

        this.peer.addEventListener("icecandidate", this.onIceCandidate.bind(this))

        this.peer.addEventListener("connectionstatechange", this.onConnectionStateChange.bind(this))
        this.peer.addEventListener("signalingstatechange", this.onSignalingStateChange.bind(this))
        this.peer.addEventListener("iceconnectionstatechange", this.onIceConnectionStateChange.bind(this))
        this.peer.addEventListener("icegatheringstatechange", this.onIceGatheringStateChange.bind(this))

        this.peer.addEventListener("track", this.onTrack.bind(this))
        this.peer.addEventListener("datachannel", this.onDataChannel.bind(this))

        this.initChannels()

        // Maybe we already received data
        if (this.remoteDescription) {
            await this.handleRemoteDescription(this.remoteDescription)
        }
        await this.tryDequeueIceCandidates()
    }

    private onError(event: Event) {
        this.logger?.debug(`Web Socket or WebRtcPeer Error`)

        console.error(`Web Socket or WebRtcPeer Error`, event)
    }

    onsendmessage: ((message: StreamSignalingMessage) => void) | null = null
    private sendMessage(message: StreamSignalingMessage) {
        if (this.onsendmessage) {
            this.onsendmessage(message)
        } else {
            this.logger?.debug("Failed to call onicecandidate because no handler is set")
        }
    }
    async onReceiveMessage(message: StreamSignalingMessage) {
        if ("Description" in message) {
            const description = message.Description;
            await this.handleRemoteDescription({
                type: description.ty as RTCSdpType,
                sdp: description.sdp
            })
        } else if ("AddIceCandidate" in message) {
            const candidate = message.AddIceCandidate
            await this.addIceCandidate({
                candidate: candidate.candidate,
                sdpMid: candidate.sdp_mid,
                sdpMLineIndex: candidate.sdp_mline_index,
                usernameFragment: candidate.username_fragment
            })
        }
    }

    private remoteDescription: RTCSessionDescriptionInit | null = null
    private async handleRemoteDescription(sdp: RTCSessionDescriptionInit | null) {
        this.logger?.debug(`Received remote description: ${sdp?.type}`)

        const remoteDescription = sdp
        this.remoteDescription = remoteDescription
        if (!this.peer) {
            return
        }
        this.remoteDescription = null

        if (remoteDescription) {
            await this.peer.setRemoteDescription(remoteDescription)

            if (remoteDescription.type == "offer") {
                await this.peer.setLocalDescription()
                const localDescription = this.peer.localDescription
                if (!localDescription) {
                    this.logger?.debug("Peer didn't have a localDescription whilst receiving an offer and trying to answer")
                    return
                }

                this.logger?.debug(`Responding to offer description: ${localDescription.type}`)
                this.sendMessage({
                    Description: {
                        ty: localDescription.type,
                        sdp: localDescription.sdp ?? ""
                    }
                })
            }
        }
    }

    private onIceCandidate(event: RTCPeerConnectionIceEvent) {
        if (event.candidate) {
            const candidate = event.candidate.toJSON()
            this.logger?.debug(`Sending ice candidate: ${candidate.candidate}`)

            this.sendMessage({
                AddIceCandidate: {
                    candidate: candidate.candidate ?? "",
                    sdp_mid: candidate.sdpMid ?? null,
                    sdp_mline_index: candidate.sdpMLineIndex ?? null,
                    username_fragment: candidate.usernameFragment ?? null
                }
            })
        } else {
            this.logger?.debug("No new ice candidates")
        }
    }

    private iceCandidates: Array<RTCIceCandidateInit> = []
    private async addIceCandidate(candidate: RTCIceCandidateInit) {
        this.logger?.debug(`Received ice candidate: ${candidate.candidate}`)

        if (!this.peer) {
            this.logger?.debug("Buffering ice candidate")

            this.iceCandidates.push(candidate)
            return
        }
        await this.tryDequeueIceCandidates()

        await this.peer.addIceCandidate(candidate)
    }
    private async tryDequeueIceCandidates() {
        if (!this.peer) {
            this.logger?.debug("called tryDequeueIceCandidates without a peer")
            return
        }

        for (const candidate of this.iceCandidates) {
            await this.peer.addIceCandidate(candidate)
        }
        this.iceCandidates.length = 0
    }

    private wasConnected = false
    private onConnectionStateChange() {
        if (!this.peer) {
            this.logger?.debug("OnConnectionStateChange without a peer")
            return
        }

        let type: null | "fatal" | "recover" = null

        if (this.peer.connectionState == "connected") {
            type = "recover"

            if (this.onconnect) {
                this.onconnect()
            }
            this.wasConnected = true
        } else if ((this.peer.connectionState == "failed" || this.peer.connectionState == "closed") && this.peer.iceGatheringState == "complete") {
            type = "fatal"
        }

        if (this.peer.connectionState == "failed" || this.peer.connectionState == "closed") {
            if (this.onclose) {
                if (this.wasConnected) {
                    this.onclose("failed")
                } else {
                    this.onclose("failednoconnect")
                }
            }
        }

        this.logger?.debug(`Changing Peer State to ${this.peer.connectionState}`, {
            type: type ?? undefined
        })
    }
    private onSignalingStateChange() {
        if (!this.peer) {
            this.logger?.debug("OnSignalingStateChange without a peer")
            return
        }
        this.logger?.debug(`Changing Peer Signaling State to ${this.peer.signalingState}`)
    }
    private onIceConnectionStateChange() {
        if (!this.peer) {
            this.logger?.debug("OnIceConnectionStateChange without a peer")
            return
        }
        this.logger?.debug(`Changing Peer Ice State to ${this.peer.iceConnectionState}`)
    }
    private onIceGatheringStateChange() {
        if (!this.peer) {
            this.logger?.debug("OnIceGatheringStateChange without a peer")
            return
        }
        this.logger?.debug(`Changing Peer Ice Gathering State to ${this.peer.iceGatheringState}`)

        if (this.peer.iceConnectionState == "new" && this.peer.iceGatheringState == "complete") {
            // we failed without connection
            if (this.onclose) {
                this.onclose("failednoconnect")
            }
        }
    }

    private channels: Array<TransportChannel | null> = []
    private initChannels() {
        if (!this.peer) {
            this.logger?.debug("Failed to initialize channel without peer")
            return
        }
        if (this.channels.length > 0) {
            this.logger?.debug("Already initialized channels")
            return
        }

        const videoChannel: VideoTrackTransportChannel = new WebRTCInboundTrackTransportChannel<"videotrack">(this.logger, "videotrack", "video", this.videoTrackHolder)
        this.channels[TransportChannelId.HOST_VIDEO] = videoChannel

        const audioChannel: AudioTrackTransportChannel = new WebRTCInboundTrackTransportChannel<"audiotrack">(this.logger, "audiotrack", "audio", this.audioTrackHolder)
        this.channels[TransportChannelId.HOST_AUDIO] = audioChannel

        // The server creates every RTCDataChannel. These placeholders keep the
        // Transport interface available while signaling is still in progress.
        for (const [label, id] of EXPECTED_DATA_CHANNELS) {
            this.channels[id] = new WebRTCDataTransportChannel(label, null, this.logger)
        }
    }

    private videoTrackHolder: TrackHolder = { ontrack: null, track: null }
    private videoReceiver: RTCRtpReceiver | null = null

    private audioTrackHolder: TrackHolder = { ontrack: null, track: null }

    private onTrack(event: RTCTrackEvent) {
        const track = event.track

        const receiver = event.receiver
        if (track.kind == "video") {
            this.videoReceiver = receiver
        }

        receiver.jitterBufferTarget = 0
        if ("playoutDelayHint" in receiver) {
            receiver.playoutDelayHint = 0
        }

        this.logger?.debug(`Adding receiver: ${track.kind}, ${track.id}, ${track.label}`)

        if (track.kind == "video") {
            if ("contentHint" in track) {
                track.contentHint = "motion"
            }

            this.videoTrackHolder.track = track
            if (!this.videoTrackHolder.ontrack) {
                throw "No video track listener registered!"
            }
            this.videoTrackHolder.ontrack()
        } else if (track.kind == "audio") {
            this.audioTrackHolder.track = track
            if (!this.audioTrackHolder.ontrack) {
                throw "No audio track listener registered!"
            }
            this.audioTrackHolder.ontrack()
        }
    }

    // Handle data channels created by the remote peer (server)
    private onDataChannel(event: RTCDataChannelEvent) {
        const remoteChannel = event.channel
        const label = remoteChannel.label

        this.logger?.debug(`Received remote data channel: ${label}`)

        // Only protocol data labels are accepted. In particular, enum-shaped
        // labels such as HOST_VIDEO must never replace a media-track channel.
        const id = EXPECTED_DATA_CHANNEL_IDS.get(label)
        if (id == null) {
            this.logger?.debug(`Unknown remote data channel: ${label}`)
            remoteChannel.close()
            return
        }

        const existingChannel = this.channels[id]
        if (!existingChannel || existingChannel.type !== "data") {
            this.logger?.debug(`Refusing remote data channel that conflicts with channel ${id}: ${label}`)
            remoteChannel.close()
            return
        }

        this.logger?.debug(`Attaching remote data channel: ${label}`)
        const dataChannel = existingChannel as WebRTCDataTransportChannel
        dataChannel.replaceChannel(remoteChannel)
    }

    async setupHostVideo(_setup: TransportVideoSetup): Promise<VideoCodecSupport> {
        // TODO: check transport type

        let capabilities
        if ("getCapabilities" in RTCRtpReceiver && (capabilities = RTCRtpReceiver.getCapabilities("video"))) {
            const codecs = emptyVideoCodecs()

            for (const codec in codecs) {
                const supportRequirements = CAPABILITIES_CODECS[codec]

                if (!supportRequirements) {
                    continue
                }

                let supported = false
                capabilityCodecLoop: for (const codecCapability of capabilities.codecs) {
                    if (codecCapability.mimeType != supportRequirements.mimeType) {
                        continue
                    }

                    for (const fmtpLine of supportRequirements.fmtpLine) {
                        if (!codecCapability.sdpFmtpLine?.includes(fmtpLine)) {
                            continue capabilityCodecLoop
                        }
                    }

                    supported = true
                    break
                }

                codecs[codec] = supported
            }

            return codecs
        } else {
            return maybeVideoCodecs()
        }
    }

    async setupHostAudio(_setup: TransportAudioSetup): Promise<void> {
        // TODO: check transport type
    }

    getChannel(id: TransportChannelIdValue): TransportChannel {
        const channel = this.channels[id]
        if (!channel) {
            this.logger?.debug("Failed to setup video without peer")
            throw `Failed to get channel because it is not yet initialized, Id: ${id}`
        }

        return channel
    }

    onconnect: (() => void) | null = null

    onclose: ((shutdown: TransportShutdown) => void) | null = null
    async close(): Promise<void> {
        this.logger?.debug("Closing WebRTC Peer")

        this.peer?.close()
    }

    async getStats(): Promise<Record<string, StatValue>> {
        const statsData: Record<string, StatValue> = {}

        const mouseMotionChannel = this.channels[TransportChannelId.MOUSE_RELATIVE]
        if (mouseMotionChannel instanceof WebRTCDataTransportChannel) {
            Object.assign(statsData, mouseMotionChannel.motionDiagnostics())
        }

        if (!this.videoReceiver) {
            return statsData
        }
        const stats = Array.from((await this.videoReceiver.getStats()).values())
        const inboundVideoReports = stats
            .map(value => value as unknown as Record<string, unknown>)
            .filter(report => (
                report.type == "inbound-rtp" &&
                (report.kind == "video" || report.mediaType == "video") &&
                (typeof report.framesReceived == "number" || typeof report.framesDecoded == "number")
            ))
        const selectedInboundVideoReport = inboundVideoReports.reduce<Record<string, unknown> | null>(
            (selected, candidate) => {
                if (!selected) {
                    return candidate
                }
                const score = (report: Record<string, unknown>) => {
                    const fps = typeof report.framesPerSecond == "number" ? report.framesPerSecond : -1
                    const decoded = typeof report.framesDecoded == "number" ? report.framesDecoded : -1
                    return [fps, decoded]
                }
                const [selectedFps, selectedDecoded] = score(selected)
                const [candidateFps, candidateDecoded] = score(candidate)
                return candidateFps > selectedFps || (
                    candidateFps == selectedFps && candidateDecoded > selectedDecoded
                ) ? candidate : selected
            },
            null,
        )
        if (selectedInboundVideoReport) {
            this.addInboundVideoIntervalStats(statsData, selectedInboundVideoReport)
        }

        for (const value of stats) {

            if ("decoderImplementation" in value && value.decoderImplementation != null) {
                statsData.decoderImplementation = value.decoderImplementation
            }
            if ("frameWidth" in value && value.frameWidth != null) {
                statsData.videoWidth = value.frameWidth
            }
            if ("frameHeight" in value && value.frameHeight != null) {
                statsData.videoHeight = value.frameHeight
            }
            if ("framesPerSecond" in value && value.framesPerSecond != null) {
                statsData.webrtcFps = value.framesPerSecond
            }

            if ("jitterBufferDelay" in value && value.jitterBufferDelay != null) {
                statsData.webrtcJitterBufferTotalDelayMs = value.jitterBufferDelay * 1000
            }
            if ("jitterBufferTargetDelay" in value && value.jitterBufferTargetDelay != null) {
                statsData.webrtcJitterBufferTotalTargetDelayMs = value.jitterBufferTargetDelay * 1000
            }
            if ("jitterBufferMinimumDelay" in value && value.jitterBufferMinimumDelay != null) {
                statsData.webrtcJitterBufferTotalMinimumDelayMs = value.jitterBufferMinimumDelay * 1000
            }
            if ("jitter" in value && value.jitter != null) {
                statsData.webrtcJitterMs = value.jitter * 1000
            }
            if ("totalDecodeTime" in value && value.totalDecodeTime != null) {
                statsData.webrtcTotalDecodeTimeMs = value.totalDecodeTime * 1000
            }
            if ("totalAssemblyTime" in value && value.totalAssemblyTime != null) {
                statsData.webrtcTotalAssemblyTimeMs = value.totalAssemblyTime * 1000
            }
            if ("totalProcessingDelay" in value && value.totalProcessingDelay != null) {
                statsData.webrtcTotalProcessingDelayMs = value.totalProcessingDelay * 1000
            }
            if ("packetsReceived" in value && value.packetsReceived != null) {
                statsData.webrtcPacketsReceived = value.packetsReceived
            }
            if ("packetsLost" in value && value.packetsLost != null) {
                statsData.webrtcPacketsLost = value.packetsLost
            }
            if ("framesDropped" in value && value.framesDropped != null) {
                statsData.webrtcFramesDropped = value.framesDropped
            }
            if ("framesReceived" in value && value.framesReceived != null) {
                statsData.webrtcFramesReceived = value.framesReceived
            }
            if ("framesDecoded" in value && value.framesDecoded != null) {
                statsData.webrtcFramesDecoded = value.framesDecoded
            }
            if ("framesRendered" in value && value.framesRendered != null) {
                statsData.webrtcFramesRendered = value.framesRendered
            }
            if ("freezeCount" in value && value.freezeCount != null) {
                statsData.webrtcFreezeCount = value.freezeCount
            }
            if ("totalFreezesDuration" in value && value.totalFreezesDuration != null) {
                statsData.webrtcTotalFreezeDurationMs = value.totalFreezesDuration * 1000
            }
            if ("keyFramesDecoded" in value && value.keyFramesDecoded != null) {
                statsData.webrtcKeyFramesDecoded = value.keyFramesDecoded
            }
            if ("nackCount" in value && value.nackCount != null) {
                statsData.webrtcNackCount = value.nackCount
            }
        }

        return statsData
    }

    private addInboundVideoIntervalStats(
        statsData: Record<string, StatValue>,
        report: Record<string, unknown>,
    ): void {
        const counterNames = [
            "bytesReceived",
            "packetsReceived",
            "packetsLost",
            "framesReceived",
            "framesDecoded",
            "framesRendered",
            "framesDropped",
            "jitterBufferDelay",
            "jitterBufferTargetDelay",
            "jitterBufferMinimumDelay",
            "jitterBufferEmittedCount",
            "totalDecodeTime",
            "totalProcessingDelay",
            "totalAssemblyTime",
            "framesAssembledFromMultiplePackets",
            "nackCount",
            "freezeCount",
            "totalFreezesDuration",
        ]
        const timestamp = report.timestamp
        const reportId = report.id
        if (
            typeof timestamp != "number" || !Number.isFinite(timestamp) ||
            typeof reportId != "string"
        ) {
            return
        }

        const current: Record<string, number> = { timestamp }
        for (const name of counterNames) {
            const value = report[name]
            if (typeof value == "number" && Number.isFinite(value)) {
                current[name] = value
            }
        }

        const previous = this.previousInboundVideoStats?.reportId == reportId
            ? this.previousInboundVideoStats.counters
            : null
        this.previousInboundVideoStats = { reportId, counters: current }
        if (!previous) {
            return
        }

        const intervalSeconds = (current.timestamp - previous.timestamp) / 1000
        if (!Number.isFinite(intervalSeconds) || intervalSeconds <= 0 || intervalSeconds > 10) {
            return
        }

        const delta = (name: string): number | null => {
            const currentValue = current[name]
            const previousValue = previous[name]
            if (currentValue == null || previousValue == null || currentValue < previousValue) {
                return null
            }
            return currentValue - previousValue
        }
        const rate = (name: string): number | null => {
            const value = delta(name)
            return value == null ? null : value / intervalSeconds
        }
        const setRate = (key: string, counter: string) => {
            const value = rate(counter)
            if (value != null) {
                statsData[key] = value
            }
        }
        const setAverageMs = (key: string, totalCounter: string, countCounter: string) => {
            const total = delta(totalCounter)
            const count = delta(countCounter)
            if (total != null && count != null && count > 0) {
                statsData[key] = total * 1000 / count
            }
        }

        setRate("webrtcReceiveFps", "framesReceived")
        setRate("webrtcDecodeFps", "framesDecoded")
        setRate("webrtcRenderFps", "framesRendered")
        setRate("webrtcDroppedFps", "framesDropped")
        setRate("webrtcNacksPerSecond", "nackCount")

        const receivedBytes = delta("bytesReceived")
        if (receivedBytes != null) {
            statsData.webrtcReceiveMbps = receivedBytes * 8 / intervalSeconds / 1_000_000
        }

        const receivedPackets = delta("packetsReceived")
        const lostPackets = delta("packetsLost")
        if (receivedPackets != null && lostPackets != null && receivedPackets + lostPackets > 0) {
            statsData.webrtcPacketLossPercent = lostPackets * 100 / (receivedPackets + lostPackets)
        }

        setAverageMs("webrtcJitterBufferPerFrameMs", "jitterBufferDelay", "jitterBufferEmittedCount")
        setAverageMs("webrtcJitterTargetPerFrameMs", "jitterBufferTargetDelay", "jitterBufferEmittedCount")
        setAverageMs("webrtcJitterMinimumPerFrameMs", "jitterBufferMinimumDelay", "jitterBufferEmittedCount")
        setAverageMs("webrtcDecodePerFrameMs", "totalDecodeTime", "framesDecoded")
        setAverageMs("webrtcProcessingPerFrameMs", "totalProcessingDelay", "framesDecoded")
        setAverageMs("webrtcAssemblyPerFrameMs", "totalAssemblyTime", "framesAssembledFromMultiplePackets")

        const freezes = delta("freezeCount")
        if (freezes != null) {
            statsData.webrtcFreezesThisInterval = freezes
        }
        const freezeDuration = delta("totalFreezesDuration")
        if (freezeDuration != null) {
            statsData.webrtcFreezeDurationThisIntervalMs = freezeDuration * 1000
        }
    }
}

type TrackHolder = {
    ontrack: (() => void) | null
    track: MediaStreamTrack | null
}

// This receives track data
class WebRTCInboundTrackTransportChannel<T extends string> implements TrackTransportChannel {
    type: T

    canReceive: boolean = true
    canSend: boolean = false

    private logger: Logger | null

    private label: string
    private trackHolder: TrackHolder

    constructor(logger: Logger | null, type: T, label: string, trackHolder: TrackHolder) {
        this.logger = logger

        this.type = type
        this.label = label
        this.trackHolder = trackHolder

        this.trackHolder.ontrack = this.onTrack.bind(this)
    }
    setTrack(_track: MediaStreamTrack | null): void {
        throw "WebRTCInboundTrackTransportChannel cannot addTrack"
    }

    private onTrack() {
        const track = this.trackHolder.track
        if (!track) {
            this.logger?.debug("WebRTC TrackHolder.track is null!")
            return
        }

        for (const listener of this.trackListeners) {
            listener(track)
        }
    }


    private trackListeners: Array<(track: MediaStreamTrack) => void> = []
    addTrackListener(listener: (track: MediaStreamTrack) => void): void {
        if (this.trackHolder.track) {
            listener(this.trackHolder.track)
        }
        this.trackListeners.push(listener)
    }
    removeTrackListener(listener: (track: MediaStreamTrack) => void): void {
        const index = this.trackListeners.indexOf(listener)
        if (index != -1) {
            this.trackListeners.splice(index, 1)
        }
    }
}

type DataChannelQueueMode = "fifo" | "latest" | "mouse-relative"

type DataChannelQueuePolicy = {
    mode: DataChannelQueueMode
    maxMessages: number
    maxBytes: number
}

type QueuedDataChannelMessage = {
    data: ArrayBuffer
    queuedAt: number
    queuedBeforeOpen: boolean
    sendAttempts: number
}

const DATA_CHANNEL_BUFFER_HIGH_WATER_MARK = 64 * 1024
const DATA_CHANNEL_BUFFER_LOW_WATER_MARK = 16 * 1024
const DATA_CHANNEL_DRAIN_MAX_MESSAGES = 32
const DATA_CHANNEL_DRAIN_MAX_BYTES = 16 * 1024
const DATA_CHANNEL_MAX_MESSAGE_BYTES = 64 * 1024
const DATA_CHANNEL_DRAIN_RETRY_MS = 4
const DATA_CHANNEL_MAX_SEND_ATTEMPTS = 16
const FIFO_RESERVED_TERMINAL_MESSAGES = 32
const FIFO_RESERVED_TERMINAL_BYTES = 64 * 1024

const FIFO_QUEUE_POLICY: DataChannelQueuePolicy = {
    mode: "fifo",
    maxMessages: 512,
    maxBytes: 512 * 1024,
}
const LATEST_QUEUE_POLICY: DataChannelQueuePolicy = {
    mode: "latest",
    maxMessages: 1,
    maxBytes: 64 * 1024,
}
const MOUSE_RELATIVE_QUEUE_POLICY: DataChannelQueuePolicy = {
    mode: "mouse-relative",
    maxMessages: 16,
    maxBytes: 1024,
}
const MOUSE_RELATIVE_PRE_OPEN_MAX_AGE_MS = 125

function dataChannelQueuePolicy(label: string): DataChannelQueuePolicy {
    if (label == "mouse_absolute" || /^controller(?:[0-9]|1[0-5])$/.test(label)) {
        return LATEST_QUEUE_POLICY
    }
    if (label == "mouse_relative") {
        return MOUSE_RELATIVE_QUEUE_POLICY
    }
    return FIFO_QUEUE_POLICY
}

class WebRTCDataTransportChannel implements DataTransportChannel {
    type: "data" = "data"

    canReceive: boolean = true
    canSend: boolean = true

    private logger: Logger | null = null

    private label: string
    private channel: RTCDataChannel | null = null
    private channelGeneration = 0
    private channelIsTerminal = false
    private detachChannelListeners: (() => void) | null = null

    private queuePolicy: DataChannelQueuePolicy
    private sendQueue: Array<QueuedDataChannelMessage> = []
    private queuedBytes = 0
    private drainScheduled = false
    private drainRetryTimer: number | null = null
    private queueOverflowLogged = false
    private terminalSendLogged = false
    private peakBufferedBytes = 0
    private peakQueuedMessages = 0
    private coalescedMotionPackets = 0

    constructor(label: string, channel: RTCDataChannel | null, logger?: Logger | null) {
        this.label = label
        this.queuePolicy = dataChannelQueuePolicy(label)
        this.logger = logger ?? null

        this.attachChannel(channel)
    }

    replaceChannel(newChannel: RTCDataChannel): void {
        if (
            this.channel &&
            this.channel !== newChannel &&
            !this.channelIsTerminal &&
            !this.isTerminalChannel(this.channel)
        ) {
            this.logger?.debug(`Rejecting duplicate live WebRTC data channel: ${this.label}`)
            newChannel.close()
            return
        }

        this.attachChannel(newChannel)
    }

    private attachChannel(channel: RTCDataChannel | null): void {
        this.detachChannelListeners?.()
        this.detachChannelListeners = null
        this.cancelDrainRetry()

        const generation = ++this.channelGeneration
        this.channel = channel
        this.channelIsTerminal = false
        this.terminalSendLogged = false
        this.drainScheduled = false

        if (!channel) {
            return
        }

        channel.binaryType = "arraybuffer"
        channel.bufferedAmountLowThreshold = this.usesSingleNativeMessagePacing() || this.queuePolicy.mode == "latest"
            ? 0
            : DATA_CHANNEL_BUFFER_LOW_WATER_MARK

        const isCurrent = () => this.channel === channel && this.channelGeneration === generation
        const onOpen = () => {
            if (!isCurrent()) {
                return
            }
            this.channelIsTerminal = false
            this.terminalSendLogged = false
            this.expireStalePreOpenMouseMotion(Date.now())
            this.tryDequeueSendQueue()
        }
        const onMessage = (event: MessageEvent) => {
            if (isCurrent()) {
                this.onMessage(event)
            }
        }
        const onClose = () => {
            if (isCurrent()) {
                this.markChannelTerminal(channel, generation, "closed")
            }
        }
        const onError = () => {
            if (!isCurrent()) {
                return
            }

            this.logger?.debug(`WebRTC data channel ${this.label} reported an error`)
            if (channel.readyState == "closing" || channel.readyState == "closed") {
                this.markChannelTerminal(channel, generation, channel.readyState)
            }
        }
        const onBufferedAmountLow = () => {
            if (isCurrent()) {
                this.tryDequeueSendQueue()
            }
        }

        channel.addEventListener("open", onOpen)
        channel.addEventListener("message", onMessage)
        channel.addEventListener("close", onClose)
        channel.addEventListener("error", onError)
        channel.addEventListener("bufferedamountlow", onBufferedAmountLow)

        this.detachChannelListeners = () => {
            channel.removeEventListener("open", onOpen)
            channel.removeEventListener("message", onMessage)
            channel.removeEventListener("close", onClose)
            channel.removeEventListener("error", onError)
            channel.removeEventListener("bufferedamountlow", onBufferedAmountLow)
        }

        // A server-created channel may already have opened before the
        // datachannel event is delivered, so do not rely solely on "open".
        if (channel.readyState == "open") {
            onOpen()
        } else if (channel.readyState == "closing" || channel.readyState == "closed") {
            this.markChannelTerminal(channel, generation, channel.readyState)
        }
    }

    private markChannelTerminal(channel: RTCDataChannel, generation: number, state: string): void {
        if (this.channel !== channel || this.channelGeneration !== generation || this.channelIsTerminal) {
            return
        }

        this.channelIsTerminal = true
        this.cancelDrainRetry()
        this.clearSendQueue()
        this.logger?.debug(`WebRTC data channel ${this.label} is terminal (${state})`)
    }

    private isTerminalChannel(channel: RTCDataChannel): boolean {
        return channel.readyState == "closing" || channel.readyState == "closed"
    }

    send(message: ArrayBuffer): void {
        if (message.byteLength > DATA_CHANNEL_MAX_MESSAGE_BYTES) {
            this.logQueueOverflowOnce()
            return
        }

        const channel = this.channel
        if (this.channelIsTerminal || (channel && this.isTerminalChannel(channel))) {
            if (channel && !this.channelIsTerminal) {
                this.markChannelTerminal(channel, this.channelGeneration, channel.readyState)
            }
            this.logTerminalSendOnce()
            return
        }

        let initialSendAttempts = 0
        if (channel?.readyState == "open") {
            // Preserve FIFO ordering by draining accepted older messages first.
            // Latest-only state instead replaces any queued stale state below.
            if (this.sendQueue.length > 0 && this.queuePolicy.mode != "latest") {
                this.tryDequeueSendQueue()
            }

            if (this.sendQueue.length == 0 && this.canSendNow(channel, message.byteLength)) {
                try {
                    channel.send(message)
                    this.updateBufferPeaks()
                    return
                } catch (_error) {
                    initialSendAttempts = 1
                    if (this.isTerminalChannel(channel)) {
                        this.markChannelTerminal(channel, this.channelGeneration, channel.readyState)
                        this.logTerminalSendOnce()
                        return
                    }
                    // A transient native-buffer failure is handled by the same
                    // bounded queue as explicit backpressure.
                }
            }
        }

        const queuedBeforeOpen = !channel || channel.readyState != "open"
        if (!this.enqueueMessage(message, queuedBeforeOpen, initialSendAttempts)) {
            return
        }

        if (channel?.readyState == "open") {
            this.tryDequeueSendQueue()
        }
    }

    private enqueueMessage(message: ArrayBuffer, queuedBeforeOpen: boolean, sendAttempts: number): boolean {
        const now = Date.now()
        if (this.queuePolicy.mode == "mouse-relative") {
            this.expireStalePreOpenMouseMotion(now)
        }

        const messageBytes = message.byteLength
        if (messageBytes > DATA_CHANNEL_MAX_MESSAGE_BYTES || messageBytes > this.queuePolicy.maxBytes) {
            this.logQueueOverflowOnce()
            return false
        }

        let copy: ArrayBuffer
        try {
            copy = message.slice(0)
        } catch (_error) {
            this.logQueueOverflowOnce()
            return false
        }

        if (this.queuePolicy.mode == "latest") {
            this.clearSendQueue()
        } else {
            this.removeSupersededQueuedMessages(copy)
        }

        if (this.queuePolicy.mode == "mouse-relative" && sendAttempts == 0 && this.isKnownRelativeMotionPacket(copy)) {
            const coalesced = this.coalesceQueuedRelativeMotion(copy, now, queuedBeforeOpen, sendAttempts)
            if (coalesced != null) {
                if (!coalesced) {
                    this.logQueueOverflowOnce()
                }
                return coalesced
            }
        }

        const isTerminalInput = this.isTerminalInputMessage(copy)
        if (this.queuePolicy.mode == "fifo" && isTerminalInput) {
            while (!this.hasQueueCapacity(messageBytes, true)) {
                const replaceableIndex = this.sendQueue.findIndex(queued => !this.isTerminalInputMessage(queued.data))
                if (replaceableIndex == -1) {
                    break
                }
                this.removeQueuedMessage(replaceableIndex)
            }
        }

        if (!this.hasQueueCapacity(messageBytes, isTerminalInput)) {
            // A small reserve prevents motion/repeat floods from consuming the
            // slots needed by key, button, touch, and controller releases.
            this.logQueueOverflowOnce()
            return false
        }

        this.sendQueue.push({
            data: copy,
            queuedAt: now,
            queuedBeforeOpen,
            sendAttempts,
        })
        this.queuedBytes += copy.byteLength
        this.updateBufferPeaks()
        return true
    }

    private coalesceQueuedRelativeMotion(
        data: ArrayBuffer,
        now: number,
        queuedBeforeOpen: boolean,
        sendAttempts: number,
    ): boolean | null {
        let tailStart = this.sendQueue.length
        while (
            tailStart > 0 &&
            this.sendQueue[tailStart - 1].queuedBeforeOpen == queuedBeforeOpen &&
            this.sendQueue[tailStart - 1].sendAttempts == 0 &&
            this.isKnownRelativeMotionPacket(this.sendQueue[tailStart - 1].data)
        ) {
            tailStart--
        }
        if (tailStart == this.sendQueue.length) {
            return null
        }

        let totalX = new DataView(data).getInt16(1, false)
        let totalY = new DataView(data).getInt16(3, false)
        let tailBytes = 0
        let queuedAt = now
        let allQueuedBeforeOpen = queuedBeforeOpen
        let maxSendAttempts = sendAttempts
        for (let i = tailStart; i < this.sendQueue.length; i++) {
            const queued = this.sendQueue[i]
            const view = new DataView(queued.data)
            totalX += view.getInt16(1, false)
            totalY += view.getInt16(3, false)
            tailBytes += queued.data.byteLength
            queuedAt = Math.min(queuedAt, queued.queuedAt)
            allQueuedBeforeOpen &&= queued.queuedBeforeOpen
            maxSendAttempts = Math.max(maxSendAttempts, queued.sendAttempts)
        }

        const packets = this.encodeRelativeMotionPackets(totalX, totalY)
        const prefixMessages = tailStart
        const prefixBytes = this.queuedBytes - tailBytes
        const replacementBytes = packets.reduce((sum, packet) => sum + packet.byteLength, 0)
        if (
            prefixMessages + packets.length > this.queuePolicy.maxMessages ||
            prefixBytes + replacementBytes > this.queuePolicy.maxBytes
        ) {
            return false
        }

        const replacements = packets.map(packet => ({
            data: packet,
            queuedAt,
            queuedBeforeOpen: allQueuedBeforeOpen,
            sendAttempts: maxSendAttempts,
        }))
        this.sendQueue.splice(tailStart, this.sendQueue.length - tailStart, ...replacements)
        this.queuedBytes = prefixBytes + replacementBytes
        this.coalescedMotionPackets++
        this.updateBufferPeaks()
        return true
    }

    private encodeRelativeMotionPackets(totalX: number, totalY: number): ArrayBuffer[] {
        const packets: ArrayBuffer[] = []
        while (totalX != 0 || totalY != 0) {
            const packetX = Math.max(-0x8000, Math.min(0x7fff, totalX))
            const packetY = Math.max(-0x8000, Math.min(0x7fff, totalY))
            const packet = new ArrayBuffer(5)
            const view = new DataView(packet)
            view.setUint8(0, 0)
            view.setInt16(1, packetX, false)
            view.setInt16(3, packetY, false)
            packets.push(packet)
            totalX -= packetX
            totalY -= packetY
        }
        return packets
    }

    private tryDequeueSendQueue(): void {
        const channel = this.channel
        const generation = this.channelGeneration
        if (!channel || this.channelIsTerminal || channel.readyState != "open") {
            return
        }

        let sentMessages = 0
        let sentBytes = 0
        while (this.sendQueue.length > 0 && sentMessages < DATA_CHANNEL_DRAIN_MAX_MESSAGES) {
            const queued = this.sendQueue[0]
            if (!this.canSendNow(channel, queued.data.byteLength)) {
                break
            }
            if (sentMessages > 0 && sentBytes + queued.data.byteLength > DATA_CHANNEL_DRAIN_MAX_BYTES) {
                break
            }

            try {
                channel.send(queued.data)
            } catch (_error) {
                // Keep the head item unless the native send succeeded. A close
                // racing with send is terminal; other failures wait for the
                // next low-buffer notification or send attempt.
                if (this.isTerminalChannel(channel)) {
                    this.markChannelTerminal(channel, generation, channel.readyState)
                    return
                }

                queued.sendAttempts++
                if (queued.sendAttempts >= DATA_CHANNEL_MAX_SEND_ATTEMPTS) {
                    this.logger?.debug(`Dropping persistently unsendable WebRTC data on ${this.label}`)
                    this.removeQueuedMessage(0)
                    this.scheduleDrain(generation)
                } else {
                    this.scheduleDrainRetry(generation)
                }
                return
            }

            this.removeQueuedMessage(0)
            sentMessages++
            sentBytes += queued.data.byteLength
            this.updateBufferPeaks()
            if (this.usesSingleNativeMessagePacing()) {
                break
            }
        }

        if (this.sendQueue.length == 0) {
            this.queueOverflowLogged = false
            this.cancelDrainRetry()
        } else if (this.usesSingleNativeMessagePacing()) {
            // A zero-threshold event normally wakes us as soon as the single
            // native message drains. Some browsers miss that edge, so poll at
            // the same short cadence without allowing another native backlog.
            this.scheduleDrainRetry(generation)
        } else if (
            sentMessages > 0 &&
            this.canSendNow(channel, this.sendQueue[0].data.byteLength)
        ) {
            this.scheduleDrain(generation)
        }
    }

    private scheduleDrain(generation: number): void {
        if (this.drainScheduled) {
            return
        }
        this.drainScheduled = true

        Promise.resolve().then(() => {
            this.drainScheduled = false
            if (generation == this.channelGeneration) {
                this.tryDequeueSendQueue()
            }
        })
    }

    private scheduleDrainRetry(generation: number): void {
        if (this.drainRetryTimer != null) {
            return
        }

        const channel = this.channel
        const timer = window.setTimeout(() => {
            if (
                this.drainRetryTimer !== timer ||
                generation != this.channelGeneration ||
                channel !== this.channel
            ) {
                return
            }
            this.drainRetryTimer = null
            this.tryDequeueSendQueue()
        }, DATA_CHANNEL_DRAIN_RETRY_MS)
        this.drainRetryTimer = timer
    }

    private cancelDrainRetry(): void {
        if (this.drainRetryTimer != null) {
            window.clearTimeout(this.drainRetryTimer)
            this.drainRetryTimer = null
        }
    }

    private canSendNow(channel: RTCDataChannel, messageBytes: number): boolean {
        if (messageBytes > DATA_CHANNEL_MAX_MESSAGE_BYTES) {
            return false
        }
        if (this.usesSingleNativeMessagePacing() || this.queuePolicy.mode == "latest") {
            return channel.bufferedAmount == 0
        }
        return channel.bufferedAmount + messageBytes <= DATA_CHANNEL_BUFFER_HIGH_WATER_MARK
    }

    private usesSingleNativeMessagePacing(): boolean {
        return this.queuePolicy.mode == "mouse-relative" || this.label == "mouse_absolute"
    }

    private hasQueueCapacity(messageBytes: number, isTerminalInput: boolean): boolean {
        let maxMessages = this.queuePolicy.maxMessages
        let maxBytes = this.queuePolicy.maxBytes
        if (this.queuePolicy.mode == "fifo" && !isTerminalInput) {
            maxMessages = Math.max(1, maxMessages - FIFO_RESERVED_TERMINAL_MESSAGES)
            maxBytes = Math.max(DATA_CHANNEL_MAX_MESSAGE_BYTES, maxBytes - FIFO_RESERVED_TERMINAL_BYTES)
        }
        return this.sendQueue.length < maxMessages && this.queuedBytes + messageBytes <= maxBytes
    }

    private isTerminalInputMessage(data: ArrayBuffer): boolean {
        const bytes = new Uint8Array(data)
        if (this.label == "keyboard") {
            return bytes.length >= 2 && bytes[0] == 0 && bytes[1] == 0
        }
        if (this.label == "mouse_reliable") {
            return bytes.length >= 2 && bytes[0] == 2 && bytes[1] == 0
        }
        if (this.label == "touch") {
            return bytes.length >= 1 && (bytes[0] == 2 || bytes[0] == 3)
        }
        if (this.label == "controllers") {
            return bytes.length >= 1 && bytes[0] == 1
        }
        return false
    }

    private removeSupersededQueuedMessages(data: ArrayBuffer): void {
        const bytes = new Uint8Array(data)
        if (this.label == "touch" && bytes.length >= 5 && bytes[0] == 1) {
            const contactId = new DataView(data).getUint32(1)
            for (let i = this.sendQueue.length - 1; i >= 0; i--) {
                const queued = this.sendQueue[i].data
                const queuedBytes = new Uint8Array(queued)
                if (
                    queuedBytes.length >= 5 &&
                    queuedBytes[0] == 1 &&
                    new DataView(queued).getUint32(1) == contactId
                ) {
                    this.removeQueuedMessage(i)
                }
            }
        } else if (this.label == "mouse_reliable" && bytes.length >= 1 && bytes[0] == 1) {
            const last = this.sendQueue[this.sendQueue.length - 1]
            if (last && new Uint8Array(last.data)[0] == 1) {
                this.removeQueuedMessage(this.sendQueue.length - 1)
            }
        }
    }

    private removeQueuedMessage(index: number): void {
        const [removed] = this.sendQueue.splice(index, 1)
        if (removed) {
            this.queuedBytes -= removed.data.byteLength
        }
    }

    private expireStalePreOpenMouseMotion(now: number): void {
        if (this.queuePolicy.mode != "mouse-relative") {
            return
        }

        for (let i = this.sendQueue.length - 1; i >= 0; i--) {
            const queued = this.sendQueue[i]
            if (
                queued.queuedBeforeOpen &&
                now - queued.queuedAt > MOUSE_RELATIVE_PRE_OPEN_MAX_AGE_MS &&
                this.isKnownRelativeMotionPacket(queued.data)
            ) {
                this.removeQueuedMessage(i)
            }
        }
    }

    private isKnownRelativeMotionPacket(data: ArrayBuffer): boolean {
        // StreamInput's relative-motion packet is tag 0 followed by two i16
        // deltas. Unknown packets and wheel deltas stay FIFO because combining
        // or discarding them could change their meaning.
        return data.byteLength == 5 && new Uint8Array(data, 0, 1)[0] == 0
    }

    private clearSendQueue(): void {
        this.sendQueue.length = 0
        this.queuedBytes = 0
        this.queueOverflowLogged = false
    }

    private logQueueOverflowOnce(): void {
        if (this.queueOverflowLogged) {
            return
        }
        this.queueOverflowLogged = true
        this.logger?.debug(`WebRTC data channel ${this.label} send queue reached its bound; rejecting new data`)
    }

    private logTerminalSendOnce(): void {
        if (this.terminalSendLogged) {
            return
        }
        this.terminalSendLogged = true
        this.logger?.debug(`WebRTC data channel ${this.label} is closed; rejecting send`)
    }

    private onMessage(event: MessageEvent) {
        const data = event.data
        if (!(data instanceof ArrayBuffer)) {
            console.warn(`received text data on webrtc channel ${this.label}`)
            return
        }

        for (const listener of this.receiveListeners) {
            listener(event.data)
        }
    }
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
    estimatedBufferedBytes(): number | null {
        if (!this.channel && this.queuedBytes == 0) {
            return null
        }
        return (this.channel?.bufferedAmount ?? 0) + this.queuedBytes
    }

    motionDiagnostics(): Record<string, StatValue> {
        this.updateBufferPeaks()
        return {
            mouseMotionBufferedBytes: this.estimatedBufferedBytes() ?? 0,
            mouseMotionPeakBufferedBytes: this.peakBufferedBytes,
            mouseMotionQueuedMessages: this.sendQueue.length,
            mouseMotionPeakQueuedMessages: this.peakQueuedMessages,
            mouseMotionCoalescedPackets: this.coalescedMotionPackets,
        }
    }

    private updateBufferPeaks(): void {
        this.peakBufferedBytes = Math.max(
            this.peakBufferedBytes,
            (this.channel?.bufferedAmount ?? 0) + this.queuedBytes,
        )
        this.peakQueuedMessages = Math.max(this.peakQueuedMessages, this.sendQueue.length)
    }
}
