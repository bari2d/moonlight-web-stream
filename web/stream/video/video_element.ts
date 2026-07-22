import { globalObject } from "../../util.js";
import { Pipe, PipeInfo } from "../pipeline/index.js";
import { addPipePassthrough } from "../pipeline/pipes.js";
import type { StatValue } from "../stats.js";
import { emptyVideoCodecs, maybeVideoCodecs, VideoCodecSupport } from "../video.js";
import { getStreamRectCorrected, TrackVideoRenderer, UrlVideoRenderer, VideoRenderer, VideoRendererSetup } from "./index.js";

const VIDEO_DECODER_CODECS: Record<keyof VideoCodecSupport, string> = {
    "H264": "avc1.42E01E",
    "H264_HIGH8_444": "avc1.640032",
    "H265": "hvc1.1.6.L93.B0",
    "H265_MAIN10": "hvc1.2.4.L120.90",
    "H265_REXT8_444": "hvc1.6.6.L93.90",
    "H265_REXT10_444": "hvc1.6.10.L120.90",
    "AV1_MAIN8": "av01.0.04M.08",
    "AV1_MAIN10": "av01.0.04M.10",
    "AV1_HIGH8_444": "av01.0.08M.08",
    "AV1_HIGH10_444": "av01.0.08M.10"
}

function detectCodecs(): VideoCodecSupport {
    if (!("canPlayType" in HTMLVideoElement.prototype)) {
        return maybeVideoCodecs()
    }

    const codecs = emptyVideoCodecs()

    const testElement = document.createElement("video")

    for (const codec in codecs) {
        const supported = testElement.canPlayType(`video/mp4; codecs=${VIDEO_DECODER_CODECS[codec]}`)

        if (supported == "probably") {
            codecs[codec] = true
        } else if (supported == "maybe") {
            codecs[codec] = "maybe"
        } else {
            // unsupported
            codecs[codec] = false
        }
    }

    return codecs
}

class VideoElementPlaybackStats {
    private readonly videoElement: HTMLVideoElement
    private frameCallbackId: number | null = null
    private frameCallbacksRunning = false
    private presentedFrameCallbacks = 0
    private lastPresentedFrameAtMs: number | null = null
    private maxPresentationGapMs = 0
    private previousReportAtMs: number | null = null
    private previousPresentedFrameCallbacks = 0
    private previousTotalFrames: number | null = null
    private previousDroppedFrames: number | null = null
    private lastStatsReportAtMs: number | null = null

    constructor(videoElement: HTMLVideoElement) {
        this.videoElement = videoElement
    }

    private readonly onVideoFrame = () => {
        this.frameCallbackId = null
        if (!this.frameCallbacksRunning) {
            return
        }
        const now = performance.now()
        if (this.lastStatsReportAtMs == null || now - this.lastStatsReportAtMs > 2500) {
            this.stop()
            return
        }
        if (this.lastPresentedFrameAtMs != null) {
            this.maxPresentationGapMs = Math.max(this.maxPresentationGapMs, now - this.lastPresentedFrameAtMs)
        }
        this.lastPresentedFrameAtMs = now
        this.presentedFrameCallbacks++
        this.scheduleFrameCallback()
    }

    start(): void {
        this.frameCallbacksRunning = true
        this.scheduleFrameCallback()
    }

    private scheduleFrameCallback(): void {
        if (!this.frameCallbacksRunning) {
            return
        }
        if (this.frameCallbackId != null) {
            return
        }

        const element = this.videoElement as HTMLVideoElement & {
            requestVideoFrameCallback?: (callback: () => void) => number
        }
        if (typeof element.requestVideoFrameCallback == "function") {
            this.frameCallbackId = element.requestVideoFrameCallback(this.onVideoFrame)
        }
    }

    stop(): void {
        this.frameCallbacksRunning = false
        if (this.frameCallbackId == null) {
            return
        }

        const element = this.videoElement as HTMLVideoElement & {
            cancelVideoFrameCallback?: (handle: number) => void
        }
        if (typeof element.cancelVideoFrameCallback == "function") {
            element.cancelVideoFrameCallback(this.frameCallbackId)
        }
        this.frameCallbackId = null
    }

    reset(): void {
        this.stop()
        this.presentedFrameCallbacks = 0
        this.lastPresentedFrameAtMs = null
        this.maxPresentationGapMs = 0
        this.previousReportAtMs = null
        this.previousPresentedFrameCallbacks = 0
        this.previousTotalFrames = null
        this.previousDroppedFrames = null
        this.lastStatsReportAtMs = null
    }

    report(statsObject: Record<string, StatValue>): void {
        const now = performance.now()
        this.lastStatsReportAtMs = now
        this.start()
        const reportIntervalSeconds = this.previousReportAtMs == null
            ? null
            : (now - this.previousReportAtMs) / 1000

        if (this.lastPresentedFrameAtMs != null) {
            statsObject.videoElementCurrentFrameAgeMs = now - this.lastPresentedFrameAtMs
            statsObject.videoElementMaxPresentationGapMs = this.maxPresentationGapMs
        }
        statsObject.videoElementPresentedFrameCallbacks = this.presentedFrameCallbacks
        statsObject.videoElementFrameCallback = typeof (
            this.videoElement as HTMLVideoElement & { requestVideoFrameCallback?: unknown }
        ).requestVideoFrameCallback == "function" ? "supported" : "unsupported"

        if (reportIntervalSeconds != null && reportIntervalSeconds > 0 && reportIntervalSeconds <= 10) {
            statsObject.videoElementCallbackFps = (
                this.presentedFrameCallbacks - this.previousPresentedFrameCallbacks
            ) / reportIntervalSeconds
        }

        if (typeof this.videoElement.getVideoPlaybackQuality == "function") {
            const quality = this.videoElement.getVideoPlaybackQuality()
            const totalFrames = quality.totalVideoFrames
            const droppedFrames = quality.droppedVideoFrames
            statsObject.videoElementTotalFrames = totalFrames
            statsObject.videoElementDroppedFrames = droppedFrames

            if (
                reportIntervalSeconds != null && reportIntervalSeconds > 0 && reportIntervalSeconds <= 10 &&
                this.previousTotalFrames != null && this.previousDroppedFrames != null &&
                totalFrames >= this.previousTotalFrames && droppedFrames >= this.previousDroppedFrames
            ) {
                const totalFramesDelta = totalFrames - this.previousTotalFrames
                const droppedFramesDelta = droppedFrames - this.previousDroppedFrames
                statsObject.videoElementPresentedFps = Math.max(0, totalFramesDelta - droppedFramesDelta) / reportIntervalSeconds
                statsObject.videoElementDroppedFps = droppedFramesDelta / reportIntervalSeconds
                if (totalFramesDelta > 0) {
                    statsObject.videoElementDropPercent = droppedFramesDelta * 100 / totalFramesDelta
                }
            }

            this.previousTotalFrames = totalFrames
            this.previousDroppedFrames = droppedFrames
        }

        this.previousReportAtMs = now
        this.previousPresentedFrameCallbacks = this.presentedFrameCallbacks
        this.maxPresentationGapMs = 0
    }
}

export class VideoElementRenderer implements TrackVideoRenderer, VideoRenderer {
    static readonly type = "videotrack"

    static async getInfo(): Promise<PipeInfo> {
        const supported = "HTMLVideoElement" in globalObject() && "srcObject" in HTMLVideoElement.prototype

        return {
            environmentSupported: supported,
            supportedVideoCodecs: supported ? detectCodecs() : emptyVideoCodecs()
        }
    }

    readonly implementationName: string = "video_element"

    private videoElement = document.createElement("video")
    private playbackStats = new VideoElementPlaybackStats(this.videoElement)
    private oldTrack: MediaStreamTrack | null = null
    private stream = new MediaStream()

    private size: [number, number] | null = null
    private hdrEnabled: boolean = false

    constructor() {
        this.videoElement.classList.add("video-stream")
        this.videoElement.preload = "none"
        this.videoElement.controls = false
        this.videoElement.autoplay = true
        this.videoElement.disablePictureInPicture = true
        this.videoElement.playsInline = true
        this.videoElement.muted = true

        if ("srcObject" in this.videoElement) {
            try {
                this.videoElement.srcObject = this.stream
            } catch (err: any) {
                if (err.name !== "TypeError") {
                    throw err;
                }

                console.error(err)
                throw `video_element renderer not supported: ${err}`
            }
        }

        addPipePassthrough(this)
    }

    async setup(setup: VideoRendererSetup) {
        this.size = [setup.width, setup.height]
    }
    cleanup(): void {
        this.playbackStats.stop()
        if (this.oldTrack) {
            this.stream.removeTrack(this.oldTrack)
        }
        this.videoElement.srcObject = null
    }

    setTrack(track: MediaStreamTrack): void {
        this.playbackStats.reset()
        if (this.oldTrack) {
            this.stream.removeTrack(this.oldTrack)
        }

        this.stream.addTrack(track)
        this.oldTrack = track
    }

    pollRequestIdr(): boolean {
        return false
    }

    mount(parent: HTMLElement): void {
        parent.appendChild(this.videoElement)
    }
    unmount(parent: HTMLElement): void {
        parent.removeChild(this.videoElement)
    }

    onUserInteraction(): void {
        if (this.videoElement.paused) {
            this.videoElement.play().then(() => {
                // Playing
            }).catch(error => {
                console.error(`Failed to play videoElement: ${error.message || error}`);
            })
        }
    }
    private getEffectiveVideoSize(): [number, number] | null {
        const width = this.videoElement.videoWidth
        const height = this.videoElement.videoHeight
        if (width > 0 && height > 0) {
            return [width, height]
        }

        return this.size
    }
    getStreamRect(): DOMRect {
        const effectiveSize = this.getEffectiveVideoSize()
        if (!effectiveSize) {
            return new DOMRect()
        }

        return getStreamRectCorrected(this.videoElement.getBoundingClientRect(), effectiveSize)
    }

    getBase(): Pipe | null {
        return null
    }

    async reportStats(statsObject: Record<string, StatValue>): Promise<void> {
        this.playbackStats.report(statsObject)
    }

    setHdrMode(enabled: boolean): void {
        this.hdrEnabled = enabled
        // Request HDR display mode if supported
        if (enabled && "requestHDR" in this.videoElement) {
            try {
                (this.videoElement as any).requestHDR()
            } catch (err) {
                console.warn("Failed to request HDR mode:", err)
            }
        }
        // Set color space attributes for HDR
        if (enabled) {
            this.videoElement.setAttribute("color-gamut", "rec2020")
            this.videoElement.setAttribute("transfer-function", "pq")
        } else {
            this.videoElement.removeAttribute("color-gamut")
            this.videoElement.removeAttribute("transfer-function")
        }
    }
}

export class UrlVideoElementRenderer implements UrlVideoRenderer, VideoRenderer {
    static readonly type = "videourl"

    static async getInfo(): Promise<PipeInfo> {
        const supported = "HTMLVideoElement" in globalObject() && "src" in HTMLVideoElement.prototype

        return {
            environmentSupported: supported,
            supportedVideoCodecs: supported ? detectCodecs() : emptyVideoCodecs()
        }
    }

    readonly implementationName: string = "video_element"

    private videoElement = document.createElement("video")
    private playbackStats = new VideoElementPlaybackStats(this.videoElement)

    private size: [number, number] | null = null

    constructor() {
        this.videoElement.classList.add("video-stream")
        this.videoElement.preload = "none"
        this.videoElement.controls = false
        this.videoElement.autoplay = true
        this.videoElement.disablePictureInPicture = true
        this.videoElement.playsInline = true
        this.videoElement.muted = true

        addPipePassthrough(this)
    }

    async setup(setup: VideoRendererSetup) {
        this.size = [setup.width, setup.height]
    }
    cleanup(): void {
        this.playbackStats.stop()
    }

    setUrl(src: string): void {
        this.playbackStats.reset()
        this.videoElement.src = src
    }

    pollRequestIdr(): boolean {
        return false
    }

    mount(parent: HTMLElement): void {
        parent.appendChild(this.videoElement)
    }
    unmount(parent: HTMLElement): void {
        parent.removeChild(this.videoElement)
    }

    onUserInteraction(): void {
        if (this.videoElement.paused) {
            this.videoElement.play().then(() => {
                // Playing
            }).catch(error => {
                console.error(`Failed to play videoElement: ${error.message || error}`);
            })
        }
    }
    private getEffectiveVideoSize(): [number, number] | null {
        const width = this.videoElement.videoWidth
        const height = this.videoElement.videoHeight
        if (width > 0 && height > 0) {
            return [width, height]
        }

        return this.size
    }
    getStreamRect(): DOMRect {
        const effectiveSize = this.getEffectiveVideoSize()
        if (!effectiveSize) {
            return new DOMRect()
        }

        return getStreamRectCorrected(this.videoElement.getBoundingClientRect(), effectiveSize)
    }

    getBase(): Pipe | null {
        return null
    }

    async reportStats(statsObject: Record<string, StatValue>): Promise<void> {
        this.playbackStats.report(statsObject)
    }

    setHdrMode(enabled: boolean): void {
        // Request HDR display mode if supported
        if (enabled && "requestHDR" in this.videoElement) {
            try {
                (this.videoElement as any).requestHDR()
            } catch (err) {
                console.warn("Failed to request HDR mode:", err)
            }
        }
        // Set color space attributes for HDR
        if (enabled) {
            this.videoElement.setAttribute("color-gamut", "rec2020")
            this.videoElement.setAttribute("transfer-function", "pq")
        } else {
            this.videoElement.removeAttribute("color-gamut")
            this.videoElement.removeAttribute("transfer-function")
        }
    }
}
