import { globalObject } from "../../util.js";
import { Logger } from "../log.js";
import { Pipe, PipeInfo } from "../pipeline/index.js";
import { addPipePassthrough } from "../pipeline/pipes.js";
import { allVideoCodecs } from "../video.js";
import { FrameVideoRenderer, TrackVideoRenderer } from "./index.js";

export class VideoTrackGeneratorPipe implements FrameVideoRenderer {
    static readonly baseType = "videotrack"
    static readonly type = "videoframe"

    static async getInfo(): Promise<PipeInfo> {
        // https://developer.mozilla.org/en-US/docs/Web/API/VideoTrackGenerator
        return {
            environmentSupported: "VideoTrackGenerator" in globalObject(),
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    readonly implementationName: string

    private base: TrackVideoRenderer

    private trackGenerator: VideoTrackGenerator
    private writer: WritableStreamDefaultWriter<VideoFrame>
    private logger: Logger | null

    constructor(base: TrackVideoRenderer, logger?: Logger) {
        this.implementationName = `video_track_generator -> ${base.implementationName}`
        this.base = base
        this.logger = logger ?? null

        this.trackGenerator = new VideoTrackGenerator()
        this.writer = this.trackGenerator.writable.getWriter()

        addPipePassthrough(this)
    }

    private isFirstSample = true
    private writeInFlight = false
    private pendingFrame: VideoFrame | null = null
    private stopped = false

    private closeFrame(frame: VideoFrame): void {
        try {
            frame.close()
        } catch (error) {
            this.logger?.debug(`Failed to close a discarded video frame: ${String(error)}`)
        }
    }

    submitFrame(frame: VideoFrame): void {
        if (this.stopped) {
            this.closeFrame(frame)
            return
        }

        if (this.isFirstSample) {
            this.isFirstSample = false

            this.base.setTrack(this.trackGenerator.track)
        }

        if (this.writeInFlight) {
            if (this.pendingFrame) {
                this.closeFrame(this.pendingFrame)
            }
            this.pendingFrame = frame
        } else {
            this.startWrite(frame)
        }
    }

    private startWrite(frame: VideoFrame): void {
        this.writeInFlight = true
        void this.writeFrame(frame)
    }

    private async writeFrame(frame: VideoFrame): Promise<void> {
        try {
            await this.writer.ready
            if (this.stopped) {
                this.closeFrame(frame)
                return
            }

            // The generator owns an accepted frame. Some implementations close it
            // automatically, so only close frames that never make it through write().
            await this.writer.write(frame)
        } catch (error) {
            this.closeFrame(frame)
            this.logger?.debug(`Video track generator write failed: ${String(error)}`)
        } finally {
            this.writeInFlight = false

            const nextFrame = this.pendingFrame
            this.pendingFrame = null
            if (nextFrame) {
                if (this.stopped) {
                    this.closeFrame(nextFrame)
                } else {
                    this.startWrite(nextFrame)
                }
            }
        }
    }

    cleanup(): void {
        if (this.stopped) {
            return
        }

        this.stopped = true
        if (this.pendingFrame) {
            this.closeFrame(this.pendingFrame)
        }
        this.pendingFrame = null
        this.trackGenerator.track.stop()

        void this.abortWriter()

        if ("cleanup" in this.base && typeof this.base.cleanup == "function") {
            this.base.cleanup()
        }
    }

    private async abortWriter(): Promise<void> {
        try {
            await this.writer.abort()
        } catch (error) {
            this.logger?.debug(`Failed to abort video track generator writer: ${String(error)}`)
        }

        try {
            this.writer.releaseLock()
        } catch (error) {
            this.logger?.debug(`Failed to release video track generator writer: ${String(error)}`)
        }
    }

    getBase(): Pipe | null {
        return this.base
    }
}
