import { globalObject } from "../../util.js";
import { Logger } from "../log.js";
import { Pipe, PipeInfo } from "../pipeline/index.js";
import { addPipePassthrough } from "../pipeline/pipes.js";
import { SampleAudioPlayer, TrackAudioPlayer } from "./index.js";

export class AudioMediaStreamTrackGeneratorPipe implements SampleAudioPlayer {

    static readonly baseType = "audiotrack"
    static readonly type = "audiosample"

    static async getInfo(): Promise<PipeInfo> {
        return {
            environmentSupported: "MediaStreamTrackGenerator" in globalObject()
        }
    }

    implementationName: string

    private base: TrackAudioPlayer

    private trackGenerator: MediaStreamTrackGenerator
    private writer: WritableStreamDefaultWriter<AudioData>
    private logger: Logger | null

    constructor(base: TrackAudioPlayer, logger?: Logger) {
        this.implementationName = `audio_media_stream_track_generator -> ${base.implementationName}`
        this.base = base
        this.logger = logger ?? null

        this.trackGenerator = new MediaStreamTrackGenerator({ kind: "audio" })
        this.writer = this.trackGenerator.writable.getWriter()

        addPipePassthrough(this)
    }

    private isFirstSample = true
    private writeInFlight = false
    private pendingSample: AudioData | null = null
    private stopped = false

    private closeSample(sample: AudioData): void {
        try {
            sample.close()
        } catch (error) {
            this.logger?.debug(`Failed to close a discarded audio sample: ${String(error)}`)
        }
    }

    submitSample(sample: AudioData): void {
        if (this.stopped) {
            this.closeSample(sample)
            return
        }

        if (this.isFirstSample) {
            this.isFirstSample = false

            this.base.setTrack(this.trackGenerator)
        }

        if (this.writeInFlight) {
            if (this.pendingSample) {
                this.closeSample(this.pendingSample)
            }
            this.pendingSample = sample
        } else {
            this.startWrite(sample)
        }
    }

    private startWrite(sample: AudioData): void {
        this.writeInFlight = true
        void this.writeSample(sample)
    }

    private async writeSample(sample: AudioData): Promise<void> {
        try {
            await this.writer.ready
            if (this.stopped) {
                this.closeSample(sample)
                return
            }

            // The generator owns an accepted sample. Close only samples that
            // are discarded before, or rejected by, write().
            await this.writer.write(sample)
        } catch (error) {
            this.closeSample(sample)
            this.logger?.debug(`Audio track generator write failed: ${String(error)}`)
        } finally {
            this.writeInFlight = false

            const nextSample = this.pendingSample
            this.pendingSample = null
            if (nextSample) {
                if (this.stopped) {
                    this.closeSample(nextSample)
                } else {
                    this.startWrite(nextSample)
                }
            }
        }
    }

    cleanup(): void {
        if (this.stopped) {
            return
        }

        this.stopped = true
        if (this.pendingSample) {
            this.closeSample(this.pendingSample)
        }
        this.pendingSample = null
        this.trackGenerator.stop()

        void this.abortWriter()

        if ("cleanup" in this.base && typeof this.base.cleanup == "function") {
            this.base.cleanup()
        }
    }

    private async abortWriter(): Promise<void> {
        try {
            await this.writer.abort()
        } catch (error) {
            this.logger?.debug(`Failed to abort audio track generator writer: ${String(error)}`)
        }

        try {
            this.writer.releaseLock()
        } catch (error) {
            this.logger?.debug(`Failed to release audio track generator writer: ${String(error)}`)
        }
    }

    getBase(): Pipe | null {
        return this.base
    }

}
