
import { Logger } from "../log.js";
import { Pipe } from "../pipeline/index.js";
import { addPipePassthrough } from "../pipeline/pipes.js";
import { StatValue } from "../stats.js";
import { AudioPlayerSetup, NodeAudioPlayer } from "./index.js";

export abstract class AudioContextBasePipe implements NodeAudioPlayer {

    readonly implementationName: string

    private logger: Logger | null = null

    private base: Pipe | null
    private audioContext: AudioContext | null = null

    constructor(implementationName: string, base: Pipe | null, logger?: Logger) {
        this.logger = logger ?? null

        this.implementationName = implementationName
        this.base = base
    }

    protected addPipePassthrough() {
        addPipePassthrough(this, ["mount", "unmount"])
    }

    setup(setup: AudioPlayerSetup) {
        // Adaptive bitrate changes reconnect the native media session while the
        // browser pipeline stays alive. Keep the running context when the
        // sample rate is unchanged: its nodes stay valid (mixing nodes from two
        // contexts throws InvalidAccessError) and iOS Safari would start a
        // replacement context suspended until the next user gesture.
        if (
            this.audioContext &&
            this.audioContext.state != "closed" &&
            this.audioContext.sampleRate == setup.sampleRate
        ) {
            if (this.base && "setup" in this.base && typeof this.base.setup == "function") {
                return this.base.setup(...arguments)
            }
            return
        }
        // Otherwise release the previous context before replacing it so every
        // reconnect does not leave an audio device and rendering graph running
        // in the background.
        if (this.audioContext) {
            void this.audioContext.close()
            this.audioContext = null
        }

        try {
            this.audioContext = new AudioContext({
                latencyHint: "interactive",
                sampleRate: setup.sampleRate
            })
        } catch (e: any) {
            this.logger?.debug(`Failed to setup audio node with latency hint "interactive". Trying to setup without latency hint. ${"toString" in e && typeof e.toString == "function" ? e.toString() : e}`)
        }

        if (!this.audioContext) {
            this.audioContext = new AudioContext({
                sampleRate: setup.sampleRate
            })
        }

        if (this.base && "setup" in this.base && typeof this.base.setup == "function") {
            return this.base.setup(...arguments)
        }
    }
    cleanup(): void {
        this.audioContext?.close()
    }

    onUserInteraction(): void {
        if (this.base && "onUserInteraction" in this.base && typeof this.base.onUserInteraction == "function") {
            return this.base.onUserInteraction(...arguments)
        }
    }

    abstract setSource(source: AudioNode): void

    async reportStats(statsObject: Record<string, StatValue>): Promise<void> {
        // Both values are in secs -> we convert into ms
        if (this.audioContext?.baseLatency) {
            statsObject.audioContextBaseLatencyMs = this.audioContext.baseLatency * 1000
        } else {
            statsObject.audioContextBaseLatencyMs = "null"
        }
        if (this.audioContext?.outputLatency) {
            statsObject.audioContextOutputLatencyMs = this.audioContext.outputLatency * 1000
        } else {
            statsObject.audioContextOutputLatencyMs = "null"
        }

        if (this.base && "reportStats" in this.base && typeof this.base.reportStats == "function") {
            // @ts-ignore
            return await this.base.reportStats(...arguments)
        }
    }

    getAudioContext(): AudioContext {
        if (!this.audioContext) {
            this.logger?.debug("Failed to get audio context", { type: "fatal" })
            throw "Failed to get audio context."
        }
        return this.audioContext
    }

    getBase(): Pipe | null {
        return this.base
    }

    // -- Only definition look addPipePassthrough
    mount(_parent: HTMLElement): void { }
    unmount(_parent: HTMLElement): void { }
}
