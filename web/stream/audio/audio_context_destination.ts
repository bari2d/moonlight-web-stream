import { globalObject } from "../../util.js";
import { Logger } from "../log.js";
import { PipeInfo } from "../pipeline/index.js";
import { AudioContextBasePipe } from "./audio_context_base.js";
import { AudioPlayer, AudioPlayerSetup } from "./index.js";

export class ContextDestinationNodeAudioPlayer extends AudioContextBasePipe implements AudioPlayer {

    static async getInfo(): Promise<PipeInfo> {
        return {
            environmentSupported: "AudioContext" in globalObject()
        }
    }

    static readonly type = "audionode"

    private destination: AudioNode | null = null
    private currentSource: AudioNode | null = null

    constructor(logger?: Logger) {
        super("node_audio_element", null, logger)

        this.addPipePassthrough()
    }

    setup(setup: AudioPlayerSetup) {
        const result = super.setup(setup)

        this.destination = this.getAudioContext().destination;

        // A source from a replaced context cannot be connected here; its
        // owner supplies a new one through setSource right after setup.
        if (this.currentSource && this.currentSource.context == this.destination.context) {
            this.currentSource.connect(this.destination)
        } else {
            this.currentSource = null
        }

        return result
    }

    setSource(source: AudioNode): void {
        if (this.currentSource && this.destination && this.currentSource.context == this.destination.context) {
            try {
                this.currentSource.disconnect(this.destination)
            } catch (_error) { }
        }

        this.currentSource = source

        if (this.destination) {
            source.connect(this.destination)
        }
    }

    mount(_parent: HTMLElement): void { }
    unmount(_parent: HTMLElement): void { }

}