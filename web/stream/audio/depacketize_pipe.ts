import { Pipe, PipeInfo } from "../pipeline/index.js";
import { addPipePassthrough, DataPipe } from "../pipeline/pipes.js";
import { AudioPlayerSetup, DataAudioPlayer } from "./index.js";

export class DepacketizeAudioPipe implements DataPipe {

    static async getInfo(): Promise<PipeInfo> {
        return {
            environmentSupported: true
        }
    }

    static readonly baseType = "audiodata"
    static readonly type = "wsdata"

    readonly implementationName: string

    private base: DataAudioPlayer
    private timestampMicroseconds: number = 0
    private packetDurationMicroseconds: number = 0

    constructor(base: DataAudioPlayer) {
        this.implementationName = `depacketize_audio -> ${base.implementationName}`
        this.base = base

        addPipePassthrough(this)
    }

    setup(setup: AudioPlayerSetup) {
        const packetDurationMicroseconds = setup.samplesPerFrame * 1_000_000 / setup.sampleRate
        this.packetDurationMicroseconds = Number.isFinite(packetDurationMicroseconds) && packetDurationMicroseconds > 0
            ? packetDurationMicroseconds
            : 1
        this.timestampMicroseconds = 0

        if ("setup" in this.base && typeof this.base.setup == "function") {
            return this.base.setup(...arguments)
        }
    }

    submitPacket(buffer: ArrayBuffer) {
        const timestampMicroseconds = Math.round(this.timestampMicroseconds)
        this.timestampMicroseconds += this.packetDurationMicroseconds
        const nextTimestampMicroseconds = Math.max(
            timestampMicroseconds + 1,
            Math.round(this.timestampMicroseconds),
        )

        this.base.decodeAndPlay({
            data: buffer,
            timestampMicroseconds,
            durationMicroseconds: nextTimestampMicroseconds - timestampMicroseconds,
        })
    }

    getBase(): Pipe | null {
        return this.base
    }
}
