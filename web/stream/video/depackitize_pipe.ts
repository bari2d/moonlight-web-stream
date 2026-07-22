import { Logger } from "../log.js";
import { Pipe, PipeInfo } from "../pipeline/index.js";
import { addPipePassthrough, DataPipe } from "../pipeline/pipes.js";
import { allVideoCodecs } from "../video.js";
import { DataVideoRenderer, VideoRendererSetup } from "./index.js";

export class DepacketizeVideoPipe implements DataPipe {

    static readonly baseType = "videodata"
    static readonly type = "wsdata"

    static async getInfo(): Promise<PipeInfo> {
        // no link
        return {
            environmentSupported: true,
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    readonly implementationName: string

    private base: DataVideoRenderer

    private lastTimestampMicroseconds = 0
    private lastRawTimestampMicroseconds: number | null = null
    private timestampWrapOffsetMicroseconds = 0
    constructor(base: DataVideoRenderer, logger?: Logger) {
        this.implementationName = `depacketize_video -> ${base.implementationName}`
        this.base = base

        addPipePassthrough(this)
    }

    submitPacket(buffer: ArrayBuffer) {
        if (buffer.byteLength < 5) {
            return
        }
        const header = new DataView(buffer, 0, 5)
        const frameType = header.getUint8(0)
        const rawTimestamp = header.getUint32(1, false)

        // The compact wire timestamp is a wrapping u32. Extend it locally so
        // sessions longer than roughly 71 minutes keep monotonic WebCodecs and
        // MediaSource timestamps instead of producing a large negative jump.
        if (
            this.lastRawTimestampMicroseconds != null &&
            rawTimestamp < this.lastRawTimestampMicroseconds &&
            this.lastRawTimestampMicroseconds - rawTimestamp > 0x80000000
        ) {
            this.timestampWrapOffsetMicroseconds += 0x100000000
        }
        // A reconnect or a duplicated/reordered frame must not produce a
        // negative WebCodecs duration. Transport delivery is ordered, but the
        // compact timestamp can still repeat because it is only a u32.
        const timestamp = Math.max(
            rawTimestamp + this.timestampWrapOffsetMicroseconds,
            this.lastTimestampMicroseconds,
        )

        const duration = timestamp - this.lastTimestampMicroseconds
        this.base.submitDecodeUnit({
            type: frameType == 0 ? "delta" : "key",
            // Retain a view into the transport-owned frame instead of making
            // another full encoded-frame copy on every video callback.
            data: new Uint8Array(buffer, 5),
            durationMicroseconds: duration,
            timestampMicroseconds: timestamp,
        })
        this.lastTimestampMicroseconds = timestamp
        this.lastRawTimestampMicroseconds = rawTimestamp

    }

    setup(setup: VideoRendererSetup) {
        this.lastTimestampMicroseconds = 0
        this.lastRawTimestampMicroseconds = null
        this.timestampWrapOffsetMicroseconds = 0

        if ("setup" in this.base && typeof this.base.setup == "function") {
            return this.base.setup(...arguments)
        }
    }

    getBase(): Pipe | null {
        return this.base
    }
}
