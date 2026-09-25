import { copyIntoYuv, yuvBufferSize } from "../../libopenh264/index.js"
import { globalObject } from "../../util.js"
import { Logger } from "../log.js"
import { Pipe, PipeInfo } from "../pipeline/index.js"
import { addPipePassthrough } from "../pipeline/pipes.js"
import { allVideoCodecs } from "../video.js"
import { CanvasVideoRendererOptions } from "./canvas.js"
import { CanvasRenderer, FramePacingMode, FrameVideoRenderer, VideoRendererSetup, RgbaFrameVideoRenderer, RgbaVideoFrame, Yuv420FrameVideoRenderer, Yuv420VideoFrame } from "./index.js"
import { StatValue } from "../stats.js"

abstract class BaseCanvasFrameDrawPipe implements Pipe {

    static async getInfo(): Promise<PipeInfo> {
        // no link
        return {
            environmentSupported: "CanvasRenderingContext2D" in globalObject() || "OffscreenCanvasRenderingContext2D" in globalObject(),
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    static readonly baseType = "canvas"

    protected base: CanvasRenderer

    private animationFrameRequest: number | null = null
    private frameDirty = false

    private drawOnSubmit: boolean

    readonly implementationName

    constructor(implementationName: string, base: CanvasRenderer, _logger?: unknown, options?: unknown) {
        this.implementationName = implementationName
        this.base = base

        const opts = options as CanvasVideoRendererOptions | undefined
        this.drawOnSubmit = opts?.drawOnSubmit ?? true

        addPipePassthrough(this)
    }

    async setup(setup: VideoRendererSetup): Promise<void> {
        if ("setup" in this.base && typeof this.base.setup == "function") {
            return this.base.setup(...arguments)
        }
    }

    cleanup() {
        if (this.animationFrameRequest != null) {
            cancelAnimationFrame(this.animationFrameRequest)
            this.animationFrameRequest = null
        }
        this.frameDirty = false

        if ("cleanup" in this.base && typeof this.base.cleanup == "function") {
            return this.base.cleanup(...arguments)
        }
    }

    protected onFrameSubmitted() {
        this.frameDirty = true
        if (this.drawOnSubmit) {
            this.drawCurrentFrameIfReady()
            this.frameDirty = false
        } else if (this.animationFrameRequest == null) {
            this.animationFrameRequest = requestAnimationFrame(this.onAnimationFrame)
        }
    }

    /** Draw currentFrame to canvas if context and frame are ready. Only updates size when dimensions change. */
    protected abstract drawCurrentFrameIfReady(): void

    private onAnimationFrame = () => {
        this.animationFrameRequest = null
        if (this.frameDirty) {
            this.drawCurrentFrameIfReady()
            this.frameDirty = false
        }
    }

    getBase(): Pipe | null {
        return this.base
    }
}

type PacingProfile = {
    /// Longest extra hold the buffer may add, in frame intervals.
    maxDelayFrames: number
    /// Decoded frames kept alive at once. Hardware decoders own a small output
    /// pool, so this stays well below typical pool sizes.
    maxQueuedFrames: number
}

const PACING_PROFILES: Record<Exclude<FramePacingMode, "off">, PacingProfile> = {
    balanced: { maxDelayFrames: 2, maxQueuedFrames: 3 },
    smooth: { maxDelayFrames: 4, maxQueuedFrames: 5 },
}
// The arrival-offset floor may creep up this much per frame so a browser
// clock that runs slightly fast relative to the host does not make every
// frame look late forever (0.02 ms/frame covers ~1200 ppm at 60 fps).
const PACING_FLOOR_DRIFT_MS_PER_FRAME = 0.02
// How quickly the jitter allowance relaxes once arrivals calm down.
const PACING_DELAY_DECAY_MS_PER_FRAME = 0.05
const PACING_DEFAULT_REFRESH_MS = 1000 / 60

type FrameCallbackHandle = { cancel: () => void }

function requestFrameCallback(callback: (nowMs: number) => void): FrameCallbackHandle {
    const global = globalObject() as {
        requestAnimationFrame?: (callback: FrameRequestCallback) => number
        cancelAnimationFrame?: (handle: number) => void
    }
    if (typeof global.requestAnimationFrame == "function" && typeof global.cancelAnimationFrame == "function") {
        const handle = global.requestAnimationFrame(callback)
        return { cancel: () => global.cancelAnimationFrame!(handle) }
    }
    // Workers without requestAnimationFrame: poll a little faster than a
    // 120 Hz display would.
    const handle = setTimeout(() => callback(performance.now()), 4)
    return { cancel: () => clearTimeout(handle) }
}

function closeFrameQuietly(frame: VideoFrame): void {
    try {
        frame.close()
    } catch (_error) {
        // Already closed by the decoder or a transfer.
    }
}

/**
 * Small adaptive jitter buffer between the decoder and the canvas.
 *
 * Frames arrive with network jitter but carry the host's capture timestamps.
 * The pacer maps those timestamps onto the local clock (the smallest observed
 * arrival offset is the "on time" reference), adds a jitter allowance that
 * tracks how late frames have recently been, and presents each frame on the
 * display refresh at which it becomes due. Frames that are already overdue
 * are skipped in favour of the newest due frame, so a burst after a stall is
 * caught up instead of replayed, and the allowance is capped so the buffer
 * can never add more than a couple of frame intervals of latency.
 */
class VideoFramePacer {
    private readonly queue: Array<VideoFrame> = []
    private floorOffsetMs: number | null = null
    private targetDelayMs = 0
    private frameIntervalMs = PACING_DEFAULT_REFRESH_MS
    private refreshIntervalMs = PACING_DEFAULT_REFRESH_MS
    private lastTickMs: number | null = null
    private pending: FrameCallbackHandle | null = null

    framesPresented = 0
    framesDropped = 0

    constructor(
        private readonly profile: PacingProfile,
        private readonly present: (frame: VideoFrame) => void,
    ) { }

    setFrameRate(fps: number): void {
        if (fps > 0) {
            this.frameIntervalMs = 1000 / fps
        }
    }

    get delayMs(): number {
        return this.targetDelayMs
    }

    get queuedFrames(): number {
        return this.queue.length
    }

    push(frame: VideoFrame, nowMs: number): void {
        const timestampMs = frame.timestamp / 1000
        const offsetMs = nowMs - timestampMs
        if (this.floorOffsetMs == null || offsetMs < this.floorOffsetMs) {
            this.floorOffsetMs = offsetMs
        } else {
            this.floorOffsetMs += PACING_FLOOR_DRIFT_MS_PER_FRAME
        }
        const jitterMs = Math.max(0, offsetMs - this.floorOffsetMs)
        const maxDelayMs = this.profile.maxDelayFrames * this.frameIntervalMs
        this.targetDelayMs = Math.min(
            maxDelayMs,
            Math.max(jitterMs, this.targetDelayMs - PACING_DELAY_DECAY_MS_PER_FRAME),
        )

        this.queue.push(frame)
        while (this.queue.length > this.profile.maxQueuedFrames) {
            const dropped = this.queue.shift()
            if (dropped) {
                closeFrameQuietly(dropped)
                this.framesDropped++
            }
        }
        this.schedule()
    }

    clear(): void {
        if (this.pending) {
            this.pending.cancel()
            this.pending = null
        }
        for (const frame of this.queue.splice(0)) {
            closeFrameQuietly(frame)
        }
        this.floorOffsetMs = null
        this.targetDelayMs = 0
        this.lastTickMs = null
    }

    private schedule(): void {
        if (this.pending != null || this.queue.length == 0) {
            return
        }
        this.pending = requestFrameCallback(this.onTick)
    }

    private dueTimeMs(frame: VideoFrame): number {
        return frame.timestamp / 1000 + (this.floorOffsetMs ?? 0) + this.targetDelayMs
    }

    private readonly onTick = (nowMs: number) => {
        this.pending = null
        if (this.lastTickMs != null) {
            const delta = nowMs - this.lastTickMs
            if (delta > 1 && delta < 100) {
                this.refreshIntervalMs = this.refreshIntervalMs * 0.9 + delta * 0.1
            }
        }
        this.lastTickMs = nowMs

        // Anything due before the next refresh is presented now; of several
        // due frames only the newest is shown and the rest are skipped.
        const horizonMs = nowMs + this.refreshIntervalMs / 2
        let dueIndex = -1
        for (let index = 0; index < this.queue.length; index++) {
            if (this.dueTimeMs(this.queue[index]) <= horizonMs) {
                dueIndex = index
            } else {
                break
            }
        }
        if (dueIndex >= 0) {
            for (let index = 0; index < dueIndex; index++) {
                closeFrameQuietly(this.queue[index])
                this.framesDropped++
            }
            const frame = this.queue[dueIndex]
            this.queue.splice(0, dueIndex + 1)
            this.framesPresented++
            this.present(frame)
        }
        this.schedule()
    }
}

export class CanvasFrameDrawPipe extends BaseCanvasFrameDrawPipe implements FrameVideoRenderer {

    static async getInfo(): Promise<PipeInfo> {
        // no link
        return {
            environmentSupported: "CanvasRenderingContext2D" in globalObject() || "OffscreenCanvasRenderingContext2D" in globalObject(),
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    static readonly type = "videoframe"

    private currentFrame: VideoFrame | null = null
    private readonly pacer: VideoFramePacer | null
    private readonly pacingMode: FramePacingMode

    constructor(base: CanvasRenderer, _logger?: unknown, options?: unknown) {
        super(`canvas_frame -> ${base.implementationName}`, base, _logger, options)

        const opts = options as CanvasVideoRendererOptions | undefined
        this.pacingMode = opts?.framePacing ?? "off"
        this.pacer = this.pacingMode == "off"
            ? null
            : new VideoFramePacer(PACING_PROFILES[this.pacingMode], frame => this.presentPacedFrame(frame))

        addPipePassthrough(this)
    }

    async setup(setup: VideoRendererSetup): Promise<void> {
        this.pacer?.clear()
        this.pacer?.setFrameRate(setup.fps)
        return super.setup(setup)
    }

    submitFrame(frame: VideoFrame): void {
        if (this.pacer) {
            this.pacer.push(frame, performance.now())
            return
        }

        this.currentFrame?.close()

        this.currentFrame = frame
        this.onFrameSubmitted()
    }

    private presentPacedFrame(frame: VideoFrame): void {
        this.currentFrame?.close()
        this.currentFrame = frame
        // Called from the display refresh callback, so draw straight away.
        this.drawCurrentFrameIfReady()
    }

    /** Draw currentFrame to canvas if context and frame are ready. Only updates size when dimensions change. */
    protected drawCurrentFrameIfReady(): void {
        const frame = this.currentFrame
        const { context, error } = this.base.useCanvasContext("2d")
        if (!frame || error) {
            return
        }

        const w = frame.displayWidth
        const h = frame.displayHeight
        this.base.setCanvasSize(w, h)

        context.clearRect(0, 0, w, h)
        context.drawImage(frame, 0, 0, w, h)

        this.base.commitFrame()
        this.currentFrame = null
        frame.close()
    }

    async reportStats(statsObject: Record<string, StatValue>): Promise<void> {
        statsObject.canvasFramePacing = this.pacingMode
        if (this.pacer) {
            statsObject.canvasPacingDelayMs = Math.round(this.pacer.delayMs * 10) / 10
            statsObject.canvasPacingQueuedFrames = this.pacer.queuedFrames
            statsObject.canvasPacingFramesPresented = this.pacer.framesPresented
            statsObject.canvasPacingFramesSkipped = this.pacer.framesDropped
        }

        const base = this.base as { reportStats?: (statsObject: Record<string, StatValue>) => Promise<void> | void }
        if (typeof base.reportStats == "function") {
            await base.reportStats(statsObject)
        }
    }

    cleanup() {
        this.pacer?.clear()
        this.currentFrame?.close()
        this.currentFrame = null
        return super.cleanup()
    }
}

export class CanvasRgbaFrameDrawPipe extends BaseCanvasFrameDrawPipe implements RgbaFrameVideoRenderer {

    static async getInfo(): Promise<PipeInfo> {
        // no link
        return {
            environmentSupported: "CanvasRenderingContext2D" in globalObject() || "OffscreenCanvasRenderingContext2D" in globalObject(),
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    static readonly type = "rgbavideoframe"

    private currentFrame: ImageData | null = null

    constructor(base: CanvasRenderer, _logger?: unknown, options?: unknown) {
        super(`rgba_canvas_frame -> ${base.implementationName}`, base, _logger, options)

        addPipePassthrough(this)
    }

    submitRawFrame(frame: RgbaVideoFrame): void {
        this.currentFrame = new ImageData(frame.buffer, frame.width, frame.height)

        this.onFrameSubmitted()
    }

    /** Draw currentFrame to canvas if context and frame are ready. Only updates size when dimensions change. */
    protected drawCurrentFrameIfReady(): void {
        const frame = this.currentFrame
        const { context, error } = this.base.useCanvasContext("2d")
        if (!frame || error) {
            return
        }

        const w = frame.width
        const h = frame.height
        this.base.setCanvasSize(w, h)

        context.clearRect(0, 0, w, h)
        context.putImageData(frame, 0, 0)

        this.base.commitFrame()
        this.currentFrame = null
    }

    cleanup() {
        this.currentFrame = null
        return super.cleanup()
    }
}

export class CanvasYuv420FrameDrawPipe extends BaseCanvasFrameDrawPipe implements Yuv420FrameVideoRenderer {
    static async getInfo(): Promise<PipeInfo> {
        // no link
        return {
            environmentSupported: "WebGLRenderingContext" in globalObject(),
            supportedVideoCodecs: allVideoCodecs()
        }
    }

    static readonly type = "yuv420videoframe"

    private logger: Logger | null
    private errored = false

    constructor(base: CanvasRenderer, logger?: Logger, options?: unknown) {
        super(`rgba_canvas_frame -> ${base.implementationName}`, base, logger, options)
        this.logger = logger ?? null

        addPipePassthrough(this)
    }

    private sizeChanged = false
    private width: number = -1
    private height: number = -1
    private currentFrame: Uint8Array | null = null

    submitRawFrame(frame: Yuv420VideoFrame): void {
        if (this.errored) {
            return
        }

        const bufferSize = yuvBufferSize(frame.width, frame.height)
        if (!this.currentFrame || this.currentFrame.length < bufferSize) {
            this.currentFrame = new Uint8Array(bufferSize)
        }

        copyIntoYuv([frame.yPlane, frame.uPlane, frame.vPlane], [frame.yStride, frame.uvStride], frame.width, frame.height, this.currentFrame)

        if (this.width != frame.width || this.height != frame.height) {
            this.width = frame.width
            this.height = frame.height
            this.sizeChanged = true
        }

        this.onFrameSubmitted()
    }

    private textureY: WebGLTexture | null = null
    private textureU: WebGLTexture | null = null
    private textureV: WebGLTexture | null = null

    private program: WebGLProgram | null = null
    private quad: WebGLBuffer | null = null

    /** Draw currentFrame to canvas if context and frame are ready. Only updates size when dimensions change. */
    protected drawCurrentFrameIfReady(): void {
        if (this.errored) {
            return
        }

        const frame = this.currentFrame
        const { context: gl, error } = this.base.useCanvasContext("webgl")
        if (!frame || error) {
            return
        }

        const w = this.width
        const h = this.height
        this.base.setCanvasSize(w, h)
        gl.viewport(0, 0, w, h)

        // -- Create Program if not present
        const program = this.getProgram(gl)
        if (!program) {
            return
        }

        // -- Create and Bind Quad to program
        const quadBuffer = this.bindQuadBuffer(gl, program)
        if (!quadBuffer) {
            return
        }

        // -- Create and bind Texture correctly

        // sizeChanged will realloc a texture -> set it to true
        if (!this.textureY) {
            this.textureY = this.createTexture(gl)
            this.sizeChanged = true

            if (!this.setProgramTexture(gl, program, "textureY", this.textureY, 1)) {
                return
            }
        }
        if (!this.textureU) {
            this.textureU = this.createTexture(gl)
            this.sizeChanged = true

            if (!this.setProgramTexture(gl, program, "textureU", this.textureU, 2)) {
                return
            }
        }
        if (!this.textureV) {
            this.textureV = this.createTexture(gl)
            this.sizeChanged = true

            if (!this.setProgramTexture(gl, program, "textureV", this.textureV, 3)) {
                return
            }
        }

        // -- Upload texture to the gpu
        const size = this.width * this.height
        const uvWidth = this.width >> 1
        const uvHeight = this.height >> 1
        const uvSize = uvWidth * uvHeight

        if (this.sizeChanged) {
            // Realloc
            gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1)

            gl.bindTexture(gl.TEXTURE_2D, this.textureY)
            gl.texImage2D(gl.TEXTURE_2D, 0, gl.LUMINANCE, this.width, this.height, 0, gl.LUMINANCE, gl.UNSIGNED_BYTE, frame.subarray(0, size))

            gl.bindTexture(gl.TEXTURE_2D, this.textureU)
            gl.texImage2D(gl.TEXTURE_2D, 0, gl.LUMINANCE, uvWidth, uvHeight, 0, gl.LUMINANCE, gl.UNSIGNED_BYTE, frame.subarray(size, size + uvSize))

            gl.bindTexture(gl.TEXTURE_2D, this.textureV)
            gl.texImage2D(gl.TEXTURE_2D, 0, gl.LUMINANCE, uvWidth, uvHeight, 0, gl.LUMINANCE, gl.UNSIGNED_BYTE, frame.subarray(size + uvSize, size + uvSize + uvSize))

            this.sizeChanged = false
        } else {
            // Only reassign
            gl.bindTexture(gl.TEXTURE_2D, this.textureY)
            gl.texSubImage2D(gl.TEXTURE_2D, 0, 0, 0, this.width, this.height, gl.LUMINANCE, gl.UNSIGNED_BYTE, frame.subarray(0, size))

            gl.bindTexture(gl.TEXTURE_2D, this.textureU)
            gl.texSubImage2D(gl.TEXTURE_2D, 0, 0, 0, uvWidth, uvHeight, gl.LUMINANCE, gl.UNSIGNED_BYTE, frame.subarray(size, size + uvSize))

            gl.bindTexture(gl.TEXTURE_2D, this.textureV)
            gl.texSubImage2D(gl.TEXTURE_2D, 0, 0, 0, uvWidth, uvHeight, gl.LUMINANCE, gl.UNSIGNED_BYTE, frame.subarray(size + uvSize, size + uvSize + uvSize))
        }

        // -- Draw the frame
        gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4)

        this.base.commitFrame()
    }

    private getProgram(gl: WebGLRenderingContext): WebGLProgram | null {
        const VERTEX_SHADER = `
attribute vec2 aPosition;

varying vec2 vPosition;

void main() {
   gl_Position = vec4(aPosition, 0.0, 1.0);
   vPosition = vec2(aPosition.x, -aPosition.y);
}
`
        const FRAGMENT_SHADER = `
precision mediump float;

varying vec2 vPosition;

uniform sampler2D textureY;
uniform sampler2D textureU;
uniform sampler2D textureV;
 
void main() {
    vec2 texCoord = vPosition.xy * 0.5 + 0.5;

    float y = texture2D(textureY, texCoord).r;
    float u = texture2D(textureU, texCoord).r - 0.5;
    float v = texture2D(textureV, texCoord).r - 0.5;

    // BT.601 conversion
    float r = y + (1.402 * v);
    float g = y - (0.344136 * u) - (0.714136 * v);
    float b = y + (1.772 * u);

    gl_FragColor = vec4(r, g, b, 1.0);
}
`

        if (!this.program) {
            // Vertex Shader
            const vertexShader = gl.createShader(gl.VERTEX_SHADER)
            if (!vertexShader) {
                this.errored = true
                this.logger?.debug("Failed to create vertex shader!", { type: "fatalDescription" })
                return null
            }

            gl.shaderSource(vertexShader, VERTEX_SHADER)
            gl.compileShader(vertexShader)

            if (!gl.getShaderParameter(vertexShader, gl.COMPILE_STATUS)) {
                const log = gl.getShaderInfoLog(vertexShader)
                this.errored = true
                this.logger?.debug("Failed to compile vertex shader!", { type: "fatalDescription" })
                if (log) {
                    this.logger?.debug(log)
                }
                return null
            }

            // Fragment Shader
            const fragmentShader = gl.createShader(gl.FRAGMENT_SHADER)
            if (!fragmentShader) {
                this.errored = true
                this.logger?.debug("Failed to create fragment shader!", { type: "fatalDescription" })
                return null
            }

            gl.shaderSource(fragmentShader, FRAGMENT_SHADER)
            gl.compileShader(fragmentShader)

            if (!gl.getShaderParameter(fragmentShader, gl.COMPILE_STATUS)) {
                this.errored = true
                const log = gl.getShaderInfoLog(fragmentShader)
                this.logger?.debug("Failed to compile fragment shader!", { type: "fatalDescription" })
                if (log) {
                    this.logger?.debug(log)
                }
                return null
            }

            // Link Program
            const program = gl.createProgram()
            gl.attachShader(program, vertexShader)
            gl.attachShader(program, fragmentShader)
            gl.linkProgram(program)
            if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
                this.errored = true
                const log = gl.getProgramInfoLog(program)
                this.logger?.debug("Failed to link program!", { type: "fatalDescription" })
                if (log) {
                    this.logger?.debug(log)
                }
                return null
            }
            this.program = program

            // Use program
            gl.useProgram(this.program)

            // Mark shaders for deletion, is allowed at this point as we've got them in a program
            gl.deleteShader(vertexShader)
            gl.deleteShader(fragmentShader)
        }

        return this.program
    }

    private bindQuadBuffer(gl: WebGLRenderingContext, program: WebGLProgram): WebGLBuffer | null {
        if (!this.quad) {
            // Note: We're using a triangle strip
            const quadData = new Float32Array([
                -1, 1,  // Top Left
                -1, -1,  // Bottom Left
                1, 1,  // Top Right
                1, -1,  // Bottom Right
            ])

            const quad = gl.createBuffer()
            gl.bindBuffer(gl.ARRAY_BUFFER, quad)
            gl.bufferData(gl.ARRAY_BUFFER, quadData, gl.STATIC_DRAW)
            this.quad = quad

            // Set Vertex Attribute
            const quadAttrib = gl.getAttribLocation(program, "aPosition")
            if (quadAttrib == -1) {
                this.errored = true
                this.logger?.debug("Failed to get \"aPosition\" attribute from program", { type: "fatalDescription" })
                return null
            }

            gl.enableVertexAttribArray(quadAttrib)
            gl.vertexAttribPointer(quadAttrib, 2, gl.FLOAT, false, 0, 0)
        }

        return this.quad
    }

    private createTexture(gl: WebGLRenderingContext): WebGLTexture {
        const texture = gl.createTexture()

        gl.bindTexture(gl.TEXTURE_2D, texture)

        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR)
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR)
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)

        return texture
    }
    private setProgramTexture(gl: WebGLRenderingContext, program: WebGLProgram, name: string, texture: WebGLTexture, textureId: number): boolean {
        const uniform = gl.getUniformLocation(program, name)
        if (uniform == null) {
            this.errored = true
            this.logger?.debug(`Failed to find uniform "${name}"`, { type: "fatalDescription" })
            return false
        }

        gl.activeTexture(gl.TEXTURE0 + textureId)
        gl.bindTexture(gl.TEXTURE_2D, texture)
        gl.uniform1i(uniform, textureId)

        // For further calls use the first texture
        gl.activeTexture(gl.TEXTURE0)

        return true
    }

    cleanup() {
        this.currentFrame = null

        const { context: gl } = this.base.useCanvasContext("webgl")
        if (gl) {
            gl.deleteTexture(this.textureY)
            this.textureY = null

            gl.deleteTexture(this.textureU)
            this.textureU = null

            gl.deleteTexture(this.textureV)
            this.textureV = null

            gl.deleteProgram(this.program)
            this.program = null

            gl.deleteBuffer(this.quad)
            this.quad = null
        }

        return super.cleanup()
    }
}
