import { StreamCapabilities, StreamControllerCapabilities, StreamMouseButton, TransportChannelId } from "../api_bindings.js"
import { showNotification } from "../component/notification.js"
import { ByteBuffer, I16_MAX, U16_MAX, U8_MAX } from "./buffer.js"
import { areGamepadStatesEqual, ControllerConfig, emptyGamepadState, extractGamepadState, GamepadState, SUPPORTED_BUTTONS } from "./gamepad.js"
import { convertToKey, convertToModifiers } from "./keyboard.js"
import { convertToButton } from "./mouse.js"
import { DataTransportChannel, Transport, TransportChannelIdKey, TransportChannelIdValue } from "./transport/index.js"

// Smooth scrolling multiplier
const TOUCH_HIGH_RES_SCROLL_MULTIPLIER = 10
// Normal scrolling multiplier
const TOUCH_SCROLL_MULTIPLIER = 1
// Distance until a touch cannot be a click anymore
const TOUCH_AS_CLICK_MAX_DISTANCE = 2
// Time till it's registered as a click, else it might be scrolling
const TOUCH_AS_CLICK_MIN_TIME_MS = 100
// Everything greater than this is a right click
const TOUCH_AS_CLICK_MAX_TIME_MS = 350
// How much to move to open up the screen keyboard when having three touches at the same time
const TOUCHES_AS_KEYBOARD_DISTANCE = 100
// Two-finger scroll only starts after one finger clearly commits to scrolling
const TWO_TOUCH_SCROLL_TRIGGER_DISTANCE = 3
// How long is the first tap allowed to be for it to maybe be a double tap
const DOUBLE_TAP_FIRST_TAP_MAX_TIME_MS = 100
// How much time is allowed after a touch release for a new tap to count both taps as a double tap
const DOUBLE_TAP_SECOND_TAP_MAX_TIME_MS = 200

const CONTROLLER_RUMBLE_INTERVAL_MS = 60
const CONTROLLER_STATE_REFRESH_INTERVAL_MS = 250
const I16_MIN = -I16_MAX - 1
const MAX_RELATIVE_MOUSE_CHUNKS = 64
const RELATIVE_MOUSE_FLUSH_INTERVAL_MS = 4
const MAX_TEXT_PACKET_BYTES = U8_MAX

const TOUCH_EVENT_DOWN = 0
const TOUCH_EVENT_MOVE = 1
const TOUCH_EVENT_UP = 2
const TOUCH_EVENT_CANCEL = 3

const POINTER_MOUSE_BUTTONS = [
    { mask: 1, button: StreamMouseButton.LEFT },
    { mask: 2, button: StreamMouseButton.RIGHT },
    { mask: 4, button: StreamMouseButton.MIDDLE },
    { mask: 8, button: StreamMouseButton.X1 },
    { mask: 16, button: StreamMouseButton.X2 },
]

type TouchInputSample = {
    identifier: number
    clientX: number
    clientY: number
    force: number
    radiusX: number
    radiusY: number
    rotationAngle: number
    timeStamp: number
}

type TrackedTouch = {
    startTime: number
    originX: number
    originY: number
    x: number
    y: number
    force: number
    radiusX: number
    radiusY: number
    rotationAngle: number
    // number is StreamMouseButton
    mouseClicked: null | number
    // point and drag: if we've moved the mouse to the touch
    // mouse relative: we've moved the mouse enough that it shouldn't be a click anymore
    mouseMoved: boolean
}

function trySendChannel(channel: DataTransportChannel | null, buffer: ByteBuffer) {
    if (!channel) {
        console.info(`dropping packet on channel ${channel} because the channel is not present.`)
        return
    }

    buffer.flip()
    const readBuffer = buffer.getRemainingBuffer()
    if (readBuffer.length == 0) {
        throw "illegal buffer size"
    }
    channel.send(readBuffer.buffer)
}

function* encodeTextChunks(text: string): Generator<Uint8Array> {
    const encoded = new TextEncoder().encode(text)
    if (encoded.length == 0) {
        yield encoded
        return
    }

    let start = 0
    while (start < encoded.length) {
        let end = Math.min(start + MAX_TEXT_PACKET_BYTES, encoded.length)

        // Each packet is decoded independently on the streamer, so keep UTF-8
        // code points intact when the byte limit falls in the middle of one.
        if (end < encoded.length) {
            while (end > start && (encoded[end] & 0xc0) == 0x80) {
                end--
            }
        }

        yield encoded.subarray(start, end)
        start = end
    }
}

export type MouseScrollMode = "highres" | "normal"
export type MouseMode = "relative" | "follow" | "localCursor" | "pointAndDrag"
export type TouchMode = "touch" | "mouseRelative" | "localCursor" | "pointAndDrag"

export type StreamInputConfig = {
    mouseMode: MouseMode
    mouseScrollMode: MouseScrollMode
    touchMode: TouchMode
    localCursorSensitivity: number
    controllerConfig: ControllerConfig
}

export function defaultStreamInputConfig(): StreamInputConfig {
    return {
        mouseMode: "follow",
        mouseScrollMode: "highres",
        touchMode: "mouseRelative",
        localCursorSensitivity: 1,
        controllerConfig: {
            invertAB: false,
            invertXY: false,
            sendIntervalOverride: null
        }
    }
}

export type PredictedTouchAction = "default" | "drag" | "scroll" | "screenKeyboard" | "longPress"
export type ScreenKeyboardSetVisibleEvent = CustomEvent<{ visible: boolean }>
export type LocalCursorState = { visible: boolean, x: number, y: number }

export class StreamInput {

    private eventTarget = new EventTarget()

    private buffer: ByteBuffer = new ByteBuffer(1024)

    private connected = false
    private config: StreamInputConfig
    private capabilities: StreamCapabilities = { touch: true }
    // Size of the streamer device
    private streamerSize: [number, number] = [0, 0]

    private keyboard: DataTransportChannel | null = null
    private mouseReliable: DataTransportChannel | null = null
    private mouseAbsolute: DataTransportChannel | null = null
    private mouseRelative: DataTransportChannel | null = null
    private touch: DataTransportChannel | null = null
    private controllers: DataTransportChannel | null = null
    private controllerInputs: Array<DataTransportChannel | null> = []

    private readonly touchDataListener = (data: ArrayBuffer) => this.onTouchData(data)
    private readonly controllerDataListener = (data: ArrayBuffer) => this.onControllerData(data)
    private readonly gamepadRumbleIntervalListener = () => this.onGamepadRumbleInterval()

    private touchSupported: boolean | null = null
    private localCursorPosition: [number, number] | null = null

    constructor(config?: StreamInputConfig) {
        this.config = defaultStreamInputConfig()
        if (config) {
            this.setConfig(config)
        }
    }

    private getDataChannel(transport: Transport, id: TransportChannelIdValue): DataTransportChannel {
        const channel = transport.getChannel(id)
        if (channel.type == "data") {
            return channel
        }
        throw `Failed to get channel ${id} as data transport channel`
    }
    setTransport(transport: Transport) {
        // Pending motion belongs to the old channel and must not leak across a
        // transport replacement.
        this.flushRelativeMouseMotion()

        this.keyboard = this.getDataChannel(transport, TransportChannelId.KEYBOARD)

        this.mouseReliable = this.getDataChannel(transport, TransportChannelId.MOUSE_RELIABLE)
        this.mouseAbsolute = this.getDataChannel(transport, TransportChannelId.MOUSE_ABSOLUTE)
        this.mouseRelative = this.getDataChannel(transport, TransportChannelId.MOUSE_RELATIVE)

        if (this.touch) {
            this.touch.removeReceiveListener(this.touchDataListener)
        }
        this.touch = this.getDataChannel(transport, TransportChannelId.TOUCH)
        this.touch.addReceiveListener(this.touchDataListener)

        if (this.controllers) {
            this.controllers.removeReceiveListener(this.controllerDataListener)
        }
        this.controllers = this.getDataChannel(transport, TransportChannelId.CONTROLLERS)
        this.controllers.addReceiveListener(this.controllerDataListener)

        this.controllerInputs.length = 0
        for (let i = 0; i < 16; i++) {
            const channelId = TransportChannelId[`CONTROLLER${i}` as TransportChannelIdKey]

            this.controllerInputs[i] = this.getDataChannel(transport, channelId)
        }

        // Register all controllers, because the missing channel could've dropped controller connect events
        this.registerBufferedControllers()
    }

    setConfig(config: StreamInputConfig, rect?: DOMRect): boolean {
        const pointingModeChanged = config.mouseMode != this.config.mouseMode || config.touchMode != this.config.touchMode
        if (pointingModeChanged) {
            this.releaseActivePointingInputs(rect ?? new DOMRect())
        }

        Object.assign(this.config, config)
        return pointingModeChanged
    }
    getConfig(): StreamInputConfig {
        return this.config
    }

    getCapabilities(): StreamCapabilities {
        return this.capabilities
    }

    // -- External Event Listeners
    addScreenKeyboardVisibleEvent(listener: (event: ScreenKeyboardSetVisibleEvent) => void) {
        this.eventTarget.addEventListener("ml-screenkeyboardvisible", listener as any)
    }

    // -- On Stream Start
    onStreamStart(capabilities: StreamCapabilities, streamerSize: [number, number]) {
        this.connected = true

        this.capabilities = capabilities
        this.streamerSize = streamerSize
        this.registerBufferedControllers()
    }

    // -- Keyboard
    private pressedKeys: Set<number> = new Set()

    onKeyDown(event: KeyboardEvent) {
        this.sendKeyEvent(true, event)
    }
    onKeyUp(event: KeyboardEvent) {
        this.sendKeyEvent(false, event)
    }

    onPaste(event: ClipboardEvent) {

        const data = event.clipboardData
        if (!data) {
            return
        }

        console.debug("PASTE", data)

        const text = data.getData("text/plain")
        if (text) {
            console.debug("PASTE TEXT", text)

            // Before sending text raise all keys
            this.raiseAllKeys()

            this.sendText(text)
        }
    }

    private sendKeyEvent(isDown: boolean, event: KeyboardEvent) {
        const key = convertToKey(event)
        if (key == null) {
            return
        }

        if (isDown) {
            if (this.pressedKeys.has(key)) {
                return
            }

            this.pressedKeys.add(key)
        } else {
            if (!this.pressedKeys.has(key)) {
                return
            }

            this.pressedKeys.delete(key)
        }

        const modifiers = convertToModifiers(event)

        if ("debug" in console) {
            console.debug(
                isDown ? "DOWN" : "UP",
                event.code,
                convertToKey(event),
                convertToModifiers(event).toString(16)
            )
        }
        this.sendKey(isDown, key, modifiers)
    }

    raiseAllKeys() {
        for (const key of this.pressedKeys) {
            this.sendKey(false, key, 0)
        }
        this.pressedKeys.clear()
    }

    // Note: key = StreamKeys.VK_, modifiers = StreamKeyModifiers.
    sendKey(isDown: boolean, key: number, modifiers: number) {
        this.buffer.reset()

        this.buffer.putU8(0)

        this.buffer.putBool(isDown)
        this.buffer.putU8(modifiers)
        this.buffer.putU16(key)

        trySendChannel(this.keyboard, this.buffer)
    }
    sendText(text: string) {
        for (const chunk of encodeTextChunks(text)) {
            this.buffer.reset()

            this.buffer.putU8(1)
            this.buffer.putU8(chunk.length)
            this.buffer.putU8Array(chunk)

            trySendChannel(this.keyboard, this.buffer)
        }
    }

    // -- Mouse
    private pressedMouseButtons: Set<number> = new Set()
    private pointerMouseButtons: Map<number, Set<number>> = new Map()
    private relativeMouseRemainderX = 0
    private relativeMouseRemainderY = 0
    private pendingRelativeMouseX = 0
    private pendingRelativeMouseY = 0
    private relativeMouseFlushTimer: number | null = null
    private relativeMouseFlushGeneration = 0

    onMouseDown(event: MouseEvent, rect: DOMRect) {
        const button = convertToButton(event)
        if (button == null) {
            return
        }

        this.onMouseButtonDown(button, event.clientX, event.clientY, rect)
    }

    private onMouseButtonDown(button: number, clientX: number, clientY: number, rect: DOMRect) {
        if (this.pressedMouseButtons.has(button)) {
            return
        }

        if (this.config.mouseMode == "relative" || this.config.mouseMode == "follow") {
            this.sendMouseButton(true, button)
        } else if (this.config.mouseMode == "localCursor") {
            this.initializeLocalCursor(rect, clientX, clientY)
            this.sendLocalCursorPosition(true)
            this.sendMouseButton(true, button)
        } else if (this.config.mouseMode == "pointAndDrag") {
            this.sendMousePositionClientCoordinates(clientX, clientY, rect, true, button)
        }
    }
    onMouseUp(event: MouseEvent) {
        const button = convertToButton(event)
        if (button == null) {
            return
        }

        this.flushRelativeMouseMotion()
        this.onMouseButtonUp(button)
    }

    private onMouseButtonUp(button: number) {
        if (this.pressedMouseButtons.has(button)) {
            this.sendMouseButton(false, button)
        }
    }

    onPointerDown(event: PointerEvent, rect: DOMRect) {
        if (event.pointerType == "touch") {
            this.onTouchSamplesStart([this.touchSampleFromPointerEvent(event)], rect)
        } else {
            this.syncPointerMouseButtons(event.pointerId, event.buttons, event.clientX, event.clientY, rect)
        }
    }

    onPointerUp(event: PointerEvent, rect: DOMRect) {
        this.flushRelativeMouseMotion()
        if (event.pointerType == "touch") {
            this.onTouchSamplesEnd([this.touchSampleFromPointerEvent(event)], rect)
        } else {
            this.syncPointerMouseButtons(event.pointerId, event.buttons, event.clientX, event.clientY, rect)
        }
    }

    onPointerCancel(event: PointerEvent, rect: DOMRect) {
        this.flushRelativeMouseMotion()
        if (event.pointerType == "touch") {
            this.onTouchSamplesCancel([this.touchSampleFromPointerEvent(event)], rect)
        } else {
            this.releasePointerMouseButtons(event.pointerId)
        }
    }

    onPointerMove(event: PointerEvent, rect: DOMRect): number {
        const samples = this.getCoalescedPointerSamples(event)
        if (event.pointerType == "touch") {
            for (const sample of samples) {
                this.onTouchSamplesMove([this.touchSampleFromPointerEvent(sample)], rect)
            }
            return samples.length
        }

        const mouseMode = this.config.mouseMode
        let movementX = 0
        let movementY = 0
        for (const sample of samples) {
            this.syncPointerMouseButtons(sample.pointerId, sample.buttons, sample.clientX, sample.clientY, rect)
            if (mouseMode == "relative" || mouseMode == "localCursor" || (mouseMode == "pointAndDrag" && sample.buttons != 0)) {
                movementX += Number.isFinite(sample.movementX) ? sample.movementX : 0
                movementY += Number.isFinite(sample.movementY) ? sample.movementY : 0
            }
        }

        const firstSample = samples[0]
        const lastSample = samples[samples.length - 1]
        if (mouseMode == "relative") {
            this.sendMouseMoveClientCoordinates(movementX, movementY, rect)
        } else if (mouseMode == "follow") {
            this.sendMousePositionClientCoordinates(lastSample.clientX, lastSample.clientY, rect, false)
        } else if (mouseMode == "localCursor") {
            this.initializeLocalCursor(rect, firstSample.clientX, firstSample.clientY)
            this.moveLocalCursorClientCoordinates(movementX, movementY, rect, false)
        } else if (mouseMode == "pointAndDrag" && (movementX != 0 || movementY != 0)) {
            this.sendMouseMoveClientCoordinates(movementX, movementY, rect)
        }
        return samples.length
    }

    private getCoalescedPointerSamples(event: PointerEvent): PointerEvent[] {
        let coalescedEvents: PointerEvent[] = []
        try {
            coalescedEvents = event.getCoalescedEvents()
        } catch {
            // Some browsers expose getCoalescedEvents() but disable it outside a secure context.
        }

        // The Pointer Events contract already returns coalesced samples in
        // chronological order. Avoid sorting/allocating on this hot path.
        return coalescedEvents.length > 0 ? coalescedEvents : [event]
    }

    private syncPointerMouseButtons(pointerId: number, buttons: number, clientX: number, clientY: number, rect: DOMRect) {
        let pointerButtons = this.pointerMouseButtons.get(pointerId)
        if (!pointerButtons) {
            pointerButtons = new Set()
            this.pointerMouseButtons.set(pointerId, pointerButtons)
        }

        for (const mapping of POINTER_MOUSE_BUTTONS) {
            const isDown = (buttons & mapping.mask) != 0
            const wasDown = pointerButtons.has(mapping.button)
            if (isDown == wasDown) {
                continue
            }

            if (isDown) {
                const alreadyOwned = this.isPointerButtonOwned(mapping.button)
                pointerButtons.add(mapping.button)
                if (!alreadyOwned) {
                    this.onMouseButtonDown(mapping.button, clientX, clientY, rect)
                }
            } else {
                pointerButtons.delete(mapping.button)
                if (!this.isPointerButtonOwned(mapping.button)) {
                    this.onMouseButtonUp(mapping.button)
                }
            }
        }

        if (pointerButtons.size == 0) {
            this.pointerMouseButtons.delete(pointerId)
        }
    }

    private isPointerButtonOwned(button: number): boolean {
        for (const buttons of this.pointerMouseButtons.values()) {
            if (buttons.has(button)) {
                return true
            }
        }
        return false
    }

    private releasePointerMouseButtons(pointerId?: number) {
        if (pointerId != null) {
            const buttons = this.pointerMouseButtons.get(pointerId)
            this.pointerMouseButtons.delete(pointerId)
            for (const button of buttons ?? []) {
                if (!this.isPointerButtonOwned(button)) {
                    this.onMouseButtonUp(button)
                }
            }
            return
        }

        const buttons = new Set<number>()
        for (const pointerButtons of this.pointerMouseButtons.values()) {
            for (const button of pointerButtons) {
                buttons.add(button)
            }
        }
        this.pointerMouseButtons.clear()
        for (const button of buttons) {
            this.onMouseButtonUp(button)
        }
    }

    onMouseMove(event: MouseEvent, rect: DOMRect) {
        if (this.config.mouseMode == "relative") {
            this.sendMouseMoveClientCoordinates(event.movementX, event.movementY, rect)
        } else if (this.config.mouseMode == "follow") {
            this.sendMousePositionClientCoordinates(event.clientX, event.clientY, rect, false)
        } else if (this.config.mouseMode == "localCursor") {
            this.initializeLocalCursor(rect, event.clientX, event.clientY)
            this.moveLocalCursorClientCoordinates(event.movementX, event.movementY, rect, false)
        } else if (this.config.mouseMode == "pointAndDrag") {
            if (event.buttons) {
                // some button pressed
                this.sendMouseMoveClientCoordinates(event.movementX, event.movementY, rect)
            }
        }
    }
    onMouseWheel(event: WheelEvent) {
        this.sendAccumulatedScroll(event.deltaX, -event.deltaY)
    }

    sendMouseMove(movementX: number, movementY: number) {
        if (!Number.isFinite(movementX) || !Number.isFinite(movementY)) {
            return
        }

        const integerMovementX = Math.trunc(movementX)
        const integerMovementY = Math.trunc(movementY)
        if (integerMovementX == 0 && integerMovementY == 0) {
            return
        }

        if (this.relativeMouseFlushTimer == null) {
            this.sendMouseMoveImmediately(integerMovementX, integerMovementY)
            const generation = ++this.relativeMouseFlushGeneration
            const timer = window.setTimeout(() => {
                if (this.relativeMouseFlushTimer !== timer || generation != this.relativeMouseFlushGeneration) {
                    return
                }
                this.relativeMouseFlushTimer = null
                this.flushRelativeMouseMotion()
            }, RELATIVE_MOUSE_FLUSH_INTERVAL_MS)
            this.relativeMouseFlushTimer = timer
            return
        }

        this.pendingRelativeMouseX += integerMovementX
        this.pendingRelativeMouseY += integerMovementY
    }

    private sendMouseMoveImmediately(movementX: number, movementY: number) {
        // Keep a malformed synthetic event from turning this synchronous hot
        // path into an unbounded serialization loop.
        let remainingX = Math.max(I16_MIN * MAX_RELATIVE_MOUSE_CHUNKS, Math.min(I16_MAX * MAX_RELATIVE_MOUSE_CHUNKS, movementX))
        let remainingY = Math.max(I16_MIN * MAX_RELATIVE_MOUSE_CHUNKS, Math.min(I16_MAX * MAX_RELATIVE_MOUSE_CHUNKS, movementY))
        while (remainingX != 0 || remainingY != 0) {
            const packetX = Math.max(I16_MIN, Math.min(I16_MAX, remainingX))
            const packetY = Math.max(I16_MIN, Math.min(I16_MAX, remainingY))

            this.buffer.reset()
            this.buffer.putU8(0)
            this.buffer.putI16(packetX)
            this.buffer.putI16(packetY)

            trySendChannel(this.mouseRelative, this.buffer)
            remainingX -= packetX
            remainingY -= packetY
        }
    }

    private flushRelativeMouseMotion() {
        this.relativeMouseFlushGeneration++
        if (this.relativeMouseFlushTimer != null) {
            window.clearTimeout(this.relativeMouseFlushTimer)
            this.relativeMouseFlushTimer = null
        }

        const movementX = this.pendingRelativeMouseX
        const movementY = this.pendingRelativeMouseY
        this.pendingRelativeMouseX = 0
        this.pendingRelativeMouseY = 0
        if (movementX != 0 || movementY != 0) {
            this.sendMouseMoveImmediately(movementX, movementY)
        }
    }
    sendMouseMoveClientCoordinates(movementX: number, movementY: number, rect: DOMRect) {
        if (
            !Number.isFinite(movementX) || !Number.isFinite(movementY) ||
            !this.isUsableRect(rect) || !this.hasUsableStreamerSize()
        ) {
            return
        }

        this.relativeMouseRemainderX += movementX / rect.width * this.streamerSize[0]
        this.relativeMouseRemainderY += movementY / rect.height * this.streamerSize[1]

        const integerMovementX = Math.trunc(this.relativeMouseRemainderX)
        const integerMovementY = Math.trunc(this.relativeMouseRemainderY)
        this.relativeMouseRemainderX -= integerMovementX
        this.relativeMouseRemainderY -= integerMovementY

        this.sendMouseMove(integerMovementX, integerMovementY)
    }
    sendMousePosition(x: number, y: number, referenceWidth: number, referenceHeight: number, reliable: boolean) {
        if (
            !Number.isFinite(x) || !Number.isFinite(y) ||
            !Number.isFinite(referenceWidth) || !Number.isFinite(referenceHeight) ||
            referenceWidth <= 0 || referenceHeight <= 0
        ) {
            return
        }

        this.buffer.reset()

        this.buffer.putU8(1)
        this.buffer.putI16(x)
        this.buffer.putI16(y)
        this.buffer.putI16(referenceWidth)
        this.buffer.putI16(referenceHeight)

        if (reliable) {
            trySendChannel(this.mouseReliable, this.buffer)
        } else {
            trySendChannel(this.mouseAbsolute, this.buffer)
        }
    }
    sendMousePositionClientCoordinates(clientX: number, clientY: number, rect: DOMRect, reliable: boolean, mouseButton?: number) {
        const position = this.calcNormalizedPosition(clientX, clientY, rect)
        if (position) {
            const [x, y] = position
            this.sendMousePosition(x * 4096.0, y * 4096.0, 4096.0, 4096.0, reliable)

            if (mouseButton != undefined) {
                this.sendMouseButton(true, mouseButton)
            }
        }
    }
    private initializeLocalCursor(rect: DOMRect, clientX?: number, clientY?: number) {
        if (this.localCursorPosition) {
            return
        }

        if (clientX != null && clientY != null) {
            const position = this.calcNormalizedPosition(clientX, clientY, rect)
            if (position) {
                this.localCursorPosition = [
                    position[0] * this.streamerSize[0],
                    position[1] * this.streamerSize[1],
                ]
                return
            }
        }

        this.localCursorPosition = [
            this.streamerSize[0] / 2,
            this.streamerSize[1] / 2,
        ]
    }
    private clampLocalCursorPosition() {
        if (!this.localCursorPosition) {
            return
        }

        this.localCursorPosition[0] = Math.min(Math.max(this.localCursorPosition[0], 0), this.streamerSize[0])
        this.localCursorPosition[1] = Math.min(Math.max(this.localCursorPosition[1], 0), this.streamerSize[1])
    }
    private sendLocalCursorPosition(reliable: boolean) {
        if (!this.localCursorPosition) {
            return
        }

        this.sendMousePosition(
            this.localCursorPosition[0],
            this.localCursorPosition[1],
            this.streamerSize[0],
            this.streamerSize[1],
            reliable
        )
    }
    private moveLocalCursorClientCoordinates(movementX: number, movementY: number, rect: DOMRect, reliable: boolean) {
        if (
            !Number.isFinite(movementX) || !Number.isFinite(movementY) ||
            !this.hasUsableStreamerSize() || !this.isUsableRect(rect)
        ) {
            return
        }

        this.initializeLocalCursor(rect)
        if (!this.localCursorPosition) {
            return
        }

        this.localCursorPosition[0] += movementX / rect.width * this.streamerSize[0] * this.config.localCursorSensitivity
        this.localCursorPosition[1] += movementY / rect.height * this.streamerSize[1] * this.config.localCursorSensitivity
        this.clampLocalCursorPosition()
        this.sendLocalCursorPosition(reliable)
    }
    // Note: button = StreamMouseButton.
    sendMouseButton(isDown: boolean, button: number) {
        // Keep pending motion on the leading side of the logical button
        // barrier, including button transitions synthesized by pointermove.
        this.flushRelativeMouseMotion()
        if (isDown) {
            this.pressedMouseButtons.add(button)
        } else {
            this.pressedMouseButtons.delete(button)
        }

        this.buffer.reset()

        this.buffer.putU8(2)
        this.buffer.putBool(isDown)
        this.buffer.putU8(button)

        trySendChannel(this.mouseReliable, this.buffer)
    }
    sendMouseWheelHighRes(deltaX: number, deltaY: number) {
        this.flushRelativeMouseMotion()
        this.buffer.reset()

        this.buffer.putU8(3)
        this.buffer.putI16(deltaX)
        this.buffer.putI16(deltaY)

        trySendChannel(this.mouseRelative, this.buffer)
    }
    sendMouseWheel(deltaX: number, deltaY: number) {
        this.flushRelativeMouseMotion()
        this.buffer.reset()

        this.buffer.putU8(4)
        this.buffer.putI8(deltaX)
        this.buffer.putI8(deltaY)

        trySendChannel(this.mouseRelative, this.buffer)
    }

    private scrollRemainderX = 0
    private scrollRemainderY = 0

    private resetScrollRemainder() {
        this.scrollRemainderX = 0
        this.scrollRemainderY = 0
    }
    private sendAccumulatedScroll(deltaX: number, deltaY: number) {
        this.scrollRemainderX += deltaX
        this.scrollRemainderY += deltaY

        const integerX = Math.trunc(this.scrollRemainderX)
        const integerY = Math.trunc(this.scrollRemainderY)

        if (integerX == 0 && integerY == 0) {
            return
        }

        this.scrollRemainderX -= integerX
        this.scrollRemainderY -= integerY

        if (this.config.mouseScrollMode == "highres") {
            this.sendMouseWheelHighRes(integerX, integerY)
        } else if (this.config.mouseScrollMode == "normal") {
            this.sendMouseWheel(integerX, integerY)
        }
    }

    // -- Touch
    private touchTracker: Map<number, TrackedTouch> = new Map()
    // The current action of all touches on screen
    // - default -> the default action for this touch mode / we're still trying to figure out what the user is trying to do
    // - drag -> movement continues without click handling on release
    // - scroll -> we're currently scrolling using primary touch
    // - screenKeyboard -> this current action is trying to pull up the on screen keyboard
    // - longPress -> single-finger long press is armed and will become a right click on release
    private touchMouseAction: PredictedTouchAction = "default"
    // The touch that is selected as the primary / controller of the action
    // Used in touch mode "relative" and "pointAndDrag"
    // E.g. scrolling movement
    private primaryTouch: number | null = null
    // If the next touch is a double tap?
    private nextTouchDoubleTap: boolean = false
    // Set when the current gesture has already been consumed by a multi-touch
    // action. This prevents the remaining finger from becoming a click/right-click.
    private touchGestureSuppressClick: boolean = false
    private pendingSingleTapTimeout: number | null = null

    private onTouchData(data: ArrayBuffer) {
        const buffer = new ByteBuffer(new Uint8Array(data))
        this.touchSupported = buffer.getBool()
    }
    getLocalCursorState(): LocalCursorState {
        if (
            (this.config.touchMode != "localCursor" && this.config.mouseMode != "localCursor") ||
            !this.localCursorPosition ||
            this.streamerSize[0] <= 0 ||
            this.streamerSize[1] <= 0
        ) {
            return { visible: false, x: 0, y: 0 }
        }

        return {
            visible: true,
            x: this.localCursorPosition[0] / this.streamerSize[0],
            y: this.localCursorPosition[1] / this.streamerSize[1],
        }
    }

    private touchSamplesFromTouchEvent(event: TouchEvent): TouchInputSample[] {
        return Array.from(event.changedTouches, touch => ({
            identifier: touch.identifier,
            clientX: touch.clientX,
            clientY: touch.clientY,
            force: touch.force,
            radiusX: touch.radiusX,
            radiusY: touch.radiusY,
            rotationAngle: touch.rotationAngle,
            timeStamp: event.timeStamp,
        }))
    }

    private touchSampleFromPointerEvent(event: PointerEvent): TouchInputSample {
        return {
            identifier: event.pointerId,
            clientX: event.clientX,
            clientY: event.clientY,
            force: event.pressure,
            radiusX: event.width / 2,
            radiusY: event.height / 2,
            rotationAngle: event.twist,
            timeStamp: event.timeStamp,
        }
    }

    private updateTouchTracker(touch: TouchInputSample) {
        const oldTouch = this.touchTracker.get(touch.identifier)
        if (!oldTouch) {
            this.touchTracker.set(touch.identifier, {
                startTime: performance.now(),
                originX: touch.clientX,
                originY: touch.clientY,
                x: touch.clientX,
                y: touch.clientY,
                force: touch.force,
                radiusX: touch.radiusX,
                radiusY: touch.radiusY,
                rotationAngle: touch.rotationAngle,
                mouseClicked: null,
                mouseMoved: false,
            })
        } else {
            oldTouch.x = touch.clientX
            oldTouch.y = touch.clientY
            oldTouch.force = touch.force
            oldTouch.radiusX = touch.radiusX
            oldTouch.radiusY = touch.radiusY
            oldTouch.rotationAngle = touch.rotationAngle
        }
    }

    private calcTouchTime(touch: { startTime: number }): number {
        return performance.now() - touch.startTime
    }
    private calcTouchOriginDistance(
        touch: { x: number, y: number } | { clientX: number, clientY: number },
        oldTouch: { originX: number, originY: number }
    ): number {
        if ("clientX" in touch) {
            return Math.hypot(touch.clientX - oldTouch.originX, touch.clientY - oldTouch.originY)
        } else {
            return Math.hypot(touch.x - oldTouch.originX, touch.y - oldTouch.originY)
        }
    }
    private shouldStartTwoTouchScroll(activeTouch?: TouchInputSample): boolean {
        if (this.touchTracker.size != 2) {
            return false
        }

        for (const [id, trackedTouch] of this.touchTracker.entries()) {
            const touchForDistance = activeTouch && activeTouch.identifier == id
                ? activeTouch
                : trackedTouch

            if (this.calcTouchOriginDistance(touchForDistance, trackedTouch) > TWO_TOUCH_SCROLL_TRIGGER_DISTANCE) {
                return true
            }
        }

        return false
    }

    onTouchStart(event: TouchEvent, rect: DOMRect) {
        this.onTouchSamplesStart(this.touchSamplesFromTouchEvent(event), rect)
    }

    private onTouchSamplesStart(touches: TouchInputSample[], rect: DOMRect) {
        if (this.config.touchMode == "touch") {
            touches = touches.filter(touch => this.calcNormalizedPosition(touch.clientX, touch.clientY, rect) != null)
        }

        if (this.touchTracker.size == 0) {
            this.touchGestureSuppressClick = false
        }

        for (const touch of touches) {
            this.updateTouchTracker(touch)
        }

        if (this.config.touchMode == "touch") {
            for (const touch of touches) {
                this.sendTouch(TOUCH_EVENT_DOWN, touch, rect)
            }
        } else if (this.config.touchMode == "mouseRelative" || this.config.touchMode == "localCursor" || this.config.touchMode == "pointAndDrag") {
            // Set primary touch if it doesn't exists currently
            for (const touch of touches) {
                if (this.primaryTouch == null) {
                    this.primaryTouch = touch.identifier
                    this.touchMouseAction = "default"

                    if (this.config.touchMode == "localCursor") {
                        this.initializeLocalCursor(rect, touch.clientX, touch.clientY)
                    }
                }
            }

            const primaryTouch = this.primaryTouch != null && this.touchTracker.get(this.primaryTouch)

            // Detect dragging in mouse relative
            if ((this.config.touchMode == "mouseRelative" || this.config.touchMode == "localCursor") && primaryTouch && this.nextTouchDoubleTap) {
                this.clearPendingSingleTapTimeout()
                if (primaryTouch.mouseClicked == null) {
                    this.sendMouseButton(true, StreamMouseButton.LEFT)
                    primaryTouch.mouseClicked = StreamMouseButton.LEFT
                }

                this.touchMouseAction = "drag"

                this.nextTouchDoubleTap = false
            }

            // Detect scrolling
            if (this.touchTracker.size == 3) {
                this.touchMouseAction = "screenKeyboard"
                this.touchGestureSuppressClick = true
            }
        }
    }

    onTouchUpdate(rect: DOMRect) {
        if (this.primaryTouch == null) {
            return
        }
        const touch = this.touchTracker.get(this.primaryTouch)
        if (!touch) {
            return
        }

        const time = this.calcTouchTime(touch)
        if (this.config.touchMode == "pointAndDrag") {
            if (this.touchMouseAction == "default" && !touch.mouseMoved && time >= TOUCH_AS_CLICK_MIN_TIME_MS) {
                this.sendMousePositionClientCoordinates(touch.originX, touch.originY, rect, true)

                touch.mouseMoved = true
            }
        } else if ((this.config.touchMode == "mouseRelative" || this.config.touchMode == "localCursor") &&
            this.touchTracker.size == 1 &&
            this.touchMouseAction == "default" &&
            !touch.mouseMoved &&
            touch.mouseClicked == null &&
            time >= TOUCH_AS_CLICK_MAX_TIME_MS) {
            this.touchMouseAction = "longPress"
            this.nextTouchDoubleTap = false
        }
    }

    hasActiveTouches(): boolean {
        return this.touchTracker.size > 0
    }

    onTouchMove(event: TouchEvent, rect: DOMRect) {
        this.onTouchSamplesMove(this.touchSamplesFromTouchEvent(event), rect)
    }

    private onTouchSamplesMove(touches: TouchInputSample[], rect: DOMRect) {
        if (this.config.touchMode == "touch") {
            for (const touch of touches) {
                if (this.touchTracker.has(touch.identifier)) {
                    this.sendTouch(TOUCH_EVENT_MOVE, touch, rect)
                }
            }
        } else if (this.config.touchMode == "mouseRelative" || this.config.touchMode == "localCursor" || this.config.touchMode == "pointAndDrag") {
            for (const touch of touches) {
                if (this.primaryTouch != touch.identifier) {
                    continue
                }
                const oldTouch = this.touchTracker.get(this.primaryTouch)
                if (!oldTouch) {
                    continue
                }

                const movementX = touch.clientX - oldTouch.x;
                const movementY = touch.clientY - oldTouch.y;

                if (this.touchMouseAction == "default") {
                    if (this.shouldStartTwoTouchScroll(touch)) {
                        this.touchMouseAction = "scroll"
                        this.touchGestureSuppressClick = true
                        this.resetScrollRemainder()
                        for (const trackedTouch of this.touchTracker.values()) {
                            trackedTouch.mouseMoved = true
                        }

                        if (oldTouch.mouseClicked != null) {
                            this.sendMouseButton(false, oldTouch.mouseClicked)
                            oldTouch.mouseClicked = null
                        }

                        if (this.config.touchMode == "pointAndDrag" && this.primaryTouch != null) {
                            const primaryTouch = this.touchTracker.get(this.primaryTouch)
                            if (primaryTouch) {
                                let middleX = 0;
                                let middleY = 0;
                                for (const trackedTouch of this.touchTracker.values()) {
                                    middleX += trackedTouch.x
                                    middleY += trackedTouch.y
                                }
                                middleX += touch.clientX - oldTouch.x
                                middleY += touch.clientY - oldTouch.y
                                middleX /= 2
                                middleY /= 2

                                primaryTouch.mouseMoved = true
                                this.sendMousePositionClientCoordinates(middleX, middleY, rect, true)
                            }
                        }
                    }
                }

                if (this.touchMouseAction == "default") {
                    const touchOriginDistance = this.calcTouchOriginDistance(touch, oldTouch)

                    // Normal mouse relative movement
                    if (this.config.touchMode == "mouseRelative") {
                        this.sendMouseMoveClientCoordinates(movementX, movementY, rect)

                        if (touchOriginDistance > TOUCH_AS_CLICK_MAX_DISTANCE) {
                            oldTouch.mouseMoved = true
                            this.touchGestureSuppressClick = true
                        }
                    } else if (this.config.touchMode == "localCursor") {
                        this.moveLocalCursorClientCoordinates(movementX, movementY, rect, false)

                        if (touchOriginDistance > TOUCH_AS_CLICK_MAX_DISTANCE) {
                            oldTouch.mouseMoved = true
                            this.touchGestureSuppressClick = true
                        }
                    }
                    // Point and Drag
                    // If we are over the touch as click distance go to the origin and drag
                    else if (this.config.touchMode == "pointAndDrag" && touchOriginDistance > TOUCH_AS_CLICK_MAX_DISTANCE) {
                        if (!oldTouch.mouseMoved) {
                            this.sendMousePositionClientCoordinates(oldTouch.originX, oldTouch.originY, rect, true)
                            oldTouch.mouseMoved = true
                        }

                        if (oldTouch.mouseClicked == null) {
                            this.sendMouseButton(true, StreamMouseButton.LEFT)
                            oldTouch.mouseClicked = StreamMouseButton.LEFT
                        }

                        this.touchMouseAction = "drag"
                    }
                } else if (this.touchMouseAction == "longPress") {
                    if (movementX != 0 || movementY != 0) {
                        this.touchMouseAction = "drag"
                        this.touchGestureSuppressClick = true
                        oldTouch.mouseMoved = true
                        oldTouch.mouseClicked = StreamMouseButton.LEFT
                        this.sendMouseButton(true, StreamMouseButton.LEFT)

                        if (this.config.touchMode == "localCursor") {
                            this.moveLocalCursorClientCoordinates(movementX, movementY, rect, false)
                        } else {
                            this.sendMouseMoveClientCoordinates(movementX, movementY, rect)
                        }
                    }
                } else if (this.touchMouseAction == "drag") {
                    // Do the dragging
                    if (this.config.touchMode == "localCursor") {
                        this.moveLocalCursorClientCoordinates(movementX, movementY, rect, false)
                    } else {
                        this.sendMouseMoveClientCoordinates(movementX, movementY, rect)
                    }
                } else if (this.touchMouseAction == "scroll") {
                    // inverting horizontal scroll
                    if (this.config.mouseScrollMode == "highres") {
                        this.sendAccumulatedScroll(
                            -movementX * TOUCH_HIGH_RES_SCROLL_MULTIPLIER,
                            movementY * TOUCH_HIGH_RES_SCROLL_MULTIPLIER
                        )
                    } else if (this.config.mouseScrollMode == "normal") {
                        this.sendAccumulatedScroll(
                            -movementX * TOUCH_SCROLL_MULTIPLIER,
                            movementY * TOUCH_SCROLL_MULTIPLIER
                        )
                    }
                } else if (this.touchMouseAction == "screenKeyboard") {
                    // calculate if we should open the screen keyboard
                    const distanceY = touch.clientY - oldTouch.originY

                    if (distanceY < -TOUCHES_AS_KEYBOARD_DISTANCE) {
                        const customEvent: ScreenKeyboardSetVisibleEvent = new CustomEvent("ml-screenkeyboardvisible", {
                            detail: { visible: true }
                        })
                        this.eventTarget.dispatchEvent(customEvent)
                    } else if (distanceY > TOUCHES_AS_KEYBOARD_DISTANCE) {
                        const customEvent: ScreenKeyboardSetVisibleEvent = new CustomEvent("ml-screenkeyboardvisible", {
                            detail: { visible: false }
                        })
                        this.eventTarget.dispatchEvent(customEvent)
                    }
                }
            }
        }

        for (const touch of touches) {
            this.updateTouchTracker(touch)
        }
    }

    onTouchEnd(event: TouchEvent, rect: DOMRect) {
        this.onTouchSamplesEnd(this.touchSamplesFromTouchEvent(event), rect)
    }

    private onTouchSamplesEnd(touches: TouchInputSample[], rect: DOMRect) {
        this.flushRelativeMouseMotion()
        if (this.config.touchMode == "touch") {
            for (const touch of touches) {
                if (this.touchTracker.has(touch.identifier)) {
                    this.sendTouch(TOUCH_EVENT_UP, touch, rect)
                }
            }
        } else if (this.config.touchMode == "mouseRelative" || this.config.touchMode == "localCursor" || this.config.touchMode == "pointAndDrag") {
            const endingScroll = this.touchMouseAction == "scroll" && this.touchTracker.size == 2
            const endingTwoTouchTap = !this.touchGestureSuppressClick && this.touchMouseAction == "default" && this.touchTracker.size == 2
            let endingTwoTouchTapShouldRightClick = false
            let handledTwoTouchTap = false

            if (endingTwoTouchTap) {
                endingTwoTouchTapShouldRightClick = true
                for (const trackedTouch of this.touchTracker.values()) {
                    if (this.calcTouchTime(trackedTouch) > TOUCH_AS_CLICK_MAX_TIME_MS) {
                        endingTwoTouchTapShouldRightClick = false
                        break
                    }
                }
            }

            for (const touch of touches) {
                if (endingTwoTouchTap) {
                    if (!handledTwoTouchTap && endingTwoTouchTapShouldRightClick) {
                        this.sendMouseButton(true, StreamMouseButton.RIGHT)
                        this.sendMouseButton(false, StreamMouseButton.RIGHT)
                    }
                    handledTwoTouchTap = true

                    this.primaryTouch = null
                    this.nextTouchDoubleTap = false
                    continue
                }

                if (this.primaryTouch != touch.identifier) {
                    continue
                }
                const oldTouch = this.touchTracker.get(this.primaryTouch)
                this.primaryTouch = null

                if (oldTouch) {
                    if (endingScroll) {
                        continue
                    }

                    if (this.touchMouseAction == "longPress") {
                        if (this.touchGestureSuppressClick) {
                            this.nextTouchDoubleTap = false
                            continue
                        }
                        this.sendMouseButton(true, StreamMouseButton.RIGHT)
                        this.sendMouseButton(false, StreamMouseButton.RIGHT)
                        this.nextTouchDoubleTap = false
                        continue
                    }

                    if (this.touchMouseAction == "drag") {
                        if (oldTouch.mouseClicked != null) {
                            this.sendMouseButton(false, oldTouch.mouseClicked)
                            oldTouch.mouseClicked = null
                        }
                        this.nextTouchDoubleTap = false
                        continue
                    }

                    const touchTime = this.calcTouchTime(oldTouch)
                    const touchOriginDistance = this.calcTouchOriginDistance(touch, oldTouch)

                    const maybeDoubleTap = !this.touchGestureSuppressClick && touchTime < DOUBLE_TAP_FIRST_TAP_MAX_TIME_MS

                    // point and drag: Before making a click we should move the mouse to the position
                    if (this.config.touchMode == "pointAndDrag" && !oldTouch.mouseMoved) {
                        this.sendMousePositionClientCoordinates(touch.clientX, touch.clientY, rect, true)
                    }

                    const doClick = (maybeDoubleTap: boolean) => {
                        // See if we should make a click
                        if (
                            touchOriginDistance < TOUCH_AS_CLICK_MAX_DISTANCE &&
                            !this.touchGestureSuppressClick &&
                            !oldTouch.mouseMoved &&
                            !maybeDoubleTap
                        ) {
                            // Should we right or left click?
                            let mouseButton
                            if (touchTime > TOUCH_AS_CLICK_MAX_TIME_MS) {
                                mouseButton = StreamMouseButton.RIGHT
                            } else {
                                mouseButton = StreamMouseButton.LEFT
                            }

                            this.sendMouseButton(true, mouseButton)
                            oldTouch.mouseClicked = mouseButton
                        }

                        // Reset mouse click to neutral
                        if (oldTouch.mouseClicked != null) {
                            this.sendMouseButton(false, oldTouch.mouseClicked)
                        }
                    }

                    doClick(maybeDoubleTap)

                    if (maybeDoubleTap) {
                        this.nextTouchDoubleTap = true

                        // Schedule the click if it's not a double tap
                        this.clearPendingSingleTapTimeout()
                        this.pendingSingleTapTimeout = window.setTimeout(() => {
                            this.pendingSingleTapTimeout = null
                            if (this.primaryTouch == null) {
                                // no click present -> no double click -> We need to do the actual click
                                doClick(false)

                                // it cannot be a double tap
                                this.nextTouchDoubleTap = false
                            }
                        }, DOUBLE_TAP_SECOND_TAP_MAX_TIME_MS)
                    } else {
                        this.nextTouchDoubleTap = false
                    }
                }
            }
        }

        for (const touch of touches) {
            this.touchTracker.delete(touch.identifier)
        }

        if (this.touchMouseAction == "scroll" && this.touchTracker.size < 2) {
            this.touchMouseAction = "default"
            this.resetScrollRemainder()
        }

        if (this.touchTracker.size == 0) {
            this.touchGestureSuppressClick = false
        }
    }

    onTouchCancel(event: TouchEvent, rect: DOMRect) {
        this.onTouchSamplesCancel(this.touchSamplesFromTouchEvent(event), rect)
    }

    private onTouchSamplesCancel(touches: TouchInputSample[], rect: DOMRect) {
        this.flushRelativeMouseMotion()
        if (this.config.touchMode == "touch") {
            for (const touch of touches) {
                if (this.touchTracker.has(touch.identifier)) {
                    this.sendTouch(TOUCH_EVENT_CANCEL, touch, rect)
                }
            }
        } else {
            for (const touch of touches) {
                const trackedTouch = this.touchTracker.get(touch.identifier)
                if (trackedTouch && trackedTouch.mouseClicked != null) {
                    this.sendMouseButton(false, trackedTouch.mouseClicked)
                    trackedTouch.mouseClicked = null
                }
            }
        }

        for (const touch of touches) {
            this.touchTracker.delete(touch.identifier)
            if (this.primaryTouch == touch.identifier) {
                this.primaryTouch = null
            }
        }

        if (this.primaryTouch == null && this.touchTracker.size > 0) {
            this.primaryTouch = this.touchTracker.keys().next().value ?? null
            this.touchGestureSuppressClick = true
        }

        this.touchMouseAction = "default"
        this.nextTouchDoubleTap = false
        if (this.touchTracker.size == 0) {
            this.touchGestureSuppressClick = false
        }
        this.clearPendingSingleTapTimeout()
        this.resetScrollRemainder()
    }

    private clearPendingSingleTapTimeout() {
        if (this.pendingSingleTapTimeout != null) {
            window.clearTimeout(this.pendingSingleTapTimeout)
            this.pendingSingleTapTimeout = null
        }
    }

    private releaseActivePointingInputs(rect: DOMRect) {
        this.flushRelativeMouseMotion()
        if (this.touchTracker.size > 0) {
            const now = performance.now()
            const touches = Array.from(this.touchTracker, ([identifier, touch]) => ({
                identifier,
                clientX: touch.x,
                clientY: touch.y,
                force: touch.force,
                radiusX: touch.radiusX,
                radiusY: touch.radiusY,
                rotationAngle: touch.rotationAngle,
                timeStamp: now,
            }))
            this.onTouchSamplesCancel(touches, rect)
        }

        this.pointerMouseButtons.clear()
        for (const button of Array.from(this.pressedMouseButtons)) {
            this.sendMouseButton(false, button)
        }

        this.touchTracker.clear()
        this.primaryTouch = null
        this.touchMouseAction = "default"
        this.nextTouchDoubleTap = false
        this.touchGestureSuppressClick = false
        this.relativeMouseRemainderX = 0
        this.relativeMouseRemainderY = 0
        this.clearPendingSingleTapTimeout()
        this.resetScrollRemainder()
    }

    releaseAllInputs(rect: DOMRect) {
        this.raiseAllKeys()
        this.releaseActivePointingInputs(rect)
    }

    dispose() {
        this.flushRelativeMouseMotion()
        if (this.touch) {
            this.touch.removeReceiveListener(this.touchDataListener)
        }
        if (this.controllers) {
            this.controllers.removeReceiveListener(this.controllerDataListener)
        }
        for (let id = 0; id < this.gamepads.length; id++) {
            if (this.gamepads[id] != null) {
                this.sendControllerRemove(id)
            }
        }
        if (this.gamepadRumbleInterval != null) {
            window.clearInterval(this.gamepadRumbleInterval)
            this.gamepadRumbleInterval = null
        }
        this.gamepads.length = 0
        this.gamepadRumbleCurrent.length = 0
        this.controllerInputs.length = 0
        this.touch = null
        this.controllers = null
    }

    private isUsableRect(rect: DOMRect): boolean {
        return Number.isFinite(rect.left) && Number.isFinite(rect.top) &&
            Number.isFinite(rect.width) && Number.isFinite(rect.height) &&
            rect.width > 0 && rect.height > 0
    }

    private hasUsableStreamerSize(): boolean {
        return Number.isFinite(this.streamerSize[0]) && Number.isFinite(this.streamerSize[1]) &&
            this.streamerSize[0] > 0 && this.streamerSize[1] > 0
    }

    private calcNormalizedPosition(clientX: number, clientY: number, rect: DOMRect): [number, number] | null {
        if (!Number.isFinite(clientX) || !Number.isFinite(clientY) || !this.isUsableRect(rect)) {
            return null
        }

        const x = (clientX - rect.left) / rect.width
        const y = (clientY - rect.top) / rect.height

        if (!Number.isFinite(x) || !Number.isFinite(y) || x < 0 || x > 1.0 || y < 0 || y > 1.0) {
            // invalid touch
            return null
        }
        return [x, y]
    }
    private calcClampedNormalizedPosition(clientX: number, clientY: number, rect: DOMRect): [number, number] | null {
        if (!Number.isFinite(clientX) || !Number.isFinite(clientY) || !this.isUsableRect(rect)) {
            return null
        }

        const x = (clientX - rect.left) / rect.width
        const y = (clientY - rect.top) / rect.height
        if (!Number.isFinite(x) || !Number.isFinite(y)) {
            return null
        }
        return [Math.min(Math.max(x, 0), 1), Math.min(Math.max(y, 0), 1)]
    }
    private sendTouch(type: number, touch: TouchInputSample, rect: DOMRect) {
        if (!Number.isFinite(touch.identifier)) {
            return
        }

        this.buffer.reset()

        this.buffer.putU8(type)

        this.buffer.putU32(touch.identifier)

        const position = type == TOUCH_EVENT_DOWN
            ? this.calcNormalizedPosition(touch.clientX, touch.clientY, rect)
            : this.calcClampedNormalizedPosition(touch.clientX, touch.clientY, rect)
        if (!position) {
            return
        }
        const [x, y] = position
        this.buffer.putF32(x)
        this.buffer.putF32(y)

        this.buffer.putF32(Number.isFinite(touch.force) ? touch.force : 0)

        this.buffer.putF32(Number.isFinite(touch.radiusX) ? touch.radiusX : 0)
        this.buffer.putF32(Number.isFinite(touch.radiusY) ? touch.radiusY : 0)
        this.buffer.putU16(Number.isFinite(touch.rotationAngle) ? touch.rotationAngle : 0)

        trySendChannel(this.touch, this.buffer)
    }

    isTouchSupported(): boolean | null {
        return this.touchSupported
    }

    getCurrentPredictedTouchAction(): PredictedTouchAction {
        return this.touchMouseAction
    }

    // -- Controller
    // Wait for stream to connect and then send controllers
    private bufferedControllers: Array<number> = []
    private registerBufferedControllers() {
        const gamepads = navigator.getGamepads()

        for (const index of this.bufferedControllers.splice(0)) {
            const gamepad = gamepads[index]
            if (gamepad) {
                this.onGamepadConnect(gamepad)
            }
        }
    }

    private collectActuators(gamepad: Gamepad): Array<GamepadHapticActuator> {
        const actuators = []
        if ("vibrationActuator" in gamepad && gamepad.vibrationActuator) {
            actuators.push(gamepad.vibrationActuator)
        }
        if ("hapticActuators" in gamepad && gamepad.hapticActuators) {
            const hapticActuators = gamepad.hapticActuators as Array<GamepadHapticActuator>
            actuators.push(...hapticActuators)
        }
        return actuators
    }

    private gamepads: Array<{ gamepadIndex: number, oldState: GamepadState } | null> = []
    private gamepadRumbleInterval: number | null = null

    onGamepadConnect(gamepad: Gamepad) {
        if (!this.connected || !this.controllers) {
            this.bufferedControllers.push(gamepad.index)
            return
        }

        if (this.gamepads.find(value => value?.gamepadIndex == gamepad.index)) {
            return
        }

        let id = -1
        for (let i = 0; i < this.gamepads.length; i++) {
            if (this.gamepads[i] == null) {
                this.gamepads[i] = { gamepadIndex: gamepad.index, oldState: emptyGamepadState() }
                id = i
                break
            }
        }
        if (id == -1) {
            id = this.gamepads.length
            this.gamepads.push({ gamepadIndex: gamepad.index, oldState: emptyGamepadState() })
        }

        // Start Rumble interval
        if (this.gamepadRumbleInterval == null) {
            this.gamepadRumbleInterval = window.setInterval(this.gamepadRumbleIntervalListener, CONTROLLER_RUMBLE_INTERVAL_MS - 10)
        }

        // Reset rumble
        this.gamepadRumbleCurrent[gamepad.index] = this.emptyGamepadRumbleState()

        let capabilities = 0

        // Rumble capabilities
        for (const actuator of this.collectActuators(gamepad)) {
            if ("effects" in actuator) {
                const supportedEffects = actuator.effects as Array<string>

                for (const effect of supportedEffects) {
                    if (effect == "dual-rumble") {
                        capabilities |= StreamControllerCapabilities.CAPABILITY_RUMBLE
                    } else if (effect == "trigger-rumble") {
                        capabilities |= StreamControllerCapabilities.CAPABILITY_TRIGGER_RUMBLE
                    }
                }
            } else if ("type" in actuator && (actuator.type == "vibration" || actuator.type == "dual-rumble")) {
                capabilities |= StreamControllerCapabilities.CAPABILITY_RUMBLE
            } else if ("playEffect" in actuator && typeof actuator.playEffect == "function") {
                // we're just hoping at this point
                capabilities |= StreamControllerCapabilities.CAPABILITY_RUMBLE | StreamControllerCapabilities.CAPABILITY_TRIGGER_RUMBLE
            } else if ("pulse" in actuator && typeof actuator.pulse == "function") {
                capabilities |= StreamControllerCapabilities.CAPABILITY_RUMBLE
            }
        }

        this.sendControllerAdd(id, SUPPORTED_BUTTONS, capabilities)

        if (gamepad.mapping != "standard") {
            console.warn(`[Gamepad]: Unable to read values of gamepad with mapping ${gamepad.mapping}`)
        }
    }
    onGamepadDisconnect(event: GamepadEvent) {
        const index = this.gamepads.findIndex(value => value?.gamepadIndex == event.gamepad.index)
        if (index != -1) {
            this.sendControllerRemove(index)

            this.gamepads[index] = null
        }

        if (this.gamepads.every(value => value == null) && this.gamepadRumbleInterval != null) {
            window.clearInterval(this.gamepadRumbleInterval)
            this.gamepadRumbleInterval = null
        }
    }

    private lastGamepadUpdate: number = performance.now()
    private lastGamepadStateRefresh: number = performance.now()
    onGamepadUpdate() {
        const now = performance.now()
        if (this.config.controllerConfig.sendIntervalOverride != null) {
            if (now - this.lastGamepadUpdate < (1000 / this.config.controllerConfig.sendIntervalOverride)) {
                return
            }
            this.lastGamepadUpdate = now
        }

        const refreshGamepadStates = now - this.lastGamepadStateRefresh >= CONTROLLER_STATE_REFRESH_INTERVAL_MS
        if (refreshGamepadStates) {
            this.lastGamepadStateRefresh = now
        }

        for (let gamepadId = 0; gamepadId < this.gamepads.length; gamepadId++) {
            const oldGamepadState = this.gamepads[gamepadId]
            if (oldGamepadState == null) {
                continue
            }
            const gamepad = navigator.getGamepads()[oldGamepadState.gamepadIndex]
            if (!gamepad) {
                continue
            }

            if (gamepad.mapping != "standard") {
                continue
            }

            const state = extractGamepadState(gamepad, this.config.controllerConfig)
            if (!refreshGamepadStates && areGamepadStatesEqual(state, oldGamepadState.oldState)) {
                continue
            }
            oldGamepadState.oldState = state

            this.sendController(gamepadId, state)
        }
    }

    private onControllerData(data: ArrayBuffer) {
        this.buffer.reset()

        this.buffer.putU8Array(new Uint8Array(data))
        this.buffer.flip()

        // TODO: maybe move this into their respective controller channels?

        const ty = this.buffer.getU8()
        if (ty == 0) {
            // Rumble
            const id = this.buffer.getU8()
            const lowFrequencyMotor = this.buffer.getU16() / U16_MAX
            const highFrequencyMotor = this.buffer.getU16() / U16_MAX

            const gamepadIndex = this.gamepads[id]?.gamepadIndex
            if (gamepadIndex == null) {
                return
            }

            this.setGamepadEffect(gamepadIndex, "dual-rumble", { lowFrequencyMotor, highFrequencyMotor })
        } else if (ty == 1) {
            // Trigger Rumble
            const id = this.buffer.getU8()
            const leftTrigger = this.buffer.getU16() / U16_MAX
            const rightTrigger = this.buffer.getU16() / U16_MAX

            const gamepadIndex = this.gamepads[id]?.gamepadIndex
            if (gamepadIndex == null) {
                return
            }

            this.setGamepadEffect(gamepadIndex, "trigger-rumble", { leftTrigger, rightTrigger })
        }
    }

    // -- Controller rumble
    private gamepadRumbleCurrent: Array<{
        lowFrequencyMotor: number, highFrequencyMotor: number,
        leftTrigger: number, rightTrigger: number
    }> = []

    private emptyGamepadRumbleState() {
        return { lowFrequencyMotor: 0, highFrequencyMotor: 0, leftTrigger: 0, rightTrigger: 0 }
    }

    private setGamepadEffect(id: number, ty: "dual-rumble", params: { lowFrequencyMotor: number, highFrequencyMotor: number }): void
    private setGamepadEffect(id: number, ty: "trigger-rumble", params: { leftTrigger: number, rightTrigger: number }): void

    private setGamepadEffect(id: number, _ty: "dual-rumble" | "trigger-rumble", params: { lowFrequencyMotor: number, highFrequencyMotor: number } | { leftTrigger: number, rightTrigger: number }) {
        const rumble = this.gamepadRumbleCurrent[id] ??= this.emptyGamepadRumbleState()

        Object.assign(rumble, params)
    }

    private onGamepadRumbleInterval() {
        for (let id = 0; id < this.gamepads.length; id++) {
            const gamepadIndex = this.gamepads[id]?.gamepadIndex
            if (gamepadIndex == null) {
                continue
            }

            const rumble = this.gamepadRumbleCurrent[gamepadIndex]
            const gamepad = navigator.getGamepads()[gamepadIndex]
            if (gamepad && rumble) {
                this.refreshGamepadRumble(rumble, gamepad)
            }
        }
    }
    private refreshGamepadRumble(
        rumble: {
            lowFrequencyMotor: number, highFrequencyMotor: number,
            leftTrigger: number, rightTrigger: number
        },
        gamepad: Gamepad
    ) {
        // Browsers are making this more complicated than it is

        const actuators = this.collectActuators(gamepad)

        for (const actuator of actuators) {
            if ("effects" in actuator) {
                const supportedEffects = actuator.effects as Array<string>

                for (const effect of supportedEffects) {
                    if (effect == "dual-rumble") {
                        actuator.playEffect("dual-rumble", {
                            duration: CONTROLLER_RUMBLE_INTERVAL_MS,
                            weakMagnitude: rumble.lowFrequencyMotor,
                            strongMagnitude: rumble.highFrequencyMotor
                        })
                    } else if (effect == "trigger-rumble") {
                        actuator.playEffect("trigger-rumble", {
                            duration: CONTROLLER_RUMBLE_INTERVAL_MS,
                            leftTrigger: rumble.leftTrigger,
                            rightTrigger: rumble.rightTrigger
                        })
                    }
                }
            } else if ("type" in actuator && (actuator.type == "vibration" || actuator.type == "dual-rumble")) {
                actuator.playEffect(actuator.type as any, {
                    duration: CONTROLLER_RUMBLE_INTERVAL_MS,
                    weakMagnitude: rumble.lowFrequencyMotor,
                    strongMagnitude: rumble.highFrequencyMotor
                })
            } else if ("playEffect" in actuator && typeof actuator.playEffect == "function") {
                actuator.playEffect("dual-rumble", {
                    duration: CONTROLLER_RUMBLE_INTERVAL_MS,
                    weakMagnitude: rumble.lowFrequencyMotor,
                    strongMagnitude: rumble.highFrequencyMotor
                })
                actuator.playEffect("trigger-rumble", {
                    duration: CONTROLLER_RUMBLE_INTERVAL_MS,
                    leftTrigger: rumble.leftTrigger,
                    rightTrigger: rumble.rightTrigger
                })
            } else if ("pulse" in actuator && typeof actuator.pulse == "function") {
                const weak = Math.min(Math.max(rumble.lowFrequencyMotor, 0), 1);
                const strong = Math.min(Math.max(rumble.highFrequencyMotor, 0), 1);

                const average = (weak + strong) / 2.0

                actuator.pulse(average, CONTROLLER_RUMBLE_INTERVAL_MS)
            }
        }
    }

    // -- Controller Sending
    sendControllerAdd(id: number, supportedButtons: number, capabilities: number) {
        this.buffer.reset()

        this.buffer.putU8(0)
        this.buffer.putU8(id)
        this.buffer.putU32(supportedButtons)
        this.buffer.putU16(capabilities)

        if (!this.controllers) {
            showNotification("controller channel is not yet present, controller connect event is dropped")
        }
        trySendChannel(this.controllers, this.buffer)
    }
    sendControllerRemove(id: number) {
        this.buffer.reset()

        this.buffer.putU8(1)
        this.buffer.putU8(id)

        trySendChannel(this.controllers, this.buffer)
    }
    // Values
    // - Trigger: range 0..1
    // - Stick: range -1..1
    sendController(id: number, state: GamepadState) {
        this.buffer.reset()

        this.buffer.putU8(0)
        this.buffer.putU32(state.buttonFlags)
        this.buffer.putU8(Math.max(0.0, Math.min(1.0, state.leftTrigger)) * U8_MAX)
        this.buffer.putU8(Math.max(0.0, Math.min(1.0, state.rightTrigger)) * U8_MAX)
        this.buffer.putI16(Math.max(-1.0, Math.min(1.0, state.leftStickX)) * I16_MAX)
        this.buffer.putI16(Math.max(-1.0, Math.min(1.0, -state.leftStickY)) * I16_MAX)
        this.buffer.putI16(Math.max(-1.0, Math.min(1.0, state.rightStickX)) * I16_MAX)
        this.buffer.putI16(Math.max(-1.0, Math.min(1.0, -state.rightStickY)) * I16_MAX)

        trySendChannel(this.controllerInputs[id], this.buffer)
    }

}
