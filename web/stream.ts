import "./polyfill/index.js"
import { Api, apiGetRole, getApi } from "./api.js";
import { Component } from "./component/index.js";
import { showNotification } from "./component/notification.js";
import { InfoEvent, Stream } from "./stream/index.js"
import { getModalBackground, Modal, showMessage, showModal } from "./component/modal/index.js";
import { getSidebarRoot, setSidebar, setSidebarExtended, setSidebarStyle, Sidebar } from "./component/sidebar/index.js";
import { defaultStreamInputConfig, MouseMode, ScreenKeyboardSetVisibleEvent, StreamInputConfig } from "./stream/input.js";
import { getLocalStreamSettings, Settings } from "./component/settings_menu.js";
import { SelectComponent } from "./component/input.js";
import { DetailedRole, LogMessageType, StreamCapabilities, StreamKeys, StreamPermissions } from "./api_bindings.js";
import { KeyboardModeEvent, KeyboardModeWillChangeEvent, ScreenKeyboard, TextEvent } from "./screen_keyboard.js";
import { FormModal } from "./component/modal/form.js";
import { streamStatsToText } from "./stream/stats.js";
import { adoptRoleDefaultLanguage, getCurrentLanguage, getTranslations } from "./i18n.js";
import { requestKeyboardLock } from "./iframe.js";

let I = getTranslations(getCurrentLanguage())

async function startApp() {
    const api = await getApi()

    const bootstrapRole = await apiGetRole(api, { id: null })
    adoptRoleDefaultLanguage(bootstrapRole.role.default_settings)
    I = getTranslations(getCurrentLanguage())

    const rootElement = document.getElementById("root");
    if (rootElement == null) {
        showNotification(I.stream.rootNotFound, "error")
        return;
    }

    // Get Host and App via Query
    const queryParams = new URLSearchParams(location.search)

    const hostIdStr = queryParams.get("hostId")
    const appIdStr = queryParams.get("appId")
    if (hostIdStr == null || appIdStr == null) {
        await showMessage(I.stream.missingHostOrApp)

        window.close()
        return
    }
    const hostId = Number.parseInt(hostIdStr)
    const appId = Number.parseInt(appIdStr)

    // event propagation on overlays
    const sidebarRoot = getSidebarRoot()
    if (sidebarRoot) {
        stopPropagationOn(sidebarRoot)
    }

    const modalBackground = getModalBackground()
    if (modalBackground) {
        stopPropagationOn(modalBackground)
    }

    // Start and Mount App
    const app = new ViewerApp(api, hostId, appId, bootstrapRole.role)
    app.mount(rootElement);

    (window as any)["app"] = app
}

// Prevent starting transition
window.requestAnimationFrame(() => {
    // Note: elements is a live array
    const elements = document.getElementsByClassName("prevent-start-transition")
    while (elements.length > 0) {
        elements.item(0)?.classList.remove("prevent-start-transition")
    }
})

startApp()

class ViewerApp implements Component {
    private api: Api

    private sidebar: ViewerSidebar

    private div = document.createElement("div")

    private statsDiv = document.createElement("div")
    private localTouchCursorDiv = document.createElement("div")
    private stream: Stream
    private inputElement: HTMLDivElement

    private inputConfig: StreamInputConfig = defaultStreamInputConfig()
    private previousMouseMode: MouseMode
    private autoEnterFullscreenOnStart: boolean = false
    private pendingAutoFullscreenPrompt: boolean = false
    private fullscreenPromptShown: boolean = false
    private fullscreenOnNextInteractionArmed: boolean = false
    private pendingAutoFullscreenTouchGesture: boolean = false
    private pendingAutoFullscreenMouseGesture: boolean = false
    private manualFullscreenExitRequested: boolean = false
    private toggleFullscreenWithKeybind: boolean = false
    private hasShownFullscreenEscapeWarning = false
    private keyboardViewportBaselineHeight: number | null = null
    private streamVideoTopOffsetPx: number = 0
    private readonly pointerEventsSupported = "PointerEvent" in window
    private readonly pointerRawUpdateSupported = supportsUsablePointerRawUpdate()
    private activePointers: Map<number, string> = new Map()
    private autoFullscreenTouchPointers: Set<number> = new Set()

    private cachedStreamRect = new DOMRect()
    private streamRectRefreshFrame: number | null = null
    private streamResizeObserver: ResizeObserver | null = null
    private streamMutationObserver: MutationObserver | null = null
    private readonly eventListenerAbort = new AbortController()
    private statsUpdateInterval: number | null = null
    private touchUpdateFrame: number | null = null
    private gamepadUpdateFrame: number | null = null
    private animationLoopsActive = true
    private readonly touchUpdateFrameCallback = () => this.onTouchUpdate()
    private readonly gamepadUpdateFrameCallback = () => this.onGamepadUpdate()
    private readonly scheduleStreamRectRefresh = () => {
        if (this.streamRectRefreshFrame != null) {
            return
        }
        this.streamRectRefreshFrame = window.requestAnimationFrame(() => {
            this.streamRectRefreshFrame = null
            this.refreshStreamRect()
        })
    }
    private readonly scheduleViewportRefresh = () => {
        this.scheduleStreamRectRefresh()
        this.scheduleTouchUpdate()
    }

    constructor(api: Api, hostId: number, appId: number, bootstrapRole: DetailedRole) {
        this.api = api

        const inputElement = document.getElementById("input")
        if (!(inputElement instanceof HTMLDivElement)) {
            throw new Error("stream input element not found")
        }
        this.inputElement = inputElement

        const settings = getLocalStreamSettings(bootstrapRole.default_settings)
        Object.assign(this.inputConfig, {
            mouseMode: settings.mouseMode,
            mouseScrollMode: settings.mouseScrollMode,
            touchMode: settings.touchMode,
            localCursorSensitivity: settings.localCursorSensitivity,
            controllerConfig: settings.controllerConfig
        })

        // Configure sidebar
        this.sidebar = new ViewerSidebar(this)
        setSidebar(this.sidebar)

        // Configure stats element
        this.statsDiv.hidden = true
        this.statsDiv.classList.add("video-stats")
        this.localTouchCursorDiv.hidden = true
        this.localTouchCursorDiv.classList.add("local-touch-cursor")

        this.statsUpdateInterval = window.setInterval(() => {
            // Update stats display every 100ms
            const stats = this.getStream()?.getStats()
            if (stats && stats.isEnabled()) {
                this.statsDiv.hidden = false

                const text = streamStatsToText(stats.getCurrentStats())
                this.statsDiv.innerText = text
            } else {
                this.statsDiv.hidden = true
            }
        }, 100)
        this.div.appendChild(this.statsDiv)
        this.div.appendChild(this.localTouchCursorDiv)

        // Configure stream
        this.previousMouseMode = this.inputConfig.mouseMode

        const browserWidth = Math.max(document.documentElement.clientWidth || 0, window.innerWidth || 0)
        const browserHeight = Math.max(document.documentElement.clientHeight || 0, window.innerHeight || 0)

        this.autoEnterFullscreenOnStart = settings.enterFullscreenOnStreamStart
        this.toggleFullscreenWithKeybind = settings.toggleFullscreenWithKeybind

        this.stream = new Stream(this.api, hostId, appId, settings, [browserWidth, browserHeight], bootstrapRole.permissions)
        this.startStream(hostId, appId, bootstrapRole.permissions, settings, [browserWidth, browserHeight])
        this.initializeStreamRectCache()

        // Configure input
        this.addListeners(document)

        const listenerOptions = { signal: this.eventListenerAbort.signal }
        window.addEventListener("blur", () => {
            this.releaseAllInputState()
        }, listenerOptions)
        document.addEventListener("visibilitychange", () => {
            if (document.visibilityState !== "visible") {
                this.releaseAllInputState()
            }
        }, listenerOptions)
        window.addEventListener("pagehide", () => this.releaseAllInputState(), listenerOptions)

        window.addEventListener("resize", this.scheduleViewportRefresh, listenerOptions)
        window.addEventListener("orientationchange", this.scheduleViewportRefresh, listenerOptions)
        window.visualViewport?.addEventListener("resize", this.scheduleViewportRefresh, listenerOptions)
        window.visualViewport?.addEventListener("scroll", this.scheduleViewportRefresh, listenerOptions)

        document.addEventListener("pointerlockchange", this.onPointerLockChange.bind(this), listenerOptions)
        document.addEventListener("fullscreenchange", this.onFullscreenChange.bind(this), listenerOptions)

        window.addEventListener("gamepadconnected", this.onGamepadConnect.bind(this), listenerOptions)
        window.addEventListener("gamepaddisconnected", this.onGamepadDisconnect.bind(this), listenerOptions)
        // Connect all gamepads
        for (const gamepad of navigator.getGamepads()) {
            if (gamepad != null) {
                this.onGamepadAdd(gamepad)
            }
        }
    }
    private addListeners(element: GlobalEventHandlers) {
        const activeOptions = { passive: false, signal: this.eventListenerAbort.signal }
        const listenerOptions = { signal: this.eventListenerAbort.signal }
        element.addEventListener("keydown", this.onKeyDown.bind(this), activeOptions)
        element.addEventListener("keyup", this.onKeyUp.bind(this), activeOptions)
        element.addEventListener("paste", this.onPaste.bind(this), listenerOptions)

        if (this.pointerEventsSupported) {
            element.addEventListener("pointerdown", this.onPointerDown.bind(this), activeOptions)
            element.addEventListener("pointerup", this.onPointerUp.bind(this), activeOptions)
            element.addEventListener("pointercancel", this.onPointerCancel.bind(this), activeOptions)
            element.addEventListener("lostpointercapture", this.onLostPointerCapture.bind(this), activeOptions)

            if (this.pointerRawUpdateSupported) {
                element.addEventListener("pointerrawupdate", this.onPointerMove.bind(this) as EventListener, activeOptions)
            } else {
                element.addEventListener("pointermove", this.onPointerMove.bind(this), activeOptions)
            }
        } else {
            element.addEventListener("mousedown", this.onMouseButtonDown.bind(this), activeOptions)
            element.addEventListener("mouseup", this.onMouseButtonUp.bind(this), activeOptions)
            element.addEventListener("mousemove", this.onMouseMove.bind(this), activeOptions)

            element.addEventListener("touchstart", this.onTouchStart.bind(this), activeOptions)
            element.addEventListener("touchend", this.onTouchEnd.bind(this), activeOptions)
            element.addEventListener("touchcancel", this.onTouchCancel.bind(this), activeOptions)
            element.addEventListener("touchmove", this.onTouchMove.bind(this), activeOptions)
        }

        element.addEventListener("wheel", this.onMouseWheel.bind(this), activeOptions)
        element.addEventListener("contextmenu", this.onContextMenu.bind(this), activeOptions)
    }

    private async startStream(hostId: number, appId: number, permissions: StreamPermissions, settings: Settings, browserSize: [number, number]) {
        setSidebarStyle({
            edge: settings.sidebarEdge,
        })

        // Add app info listener
        this.stream.addInfoListener(this.onInfo.bind(this))

        // Create connection info modal
        const connectionInfo = new ConnectionInfoModal()
        const connectionInfoListener = connectionInfo.onInfo.bind(connectionInfo)
        this.stream.addInfoListener(connectionInfoListener)
        void showModal(connectionInfo).then(async () => {
            this.stream.removeInfoListener(connectionInfoListener)
            if (this.autoEnterFullscreenOnStart && this.pendingAutoFullscreenPrompt && !this.fullscreenPromptShown && !this.isFullscreen()) {
                this.fullscreenPromptShown = true
                this.pendingAutoFullscreenPrompt = false
                this.armFullscreenOnNextInteraction()
            }
        })

        // Prime UI state once. Ongoing touch/gamepad polling is scheduled only
        // while that kind of input is active.
        this.scheduleTouchUpdate()

        this.stream.getInput().addScreenKeyboardVisibleEvent(this.onScreenKeyboardSetVisible.bind(this))

        this.stream.mount(this.div)

        if (this.autoEnterFullscreenOnStart) {
            this.pendingAutoFullscreenPrompt = true
        }
    }

    private async onInfo(event: InfoEvent) {
        const data = event.detail

        if (data.type == "app") {
            const app = data.app

            document.title = `Stream: ${app.title}`
        } else if (data.type == "connectionComplete") {
            this.sidebar.onCapabilitiesChange(data.capabilities)
        } else if (data.type == "videoReady") {
            this.scheduleStreamRectRefresh()
        }
    }

    private focusInput() {
        if (this.stream.getInput().getCurrentPredictedTouchAction() != "screenKeyboard" && !this.sidebar.getScreenKeyboard().isVisible()) {
            const inputElement = document.getElementById("input") as HTMLDivElement
            inputElement.focus()
        }
    }

    onUserInteraction() {
        this.focusInput()

        this.stream.getVideoRenderer()?.onUserInteraction()
        this.stream.getAudioPlayer()?.onUserInteraction()
    }
    private armFullscreenOnNextInteraction() {
        if (this.autoEnterFullscreenOnStart) {
            this.fullscreenOnNextInteractionArmed = true
        }
    }
    private consumeAutoFullscreenInteraction(): boolean {
        if (!this.fullscreenOnNextInteractionArmed || this.isFullscreen()) {
            return false
        }

        this.fullscreenOnNextInteractionArmed = false
        void this.requestFullscreen().then(() => {
            if (!this.isFullscreen()) {
                this.armFullscreenOnNextInteraction()
            }
        })
        return true
    }
    private beginAutoFullscreenTouchGesture(): boolean {
        if (!this.fullscreenOnNextInteractionArmed || this.isFullscreen()) {
            return false
        }

        this.pendingAutoFullscreenTouchGesture = true
        return true
    }
    private consumeAutoFullscreenTouchGesture(): boolean {
        if (!this.pendingAutoFullscreenTouchGesture) {
            return false
        }

        this.pendingAutoFullscreenTouchGesture = false
        return this.consumeAutoFullscreenInteraction()
    }
    private onScreenKeyboardSetVisible(event: ScreenKeyboardSetVisibleEvent) {
        console.info(event.detail)
        const screenKeyboard = this.sidebar.getScreenKeyboard()

        const newShown = event.detail.visible
        if (newShown != screenKeyboard.isVisible()) {
            if (newShown) {
                screenKeyboard.show()
            } else {
                screenKeyboard.hide()
            }
        }
    }

    // Input
    getInputConfig(): StreamInputConfig {
        return this.inputConfig
    }
    setInputConfig(config: StreamInputConfig) {
        Object.assign(this.inputConfig, config)

        const pointingModeChanged = this.stream.getInput().setConfig(this.inputConfig, this.getStreamRect())
        if (pointingModeChanged) {
            this.clearActivePointerCaptures()
        }
        this.renderLocalTouchCursor()
    }

    // Keyboard
    onKeyDown(event: KeyboardEvent) {
        this.onUserInteraction()

        console.debug(event)
        if (event.shiftKey && event.ctrlKey && event.code == "KeyV") {
            // We are likely pasting -> don't send keys
        } else if (event.code == "F11") {
            // Allow manual fullscreen
        } else {
            event.preventDefault()
            this.stream.getInput().onKeyDown(event)
        }

        event.stopPropagation()
    }

    private isTogglingFullscreenWithKeybind: "waitForCtrl" | "makingFullscreen" | "none" = "none"
    onKeyUp(event: KeyboardEvent) {
        this.onUserInteraction()

        event.preventDefault()
        this.stream.getInput().onKeyUp(event)
        event.stopPropagation()

        if (this.toggleFullscreenWithKeybind && this.isTogglingFullscreenWithKeybind == "none" && event.ctrlKey && event.shiftKey && event.code == "KeyI") {
            this.isTogglingFullscreenWithKeybind = "waitForCtrl"
        }
        if (this.isTogglingFullscreenWithKeybind == "waitForCtrl" && (event.code == "ControlRight" || event.code == "ControlLeft")) {
            this.isTogglingFullscreenWithKeybind = "makingFullscreen";

            (async () => {
                if (this.isFullscreen()) {
                    await this.exitPointerLock()
                    await this.exitFullscreen()
                } else {
                    await this.requestFullscreen()
                    await this.requestPointerLock()
                }

                this.isTogglingFullscreenWithKeybind = "none"
            })()
        }
    }

    onPaste(event: ClipboardEvent) {
        this.onUserInteraction()

        this.stream.getInput().onPaste(event)

        event.stopPropagation()
    }

    // Mouse
    onMouseButtonDown(event: MouseEvent) {
        if (this.consumeAutoFullscreenInteraction()) {
            this.pendingAutoFullscreenMouseGesture = true
            event.preventDefault()
            event.stopPropagation()
            return
        }

        this.onUserInteraction()

        event.preventDefault()
        this.stream.getInput().onMouseDown(event, this.getStreamRect());

        event.stopPropagation()
    }
    onMouseButtonUp(event: MouseEvent) {
        if (this.pendingAutoFullscreenMouseGesture) {
            this.pendingAutoFullscreenMouseGesture = false
            event.preventDefault()
            event.stopPropagation()
            return
        }

        this.onUserInteraction()

        event.preventDefault()
        this.stream.getInput().onMouseUp(event)

        event.stopPropagation()
    }
    onMouseMove(event: MouseEvent) {
        if (this.pendingAutoFullscreenMouseGesture) {
            event.preventDefault()
            event.stopPropagation()
            return
        }

        event.preventDefault()
        this.stream.getInput().onMouseMove(event, this.getStreamRect())
        this.scheduleTouchUpdate()

        event.stopPropagation()
    }
    onMouseWheel(event: WheelEvent) {
        event.preventDefault()
        this.stream.getInput().onMouseWheel(event)

        event.stopPropagation()
    }
    onContextMenu(event: MouseEvent) {
        event.preventDefault()

        event.stopPropagation()
    }

    // Pointer Events are used for mouse, pen, and multi-touch on supporting browsers.
    onPointerDown(event: PointerEvent) {
        this.capturePointer(event)

        if (event.pointerType == "touch") {
            if (this.pendingAutoFullscreenTouchGesture || this.beginAutoFullscreenTouchGesture()) {
                this.autoFullscreenTouchPointers.add(event.pointerId)
                event.preventDefault()
                event.stopPropagation()
                return
            }
        } else if (this.consumeAutoFullscreenInteraction()) {
            this.pendingAutoFullscreenMouseGesture = true
            event.preventDefault()
            event.stopPropagation()
            return
        }

        this.onUserInteraction()
        event.preventDefault()
        this.stream.getInput().onPointerDown(event, this.getStreamRect())
        this.scheduleTouchUpdate()
        event.stopPropagation()
    }

    onPointerUp(event: PointerEvent) {
        if (event.pointerType == "touch" && this.autoFullscreenTouchPointers.has(event.pointerId)) {
            this.autoFullscreenTouchPointers.delete(event.pointerId)
            this.finishPointerCapture(event.pointerId)
            if (this.autoFullscreenTouchPointers.size == 0) {
                this.consumeAutoFullscreenTouchGesture()
            }
            event.preventDefault()
            event.stopPropagation()
            return
        }
        if (event.pointerType != "touch" && this.pendingAutoFullscreenMouseGesture) {
            this.pendingAutoFullscreenMouseGesture = false
            this.finishPointerCapture(event.pointerId)
            event.preventDefault()
            event.stopPropagation()
            return
        }

        this.onUserInteraction()
        event.preventDefault()
        this.stream.getInput().onPointerUp(event, this.getStreamRect())
        this.scheduleTouchUpdate()
        this.finishPointerCapture(event.pointerId)
        event.stopPropagation()
    }

    onPointerCancel(event: PointerEvent) {
        const wasAutoFullscreenTouch = this.autoFullscreenTouchPointers.delete(event.pointerId)
        if (wasAutoFullscreenTouch) {
            if (this.autoFullscreenTouchPointers.size == 0) {
                this.pendingAutoFullscreenTouchGesture = false
            }
        } else if (event.pointerType != "touch" && this.pendingAutoFullscreenMouseGesture) {
            this.pendingAutoFullscreenMouseGesture = false
        } else {
            this.stream.getInput().onPointerCancel(event, this.getStreamRect())
            this.scheduleTouchUpdate()
        }

        this.finishPointerCapture(event.pointerId)
        event.preventDefault()
        event.stopPropagation()
    }

    onPointerMove(event: PointerEvent) {
        if (
            this.autoFullscreenTouchPointers.has(event.pointerId) ||
            (event.pointerType != "touch" && this.pendingAutoFullscreenMouseGesture)
        ) {
            event.preventDefault()
            event.stopPropagation()
            return
        }

        event.preventDefault()
        this.stream.getInput().onPointerMove(event, this.getStreamRect())
        this.scheduleTouchUpdate()
        event.stopPropagation()
    }

    onLostPointerCapture(event: PointerEvent) {
        if (!this.activePointers.has(event.pointerId)) {
            return
        }

        this.activePointers.delete(event.pointerId)
        if (this.autoFullscreenTouchPointers.delete(event.pointerId)) {
            if (this.autoFullscreenTouchPointers.size == 0) {
                this.pendingAutoFullscreenTouchGesture = false
            }
            return
        }

        this.stream.getInput().onPointerCancel(event, this.getStreamRect())
        this.scheduleTouchUpdate()
    }

    private capturePointer(event: PointerEvent) {
        this.activePointers.set(event.pointerId, event.pointerType)
        try {
            this.inputElement.setPointerCapture(event.pointerId)
        } catch {
            // Pointer capture can fail for synthetic or already-ended pointers.
        }
    }

    private finishPointerCapture(pointerId: number) {
        this.activePointers.delete(pointerId)
        try {
            if (this.inputElement.hasPointerCapture(pointerId)) {
                this.inputElement.releasePointerCapture(pointerId)
            }
        } catch {
            // The browser may have implicitly released capture already.
        }
    }

    private clearActivePointerCaptures() {
        const pointerIds = Array.from(this.activePointers.keys())
        this.activePointers.clear()
        this.autoFullscreenTouchPointers.clear()
        this.pendingAutoFullscreenTouchGesture = false
        this.pendingAutoFullscreenMouseGesture = false

        for (const pointerId of pointerIds) {
            try {
                if (this.inputElement.hasPointerCapture(pointerId)) {
                    this.inputElement.releasePointerCapture(pointerId)
                }
            } catch {
                // Ignore pointers that ended while cleanup was running.
            }
        }
        this.scheduleTouchUpdate()
    }

    private releaseAllInputState() {
        this.stream.getInput().releaseAllInputs(this.getStreamRect())
        this.clearActivePointerCaptures()
        this.scheduleTouchUpdate()
    }

    // Touch
    onTouchStart(event: TouchEvent) {
        if (this.beginAutoFullscreenTouchGesture()) {
            event.preventDefault()
            event.stopPropagation()
            return
        }

        this.onUserInteraction()

        event.preventDefault()
        this.stream.getInput().onTouchStart(event, this.getStreamRect())
        this.scheduleTouchUpdate()

        event.stopPropagation()
    }
    onTouchEnd(event: TouchEvent) {
        if (this.consumeAutoFullscreenTouchGesture()) {
            event.preventDefault()
            event.stopPropagation()
            return
        }

        this.onUserInteraction()

        event.preventDefault()
        this.stream.getInput().onTouchEnd(event, this.getStreamRect())
        this.scheduleTouchUpdate()

        event.stopPropagation()
    }
    onTouchCancel(event: TouchEvent) {
        if (this.pendingAutoFullscreenTouchGesture) {
            this.pendingAutoFullscreenTouchGesture = false
            event.preventDefault()
            event.stopPropagation()
            return
        }

        this.pendingAutoFullscreenTouchGesture = false

        this.onUserInteraction()

        event?.preventDefault()
        this.stream.getInput().onTouchCancel(event, this.getStreamRect())
        this.scheduleTouchUpdate()

        event.stopPropagation()
    }
    private scheduleTouchUpdate() {
        if (!this.animationLoopsActive || this.touchUpdateFrame != null) {
            return
        }
        this.touchUpdateFrame = window.requestAnimationFrame(this.touchUpdateFrameCallback)
    }
    onTouchUpdate() {
        this.touchUpdateFrame = null
        if (!this.animationLoopsActive) {
            return
        }
        this.stream.getInput().onTouchUpdate(this.getStreamRect())
        this.updateKeyboardViewportVideoOffset()
        this.renderLocalTouchCursor()

        if (this.stream.getInput().hasActiveTouches()) {
            this.scheduleTouchUpdate()
        }
    }
    onTouchMove(event: TouchEvent) {
        if (this.pendingAutoFullscreenTouchGesture) {
            event.preventDefault()
            event.stopPropagation()
            return
        }

        event.preventDefault()
        this.stream.getInput().onTouchMove(event, this.getStreamRect())
        this.scheduleTouchUpdate()

        event.stopPropagation()
    }

    // Gamepad
    onGamepadConnect(event: GamepadEvent) {
        this.onGamepadAdd(event.gamepad)
    }
    onGamepadAdd(gamepad: Gamepad) {
        this.stream.getInput().onGamepadConnect(gamepad)
        this.scheduleGamepadUpdate()
    }
    onGamepadDisconnect(event: GamepadEvent) {
        this.stream.getInput().onGamepadDisconnect(event)
        this.scheduleGamepadUpdate()
    }
    private scheduleGamepadUpdate() {
        if (!this.animationLoopsActive || this.gamepadUpdateFrame != null) {
            return
        }
        this.gamepadUpdateFrame = window.requestAnimationFrame(this.gamepadUpdateFrameCallback)
    }
    onGamepadUpdate() {
        this.gamepadUpdateFrame = null
        if (!this.animationLoopsActive) {
            return
        }
        this.stream.getInput().onGamepadUpdate()

        if (navigator.getGamepads().some(gamepad => gamepad != null)) {
            this.scheduleGamepadUpdate()
        }
    }

    // Fullscreen
    private async promptAutoFullscreen() {
        await showModal(new AutoFullscreenModal(this.requestFullscreen.bind(this)))
    }
    async requestFullscreen(showEscapeWarning: boolean = true) {
        const body = document.body
        if (body) {
            if (!("requestFullscreen" in body && typeof body.requestFullscreen == "function")) {
                await showMessage(I.stream.fullscreenUnsupported)

                return
            }

            this.focusInput()

            if (!this.isFullscreen()) {
                try {
                    await body.requestFullscreen({
                        navigationUI: "hide"
                    })
                } catch (e) {
                    console.warn("failed to request fullscreen", e)
                }
            }

            try {
                await requestKeyboardLock();
                if (showEscapeWarning && !this.hasShownFullscreenEscapeWarning) {
                    showNotification(I.stream.fullscreenEscapeHint, "info")
                    this.hasShownFullscreenEscapeWarning = true
                }
            } catch (e) {
                console.warn("Keyboard lock failed, skipping notification.", e);
            }

            if (this.getStream()?.getInput().getConfig().mouseMode == "relative") {
                await this.requestPointerLock()
            }

            try {
                if (screen && "orientation" in screen) {
                    const orientation = screen.orientation

                    if ("lock" in orientation && typeof orientation.lock == "function") {
                        await orientation.lock("landscape")
                    }
                }
            } catch (e) {
                console.warn("failed to set orientation to landscape", e)
            }
        } else {
            console.warn("root element not found")
        }
    }
    async exitFullscreen() {
        if ("keyboard" in navigator && navigator.keyboard && "unlock" in navigator.keyboard) {
            await navigator.keyboard.unlock()
        }

        if ("exitFullscreen" in document && typeof document.exitFullscreen == "function") {
            await document.exitFullscreen()
        }
    }
    isFullscreen(): boolean {
        return "fullscreenElement" in document && !!document.fullscreenElement
    }
    private async onFullscreenChange() {
        this.scheduleStreamRectRefresh()
        if (this.isFullscreen()) {
            this.fullscreenOnNextInteractionArmed = false
            this.pendingAutoFullscreenTouchGesture = false
            this.pendingAutoFullscreenMouseGesture = false
            this.manualFullscreenExitRequested = false
        } else {
            const manualExit = this.manualFullscreenExitRequested
            this.manualFullscreenExitRequested = false

            if (this.autoEnterFullscreenOnStart && !manualExit) {
                this.armFullscreenOnNextInteraction()
            }
        }

        this.checkFullyImmersed()
    }
    markManualFullscreenExitRequested() {
        this.manualFullscreenExitRequested = true
    }

    // Pointer Lock
    async requestPointerLock(errorIfNotFound: boolean = false) {
        this.previousMouseMode = this.inputConfig.mouseMode

        const inputElement = document.getElementById("input") as HTMLDivElement

        if (inputElement && "requestPointerLock" in inputElement && typeof inputElement.requestPointerLock == "function") {
            this.focusInput()

            this.inputConfig.mouseMode = "relative"
            this.setInputConfig(this.inputConfig)

            setSidebarExtended(false)

            const onLockError = () => {
                document.removeEventListener("pointerlockerror", onLockError)

                // Fallback: try to request pointer lock without options
                inputElement.requestPointerLock()
            }

            document.addEventListener("pointerlockerror", onLockError, { once: true })

            try {
                let promise = inputElement.requestPointerLock({
                    unadjustedMovement: true
                })

                if (promise) {
                    await promise
                } else {
                    inputElement.requestPointerLock()
                }
            } catch (error) {
                // Some platforms do not support unadjusted movement. If you
                // would like PointerLock anyway, request again.
                if (error instanceof Error && error.name == "NotSupportedError") {
                    inputElement.requestPointerLock()
                } else {
                    throw error
                }
            } finally {
                document.removeEventListener("pointerlockerror", onLockError)
            }

        } else if (errorIfNotFound) {
            await showMessage(I.stream.pointerLockUnsupported)
        }
    }
    async exitPointerLock() {
        if ("exitPointerLock" in document && typeof document.exitPointerLock == "function") {
            document.exitPointerLock()
        }
    }
    private onPointerLockChange() {
        this.checkFullyImmersed()

        if (!document.pointerLockElement) {
            this.inputConfig.mouseMode = this.previousMouseMode
            this.setInputConfig(this.inputConfig)
        }
    }

    // -- Fully immersed Fullscreen -> Fullscreen API + Pointer Lock
    private checkFullyImmersed() {
        if ("pointerLockElement" in document && document.pointerLockElement &&
            "fullscreenElement" in document && document.fullscreenElement) {
            // We're fully immersed -> remove sidebar
            setSidebar(null)
        } else {
            setSidebar(this.sidebar)
        }
    }
    private initializeStreamRectCache() {
        if ("ResizeObserver" in window) {
            this.streamResizeObserver = new ResizeObserver(this.scheduleStreamRectRefresh)
            this.streamResizeObserver.observe(document.documentElement)
            this.streamResizeObserver.observe(this.div)
        }

        if ("MutationObserver" in window) {
            this.streamMutationObserver = new MutationObserver(this.scheduleStreamRectRefresh)
            this.streamMutationObserver.observe(this.div, { childList: true, subtree: true })
        }

        this.refreshStreamRect()
    }
    private refreshStreamRect() {
        const renderer = this.stream.getVideoRenderer()
        const rect = renderer?.getStreamRect()
        if (rect && this.isUsableStreamRect(rect)) {
            this.cachedStreamRect = new DOMRect(rect.left, rect.top, rect.width, rect.height)
        } else if (!this.isUsableStreamRect(this.cachedStreamRect)) {
            this.cachedStreamRect = new DOMRect()
        }

        if (this.streamResizeObserver) {
            for (const element of this.div.querySelectorAll(".video-stream")) {
                this.streamResizeObserver.observe(element)
            }
        }
    }
    private isUsableStreamRect(rect: DOMRect): boolean {
        return Number.isFinite(rect.left) && Number.isFinite(rect.top) &&
            Number.isFinite(rect.width) && Number.isFinite(rect.height) &&
            rect.width > 0 && rect.height > 0
    }
    private renderLocalTouchCursor() {
        const localCursorState = this.stream.getInput().getLocalCursorState()
        if (!localCursorState?.visible) {
            this.localTouchCursorDiv.hidden = true
            return
        }

        const rect = this.getStreamRect()
        if (rect.width <= 0 || rect.height <= 0) {
            this.localTouchCursorDiv.hidden = true
            return
        }

        this.localTouchCursorDiv.hidden = false
        this.localTouchCursorDiv.style.left = `${rect.left + localCursorState.x * rect.width}px`
        this.localTouchCursorDiv.style.top = `${rect.top + localCursorState.y * rect.height}px`
    }

    onScreenKeyboardModeWillChange(event: KeyboardModeWillChangeEvent) {
        if (event.detail.enabled) {
            this.captureKeyboardViewportBaseline()
        }
    }

    private captureKeyboardViewportBaseline() {
        this.keyboardViewportBaselineHeight = window.visualViewport?.height ?? null
        this.streamVideoTopOffsetPx = 0
        this.applyStreamVideoTopOffset()
        this.updateKeyboardFloatingButtonPosition()
    }
    resetKeyboardViewportVideoOffset() {
        this.keyboardViewportBaselineHeight = null
        this.streamVideoTopOffsetPx = 0
        this.applyStreamVideoTopOffset()
        this.resetKeyboardFloatingButtonPosition()
    }
    private updateKeyboardViewportVideoOffset() {
        this.updateKeyboardFloatingButtonPosition()

        const screenKeyboard = this.sidebar.getScreenKeyboard()
        const visualViewport = window.visualViewport
        const baselineHeight = this.keyboardViewportBaselineHeight
        const localCursorState = this.stream.getInput().getLocalCursorState()

        if (!screenKeyboard.isVisible() || !visualViewport || baselineHeight == null) {
            if (this.streamVideoTopOffsetPx != 0 && !screenKeyboard.isVisible()) {
                this.resetKeyboardViewportVideoOffset()
            }
            return
        }

        const viewportShrink = baselineHeight - visualViewport.height
        if (viewportShrink < 80) {
            if (this.streamVideoTopOffsetPx != 0) {
                this.streamVideoTopOffsetPx = 0
                this.applyStreamVideoTopOffset()
            }
            return
        }

        const streamRect = this.getStreamRect()
        if (streamRect.width <= 0 || streamRect.height <= 0) {
            return
        }

        const visibleTop = visualViewport.offsetTop
        const visibleBottom = visualViewport.offsetTop + visualViewport.height

        let newTopOffsetPx = this.streamVideoTopOffsetPx
        if (localCursorState.visible) {
            let delta = 0

            const safeMargin = Math.min(100, visualViewport.height * 0.25)
            const cursorY = streamRect.top + localCursorState.y * streamRect.height

            if (cursorY < visibleTop + safeMargin) {
                delta = visibleTop + safeMargin - cursorY
            } else if (cursorY > visibleBottom - safeMargin) {
                delta = visibleBottom - safeMargin - cursorY
            }

            newTopOffsetPx += delta
        } else {
            const screenTopToVideoTop = visualViewport.height - streamRect.height
            if (screenTopToVideoTop > 0) {
                newTopOffsetPx = visibleTop - screenTopToVideoTop
            }
        }

        if (Math.abs(newTopOffsetPx - this.streamVideoTopOffsetPx) >= 1) {
            this.streamVideoTopOffsetPx = newTopOffsetPx
            this.applyStreamVideoTopOffset()
        }
    }
    private applyStreamVideoTopOffset() {
        if (Math.abs(this.streamVideoTopOffsetPx) < 0.5) {
            document.documentElement.style.removeProperty("--stream-video-top")
            this.scheduleStreamRectRefresh()
            return
        }

        document.documentElement.style.setProperty("--stream-video-top", `calc(50% + ${this.streamVideoTopOffsetPx}px)`)
        this.scheduleStreamRectRefresh()
    }
    private updateKeyboardFloatingButtonPosition() {
        const screenKeyboard = this.sidebar.getScreenKeyboard()
        const visualViewport = window.visualViewport
        if (!screenKeyboard.isVisible() || !visualViewport) {
            this.resetKeyboardFloatingButtonPosition()
            return
        }

        const bottomInset = Math.min(16, visualViewport.height * 0.08)
        const buttonTop = visualViewport.offsetTop + visualViewport.height - bottomInset
        document.documentElement.style.setProperty("--stream-keyboard-button-top", `${buttonTop}px`)
    }
    private resetKeyboardFloatingButtonPosition() {
        document.documentElement.style.removeProperty("--stream-keyboard-button-top")
    }

    mount(parent: HTMLElement): void {
        parent.appendChild(this.div)
        this.scheduleStreamRectRefresh()
    }
    unmount(parent: HTMLElement): void {
        this.releaseAllInputState()
        this.stream.getInput().dispose()
        this.animationLoopsActive = false
        this.eventListenerAbort.abort()
        if (this.statsUpdateInterval != null) {
            window.clearInterval(this.statsUpdateInterval)
            this.statsUpdateInterval = null
        }
        if (this.touchUpdateFrame != null) {
            window.cancelAnimationFrame(this.touchUpdateFrame)
            this.touchUpdateFrame = null
        }
        if (this.gamepadUpdateFrame != null) {
            window.cancelAnimationFrame(this.gamepadUpdateFrame)
            this.gamepadUpdateFrame = null
        }
        if (this.streamRectRefreshFrame != null) {
            window.cancelAnimationFrame(this.streamRectRefreshFrame)
            this.streamRectRefreshFrame = null
        }
        this.streamResizeObserver?.disconnect()
        this.streamMutationObserver?.disconnect()
        window.removeEventListener("resize", this.scheduleViewportRefresh)
        window.removeEventListener("orientationchange", this.scheduleViewportRefresh)
        window.visualViewport?.removeEventListener("resize", this.scheduleViewportRefresh)
        window.visualViewport?.removeEventListener("scroll", this.scheduleViewportRefresh)
        parent.removeChild(this.div)
    }

    getStreamRect(): DOMRect {
        return this.cachedStreamRect
    }
    getStream(): Stream | null {
        return this.stream
    }
}

class ConnectionInfoModal implements Modal<void> {

    private eventTarget = new EventTarget()

    private root = document.createElement("div")

    private textTy: LogMessageType | null = null
    private text = document.createElement("p")

    private options = document.createElement("div")
    private debugDetailButton = document.createElement("button")
    private closeButton = document.createElement("button")

    private debugDetail = "" // We store this seperate because line breaks don't work when the element is not mounted on the dom
    private debugDetailDisplay = document.createElement("div")

    constructor() {
        this.root.classList.add("modal-video-connect")

        this.text.innerText = I.stream.connecting
        this.root.appendChild(this.text)

        this.root.appendChild(this.options)
        this.options.classList.add("modal-video-connect-options")

        this.debugDetailButton.innerText = I.stream.showLogs
        this.debugDetailButton.addEventListener("click", this.onDebugDetailClick.bind(this))
        this.options.appendChild(this.debugDetailButton)

        this.closeButton.innerText = I.stream.close
        this.closeButton.addEventListener("click", this.onClose.bind(this))
        this.options.appendChild(this.closeButton)

        this.debugDetailDisplay.classList.add("textlike")
        this.debugDetailDisplay.classList.add("modal-video-connect-debug")
    }

    private onDebugDetailClick() {
        let debugDetailCurrentlyShown = this.root.contains(this.debugDetailDisplay)

        if (debugDetailCurrentlyShown) {
            this.debugDetailButton.innerText = I.stream.showLogs
            this.root.removeChild(this.debugDetailDisplay)
        } else {
            this.debugDetailButton.innerText = I.stream.hideLogs
            this.root.appendChild(this.debugDetailDisplay)
            this.debugDetailDisplay.innerText = this.debugDetail
        }
    }

    private debugLog(line: string) {
        this.debugDetail += `${line}\n`
        this.debugDetailDisplay.innerText = this.debugDetail
        console.info(`[Stream]: ${line}`)
    }

    onInfo(event: InfoEvent) {
        const data = event.detail

        if (data.type == "connectionComplete") {
            const text = I.stream.connectionComplete
            this.text.innerText = text
            this.debugLog(text)
        } else if (data.type == "videoReady") {

            this.eventTarget.dispatchEvent(new Event("ml-connected"))
        } else if (data.type == "addDebugLine") {
            const message = data.line.trim()
            if (message) {
                this.debugLog(message)

                if (!this.textTy) {
                    this.text.innerText = message
                    this.textTy = data.additional?.type ?? null
                } else if (data.additional?.type == "fatalDescription" || data.additional?.type == "ifErrorDescription") {
                    if (this.text.innerText) {
                        this.text.innerText += "\n" + message
                    } else {
                        this.text.innerText = message
                    }
                    this.textTy = data.additional.type
                }
            }

            if (data.additional?.type == "fatal" || data.additional?.type == "fatalDescription") {
                showModal(this)
            } else if (data.additional?.type == "informError") {
                showNotification(data.line)
            }
        } else if (data.type == "serverMessage") {
            const text = I.stream.serverMessage(data.message)
            this.text.innerText = text
            this.debugLog(text)
        }
    }

    onClose() {
        showModal(null)
    }

    onFinish(abort: AbortSignal): Promise<void> {
        return new Promise((resolve, reject) => {
            this.eventTarget.addEventListener("ml-connected", () => resolve(), { once: true, signal: abort })
        })
    }

    mount(parent: HTMLElement): void {
        parent.appendChild(this.root)
    }
    unmount(parent: HTMLElement): void {
        parent.removeChild(this.root)
    }
}

class AutoFullscreenModal implements Component, Modal<void> {
    private message = document.createElement("p")
    private root = document.createElement("div")
    private okButton = document.createElement("button")
    private cancelButton = document.createElement("button")
    private onConfirm: () => Promise<void>

    constructor(onConfirm: () => Promise<void>) {
        this.onConfirm = onConfirm
        this.message.innerText = I.stream.autoFullscreenPrompt
        this.okButton.innerText = I.modal.ok
        this.cancelButton.innerText = I.modal.cancel
    }

    mount(parent: HTMLElement): void {
        this.root.appendChild(this.message)
        this.root.appendChild(this.okButton)
        this.root.appendChild(this.cancelButton)
        parent.appendChild(this.root)
    }
    unmount(parent: HTMLElement): void {
        parent.removeChild(this.root)
    }

    onFinish(abort: AbortSignal): Promise<void> {
        return new Promise((resolve) => {
            this.okButton.addEventListener("click", async () => {
                await this.onConfirm()
                resolve()
            }, { once: true, signal: abort })

            this.cancelButton.addEventListener("click", () => {
                resolve()
            }, { once: true, signal: abort })
        })
    }
}

class ViewerSidebar implements Component, Sidebar {
    private app: ViewerApp

    private div = document.createElement("div")

    private buttonDiv = document.createElement("div")

    private sendKeycodeButton = document.createElement("button")

    private keyboardButton = document.createElement("button")
    private floatingKeyboardButton = document.createElement("button")
    private screenKeyboard = new ScreenKeyboard()

    private lockMouseButton = document.createElement("button")
    private fullscreenButton = document.createElement("button")

    private statsButton = document.createElement("button")
    private exitStreamButton = document.createElement("button")

    private mouseMode: SelectComponent
    private touchMode: SelectComponent

    constructor(app: ViewerApp) {
        this.app = app

        // Configure divs
        this.div.classList.add("sidebar-stream")

        this.buttonDiv.classList.add("sidebar-stream-buttons")
        this.div.appendChild(this.buttonDiv)

        // Send keycode
        this.sendKeycodeButton.innerText = I.stream.sendKeycode
        this.sendKeycodeButton.addEventListener("click", async () => {
            const key = await showModal(new SendKeycodeModal())

            if (key == null) {
                return
            }

            this.app.getStream()?.getInput().sendKey(true, key, 0)
            this.app.getStream()?.getInput().sendKey(false, key, 0)
        })
        this.buttonDiv.appendChild(this.sendKeycodeButton)

        // Pointer Lock
        this.lockMouseButton.innerText = I.stream.lockMouse
        this.lockMouseButton.addEventListener("click", async () => {
            await this.app.requestPointerLock(true)
        })
        this.buttonDiv.appendChild(this.lockMouseButton)

        // Pop up keyboard
        this.keyboardButton.innerText = I.stream.keyboard
        this.keyboardButton.addEventListener("click", async () => {
            setSidebarExtended(false)
            this.screenKeyboard.show()
        })
        this.buttonDiv.appendChild(this.keyboardButton)

        this.floatingKeyboardButton.innerText = "⌨×"
        this.floatingKeyboardButton.title = I.stream.hideKeyboard
        this.floatingKeyboardButton.ariaLabel = I.stream.hideKeyboard
        this.floatingKeyboardButton.classList.add("stream-keyboard-floating-button")
        this.floatingKeyboardButton.addEventListener("click", event => {
            event.preventDefault()
            event.stopPropagation()
            this.screenKeyboard.hide()
        })
        stopPropagationOn(this.floatingKeyboardButton)
        this.screenKeyboard.addKeyDownListener(this.onKeyDown.bind(this))
        this.screenKeyboard.addKeyUpListener(this.onKeyUp.bind(this))
        this.screenKeyboard.addTextListener(this.onText.bind(this))
        this.screenKeyboard.addKeyboardModeWillChangeListener(this.app.onScreenKeyboardModeWillChange.bind(this.app))
        this.screenKeyboard.addKeyboardModeListener(this.onKeyboardModeChange.bind(this))
        this.div.appendChild(this.screenKeyboard.getHiddenElement())


        // Fullscreen
        this.fullscreenButton.innerText = I.stream.fullscreen
        this.fullscreenButton.addEventListener("click", async () => {
            if (this.app.isFullscreen()) {
                this.app.markManualFullscreenExitRequested()
                await this.app.exitFullscreen()
            } else {
                await this.app.requestFullscreen()
            }
        })
        this.buttonDiv.appendChild(this.fullscreenButton)

        // Stats
        this.statsButton.innerText = I.stream.stats
        this.statsButton.addEventListener("click", () => {
            const stats = this.app.getStream()?.getStats()
            if (stats) {
                stats.toggle()
            }
        })
        this.buttonDiv.appendChild(this.statsButton)

        // Close stream
        this.exitStreamButton.innerText = I.stream.exit
        this.exitStreamButton.addEventListener("click", async () => {
            const stream = this.app.getStream()
            if (stream) {
                const success = await stream.stop()
                if (!success) {
                    console.debug("Failed to close stream correctly")
                }
            }

            if (window.matchMedia('(display-mode: standalone)').matches) {
                history.back()
            } else {
                window.close()
            }

        })
        this.buttonDiv.appendChild(this.exitStreamButton)

        // Select Mouse Mode
        this.mouseMode = new SelectComponent("mouseMode", [
            { value: "relative", name: I.stream.relative },
            { value: "follow", name: I.stream.follow },
            { value: "localCursor", name: I.stream.localCursor },
            { value: "pointAndDrag", name: I.stream.pointAndDrag }
        ], {
            displayName: I.stream.mouseMode,
            preSelectedOption: this.app.getInputConfig().mouseMode
        })
        this.mouseMode.addChangeListener(this.onMouseModeChange.bind(this))
        this.mouseMode.mount(this.div)

        // Select Touch Mode
        this.touchMode = new SelectComponent("touchMode", [
            { value: "touch", name: I.stream.touch },
            { value: "mouseRelative", name: I.stream.relative },
            { value: "localCursor", name: I.stream.localCursor },
            { value: "pointAndDrag", name: I.stream.pointAndDrag }
        ], {
            displayName: I.stream.touchMode,
            preSelectedOption: this.app.getInputConfig().touchMode
        })
        this.touchMode.addChangeListener(this.onTouchModeChange.bind(this))
        this.touchMode.mount(this.div)
    }

    onCapabilitiesChange(capabilities: StreamCapabilities) {
        this.touchMode.setOptionEnabled("touch", capabilities.touch)
    }

    getScreenKeyboard(): ScreenKeyboard {
        return this.screenKeyboard
    }

    // -- Keyboard
    private onText(event: TextEvent) {
        this.app.getStream()?.getInput().sendText(event.detail.text)
    }
    private onKeyDown(event: KeyboardEvent) {
        this.app.getStream()?.getInput().onKeyDown(event)
    }
    private onKeyUp(event: KeyboardEvent) {
        this.app.getStream()?.getInput().onKeyUp(event)
    }
    private onKeyboardModeChange(event: KeyboardModeEvent) {
        if (event.detail.enabled) {
            this.floatingKeyboardButton.classList.add("visible")
        } else {
            this.floatingKeyboardButton.classList.remove("visible")
            this.app.resetKeyboardViewportVideoOffset()
        }
    }

    // -- Mouse Mode
    private onMouseModeChange() {
        const config = this.app.getInputConfig()
        config.mouseMode = this.mouseMode.getValue() as any
        this.app.setInputConfig(config)
    }

    // -- Touch Mode
    private onTouchModeChange() {
        const config = this.app.getInputConfig()
        config.touchMode = this.touchMode.getValue() as any
        this.app.setInputConfig(config)
    }

    extended(): void {

    }
    unextend(): void {

    }

    mount(parent: HTMLElement): void {
        parent.appendChild(this.div)
        const appRoot = document.getElementById("root")
            ; (appRoot ?? document.body).appendChild(this.floatingKeyboardButton)
    }
    unmount(parent: HTMLElement): void {
        parent.removeChild(this.div)
        if (this.floatingKeyboardButton.parentElement) {
            this.floatingKeyboardButton.parentElement.removeChild(this.floatingKeyboardButton)
        }
    }
}

class SendKeycodeModal extends FormModal<number> {

    private dropdownSearch: SelectComponent

    constructor() {
        super()

        const keyList = []
        for (const keyNameRaw in StreamKeys) {
            const keyName = keyNameRaw as keyof typeof StreamKeys
            const keyValue = StreamKeys[keyName]

            const PREFIX = "VK_"

            let name: string = keyName
            if (name.startsWith(PREFIX)) {
                name = name.slice(PREFIX.length)
            }

            keyList.push({
                value: keyValue.toString(),
                name
            })
        }

        this.dropdownSearch = new SelectComponent("winKeycode", keyList, {
            hasSearch: true,
            displayName: I.stream.selectKeycode
        })
    }

    mountForm(form: HTMLFormElement): void {
        this.dropdownSearch.mount(form)
    }


    reset(): void {
        this.dropdownSearch.reset()
    }

    submit(): number | null {
        const keyString = this.dropdownSearch.getValue()
        if (keyString == null) {
            return null
        }

        return parseInt(keyString)
    }
}

// Stop propagation so the stream doesn't get it
function stopPropagationOn(element: HTMLElement) {
    element.addEventListener("keydown", onStopPropagation)
    element.addEventListener("keyup", onStopPropagation)
    element.addEventListener("keypress", onStopPropagation)
    element.addEventListener("click", onStopPropagation)
    element.addEventListener("mousedown", onStopPropagation)
    element.addEventListener("mouseup", onStopPropagation)
    element.addEventListener("mousemove", onStopPropagation)
    element.addEventListener("wheel", onStopPropagation)
    element.addEventListener("contextmenu", onStopPropagation)
    element.addEventListener("touchstart", onStopPropagation)
    element.addEventListener("touchmove", onStopPropagation)
    element.addEventListener("touchend", onStopPropagation)
    element.addEventListener("touchcancel", onStopPropagation)
    element.addEventListener("pointerdown", onStopPropagation)
    element.addEventListener("pointerup", onStopPropagation)
    element.addEventListener("pointermove", onStopPropagation)
    element.addEventListener("pointerrawupdate", onStopPropagation)
    element.addEventListener("pointercancel", onStopPropagation)
    element.addEventListener("lostpointercapture", onStopPropagation)
}
function onStopPropagation(event: Event) {
    event.stopPropagation()
}

function supportsUsablePointerRawUpdate(): boolean {
    if (!window.isSecureContext || !("onpointerrawupdate" in window)) {
        return false
    }

    // Firefox exposed pointerrawupdate before movementX/Y worked on those
    // events. Keep those releases on pointermove so relative input is not zero.
    const firefoxVersion = navigator.userAgent.match(/Firefox\/(\d+)/)?.[1]
    return firefoxVersion == null || Number.parseInt(firefoxVersion, 10) >= 148
}
