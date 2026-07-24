import { ControllerConfig } from "../stream/gamepad.js";
import { MouseMode, MouseScrollMode, TouchMode } from "../stream/input.js";
import type { PageStyle } from "../styles/index.js";
import { getLanguageOptions, getTranslations, Language, normalizeLanguage } from "../i18n.js";
import { Component, ComponentEvent } from "./index.js";
import { InputComponent, SelectComponent } from "./input.js";
import { SidebarEdge } from "./sidebar/index.js";
import {
    compareOutboxEnvelopes,
    createOutboxEnvelope,
    isSettingsObject,
    listOutboxEntries,
    newestOutboxEntry,
    OutboxEntry,
    removeOutboxEnvelope,
    SettingsObject,
    writeOutboxEnvelope,
} from "../settings_outbox.js";

type UserSettings = SettingsObject

export type UserSettingsSnapshot = {
    settings: UserSettings | null,
    revision: number,
}

export type UserSettingsSaveResult = {
    revision: number,
    applied: boolean,
}

export type Settings = {
    sidebarEdge: SidebarEdge,
    bitrate: number
    adaptiveBitrate: boolean
    minimumBitrate: number
    videoFrameQueueSize: number
    videoSize: "720p" | "1080p" | "1440p" | "4k" | "native" | "custom"
    videoSizeCustom: {
        width: number
        height: number
    },
    fps: number
    videoCodec: StreamCodec,
    forceVideoElementRenderer: boolean
    canvasRenderer: boolean
    canvasVsync: boolean
    playAudioLocal: boolean
    encryptHostVideo: boolean
    encryptHostAudio: boolean
    audioSampleQueueSize: number
    mouseScrollMode: MouseScrollMode
    mouseMode: MouseMode
    touchMode: TouchMode
    localCursorSensitivity: number
    controllerConfig: ControllerConfig
    dataTransport: TransportType
    language: Language
    enterFullscreenOnStreamStart: boolean
    toggleFullscreenWithKeybind: boolean
    pageStyle: PageStyle
    hdr: boolean
    useSelectElementPolyfill: boolean
}

export type StreamCodec = "h264" | "auto" | "h265" | "av1"
export type TransportType = "auto" | "webtransport" | "webrtc" | "websocket"

import DEFAULT_SETTINGS from "../default_settings.js"
import { StreamPermissions } from "../api_bindings.js";

/// You should use the role default settings instead!
export function globalDefaultSettings(): Settings {
    // We are deep cloning this
    return deepClone(DEFAULT_SETTINGS)
}

function deepClone<T>(value: T): T {
    if (typeof structuredClone == "function") {
        return structuredClone(value)
    } else {
        return JSON.parse(JSON.stringify(value))
    }
}
function deepMerge(target: any, source: any) {
    if (!source || typeof source !== "object" || Array.isArray(source)) {
        return target
    }

    for (const key in source) {
        if (key === "__proto__" || key === "constructor" || key === "prototype") {
            continue
        }

        const sourceVal = source[key]
        const targetVal = target[key]

        if (
            sourceVal &&
            typeof sourceVal === "object" &&
            !Array.isArray(sourceVal)
        ) {
            target[key] = deepMerge(
                targetVal && typeof targetVal === "object" ? targetVal : {},
                sourceVal
            )
        } else if (sourceVal !== undefined) {
            target[key] = sourceVal
        }
    }
    return target
}

const LEGACY_SETTINGS_KEY = "mlSettings"
const LEGACY_OWNER_KEY = "mlSettings:legacy-owner"
const LEGACY_CLAIMED_KEY = "mlSettings:legacy-claimed"
const USER_SETTINGS_KEY_PREFIX = "mlSettings:user:"
const SAVE_RETRY_INITIAL_MS = 1000
const SAVE_RETRY_MAX_MS = 30000
// Must match the server's persisted idempotency window.
const SETTINGS_MUTATION_HISTORY_SIZE = 32
const FALLBACK_LOCK_EXPIRY_MS = 30000
const FALLBACK_LOCK_POLL_MS = 25
const SETTINGS_TAB_ID = createUniqueToken()
let settingsTokenCounter = 0

type QueuedSettings = {
    entry: OutboxEntry,
    persisted: boolean,
}

type SettingsSyncState = {
    save: (patch: UserSettings, mutationId: string) => Promise<UserSettingsSaveResult>,
    isRetryable: (error: unknown) => boolean,
    defaultSettings: Settings,
    serverRevision: number,
    storageKey: string,
    revisionKey: string,
    outboxKey: string,
    lockName: string,
    fallbackLockPrefix: string,
    mirrorBootstrap: boolean,
    pending: QueuedSettings[],
    retry: QueuedSettings | null,
    retryDelayMs: number,
    retryTimer: number | null,
    flushing: boolean,
    failureNotified: boolean,
    blockedTokens: Set<string>,
    waiters: Array<(saved: boolean) => void>,
    onFailure?: (error: unknown) => void,
}

let activeUserSettings: UserSettings | null = null
let activeEffectiveSettings: Settings | null = null
let lastSubmittedEffectiveSettings: Settings | null = null
let settingsSyncState: SettingsSyncState | null = null
let onlineRetryInstalled = false

function isUserSettings(value: unknown): value is UserSettings {
    return isSettingsObject(value)
}

function readStoredSettings(key: string): UserSettings | null {
    try {
        const json = localStorage.getItem(key)
        if (!json) {
            return null
        }

        const parsed = JSON.parse(json)
        if (!isUserSettings(parsed)) {
            localStorage.removeItem(key)
            return null
        }
        return parsed
    } catch {
        try {
            localStorage.removeItem(key)
        } catch {
            // Storage can be unavailable in restrictive/private browser modes.
        }
        return null
    }
}

function writeStoredSettings(key: string, settings: UserSettings): boolean {
    try {
        localStorage.setItem(key, JSON.stringify(settings))
        return true
    } catch {
        return false
    }
}

function removeStoredSettings(key: string): boolean {
    try {
        localStorage.removeItem(key)
        return true
    } catch {
        return false
    }
}

function readStoredRevision(key: string): number | null {
    try {
        const value = Number.parseInt(localStorage.getItem(key) ?? "", 10)
        return Number.isSafeInteger(value) && value >= 0 ? value : null
    } catch {
        return null
    }
}

function writeStoredRevision(key: string, revision: number): boolean {
    try {
        localStorage.setItem(key, String(revision))
        return true
    } catch {
        return false
    }
}

function safeListOutboxEntries(outboxKey: string): OutboxEntry[] {
    try {
        return listOutboxEntries(localStorage, outboxKey)
    } catch {
        return []
    }
}

function orderedUniqueOutboxEntries(entries: OutboxEntry[]): OutboxEntry[] {
    const byToken = new Map<string, OutboxEntry>()
    for (const entry of entries) {
        byToken.set(entry.envelope.token, entry)
    }
    return [...byToken.values()].sort((a, b) =>
        compareOutboxEnvelopes(a.envelope, b.envelope)
    )
}

function safeWriteOutboxEnvelope(key: string, envelope: OutboxEntry["envelope"]): boolean {
    try {
        return writeOutboxEnvelope(localStorage, key, envelope)
    } catch {
        return false
    }
}

function safeRemoveOutboxEnvelope(key: string, expectedToken: string): boolean {
    try {
        return removeOutboxEnvelope(localStorage, key, expectedToken)
    } catch {
        return false
    }
}

function createUniqueToken(): string {
    try {
        if (typeof crypto.randomUUID === "function") {
            return crypto.randomUUID()
        }
    } catch {
        // Fall through for older embedded browsers.
    }
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
}

function createSettingsToken(): string {
    settingsTokenCounter += 1
    return `${SETTINGS_TAB_ID}-${Date.now().toString(36)}-${settingsTokenCounter.toString(36)}`
}

function mergeStreamSettings(defaultSettings: Settings, userSettings?: UserSettings | null): Settings {
    // Start with FULL global defaults
    let settings = globalDefaultSettings()

    // Fill/override with role defaults (even if partial)
    settings = deepMerge(settings, defaultSettings)

    if (userSettings) {
        // Finally override with user settings
        settings = deepMerge(settings, userSettings)
    }

    // Migration
    if (settings?.pageStyle === "old") {
        settings.pageStyle = "moonlight"
    }
    if (settings?.dataTransport === "webrtc") {
        settings.dataTransport = "auto"
    }

    return settings
}

function settingsValuesEqual(a: unknown, b: unknown): boolean {
    if (Object.is(a, b)) {
        return true
    }
    if (
        a === null
        || b === null
        || typeof a !== "object"
        || typeof b !== "object"
        || Array.isArray(a) !== Array.isArray(b)
    ) {
        return false
    }

    if (Array.isArray(a) && Array.isArray(b)) {
        return a.length === b.length && a.every((value, index) => settingsValuesEqual(value, b[index]))
    }

    const aObject = a as Record<string, unknown>
    const bObject = b as Record<string, unknown>
    const aKeys = Object.keys(aObject)
    const bKeys = Object.keys(bObject)
    return aKeys.length === bKeys.length
        && aKeys.every(key => Object.prototype.hasOwnProperty.call(bObject, key)
            && settingsValuesEqual(aObject[key], bObject[key]))
}

function isUnsafeSettingsKey(key: string): boolean {
    return key === "__proto__" || key === "constructor" || key === "prototype"
}

function createSettingsPatch(previous: Settings | null, next: Settings): UserSettings {
    if (previous === null) {
        return deepClone(next) as unknown as UserSettings
    }

    const patch: UserSettings = {}
    const previousObject = previous as unknown as UserSettings
    const nextObject = next as unknown as UserSettings
    for (const key of new Set([...Object.keys(previousObject), ...Object.keys(nextObject)])) {
        if (isUnsafeSettingsKey(key)) {
            continue
        }
        if (settingsValuesEqual(previousObject[key], nextObject[key])) {
            continue
        }
        patch[key] = Object.prototype.hasOwnProperty.call(nextObject, key)
            ? deepClone(nextObject[key])
            : null
    }
    return patch
}

function applySettingsPatch(settings: UserSettings | null, patch: UserSettings): UserSettings {
    const merged = settings ? deepClone(settings) : {}
    for (const key in patch) {
        if (isUnsafeSettingsKey(key)) {
            continue
        }
        const value = patch[key]
        if (value === null) {
            delete merged[key]
        } else {
            merged[key] = deepClone(value)
        }
    }
    return merged
}

/**
 * Selects the authenticated user's local cache and server sync target.
 * `serverSnapshot === undefined` means the GET failed. A snapshot containing
 * `settings: null` means this account has no server-side overrides yet.
 */
export async function initializeUserStreamSettings(
    userIdentity: string | number,
    defaultSettings: Settings,
    serverSnapshot: UserSettingsSnapshot | undefined,
    save: (patch: UserSettings, mutationId: string) => Promise<UserSettingsSaveResult>,
    load: () => Promise<UserSettingsSnapshot>,
    isRetryable: (error: unknown) => boolean = () => true,
    onFailure?: (error: unknown) => void,
): Promise<Settings> {
    const userIdentityString = String(userIdentity)
    const storageKey = `${USER_SETTINGS_KEY_PREFIX}${userIdentityString}`
    const revisionKey = `${storageKey}:server-revision`
    const outboxKey = `${storageKey}:pending`
    let cachedSettings = readStoredSettings(storageKey)
    let persistedEntries = orderedUniqueOutboxEntries(safeListOutboxEntries(outboxKey))
    let legacyClaimed = false

    // The old key was shared by every account on an origin. Claim it under one
    // origin-wide lock so two accounts opening in different tabs cannot both
    // migrate and upload the same legacy snapshot.
    await withCrossTabSettingsLock(
        "moonlight-legacy-settings-migration",
        `${LEGACY_SETTINGS_KEY}:migration-lock:`,
        async () => {
            try {
                let legacyOwner = localStorage.getItem(LEGACY_OWNER_KEY)
                if (legacyOwner === null) {
                    localStorage.setItem(LEGACY_OWNER_KEY, userIdentityString)
                    legacyOwner = localStorage.getItem(LEGACY_OWNER_KEY)
                }

                if (legacyOwner === userIdentityString && localStorage.getItem(LEGACY_CLAIMED_KEY) !== "true") {
                    if (cachedSettings === null) {
                        const legacySettings = readStoredSettings(LEGACY_SETTINGS_KEY)
                        if (legacySettings !== null) {
                            cachedSettings = legacySettings
                            // The in-memory copy can still seed the server if quota
                            // prevents creating the new per-user cache key.
                            writeStoredSettings(storageKey, legacySettings)
                        }
                    }

                    // `mlSettings` becomes a mutable language/theme bootstrap mirror
                    // below, so it must never be considered migration input again.
                    localStorage.setItem(LEGACY_CLAIMED_KEY, "true")
                }
                legacyClaimed = localStorage.getItem(LEGACY_CLAIMED_KEY) === "true"
            } catch {
                // The app can still use server settings when browser storage is blocked.
            }
        },
    )

    let selectedSettings: UserSettings | null
    if (serverSnapshot?.settings !== undefined && serverSnapshot.settings !== null) {
        // A non-null server response is always authoritative over this device's
        // cache, allowing account settings to follow the user to other origins.
        selectedSettings = deepClone(serverSnapshot.settings)
        writeStoredSettings(storageKey, selectedSettings)
    } else if (serverSnapshot?.settings === null && serverSnapshot.revision > 0) {
        // A positive revision with null settings is an intentional reset and
        // must clear stale device caches. Revision zero is the legacy/no-value
        // case where a one-time local migration can still seed the account.
        selectedSettings = null
        removeStoredSettings(storageKey)
    } else {
        selectedSettings = cachedSettings
    }

    const serverRevision = serverSnapshot?.revision ?? readStoredRevision(revisionKey) ?? 0
    if (serverSnapshot !== undefined) {
        writeStoredRevision(revisionKey, serverSnapshot.revision)
    }

    if (serverSnapshot !== undefined && serverRevision > SETTINGS_MUTATION_HISTORY_SIZE) {
        const oldestDeduplicatedBase = serverRevision - SETTINGS_MUTATION_HISTORY_SIZE
        const staleTokens = new Set(
            persistedEntries
                .filter(entry => entry.envelope.baseRevision < oldestDeduplicatedBase)
                .map(entry => entry.envelope.token),
        )
        if (staleTokens.size > 0) {
            // Once a mutation is older than the server's bounded ID history,
            // replay can no longer prove whether it already committed. Prefer
            // the newer server state rather than risk reverting another device.
            for (const entry of safeListOutboxEntries(outboxKey)) {
                if (staleTokens.has(entry.envelope.token)) {
                    safeRemoveOutboxEnvelope(entry.key, entry.envelope.token)
                }
            }
            persistedEntries = persistedEntries.filter(
                entry => !staleTokens.has(entry.envelope.token),
            )
        }
    }
    const persistedOutbox = persistedEntries[0] ?? null

    // Keep every locally journaled edit visible while it is reconciled. The
    // server deduplicates mutation IDs; after a successful drain we refetch the
    // authoritative snapshot so an already-applied mutation cannot mask a
    // newer change from another device.
    for (const entry of persistedEntries) {
        selectedSettings = applySettingsPatch(selectedSettings, entry.envelope.patch)
    }
    if (persistedEntries.length > 0 && selectedSettings !== null) {
        writeStoredSettings(storageKey, selectedSettings)
    }

    activeUserSettings = selectedSettings ? deepClone(selectedSettings) : {}
    activeEffectiveSettings = mergeStreamSettings(defaultSettings, activeUserSettings)
    lastSubmittedEffectiveSettings = deepClone(activeEffectiveSettings)
    settingsSyncState = {
        save,
        isRetryable,
        defaultSettings: deepClone(defaultSettings),
        serverRevision,
        storageKey,
        revisionKey,
        outboxKey,
        lockName: `moonlight-user-settings:${userIdentityString}`,
        fallbackLockPrefix: `${outboxKey}:lock:`,
        mirrorBootstrap: legacyClaimed,
        pending: [],
        retry: null,
        retryDelayMs: SAVE_RETRY_INITIAL_MS,
        retryTimer: null,
        flushing: false,
        failureNotified: false,
        blockedTokens: new Set(),
        waiters: [],
        onFailure,
    }

    if (!onlineRetryInstalled) {
        window.addEventListener("online", retryUnsavedSettings)
        onlineRetryInstalled = true
    }

    // Mutation IDs are persisted with the settings on the server, so replaying
    // an unload-time PATCH is safe even when it actually committed before the
    // browser closed. Wait for this one reconciliation before rendering so a
    // genuinely unsent edit is visible immediately.
    if (persistedOutbox !== null) {
        const reconciled = await queueServerSave(persistedOutbox.envelope.patch, persistedOutbox)
        if (reconciled) {
            try {
                const refreshed = await load()
                if (settingsSyncState !== null) {
                    settingsSyncState.serverRevision = refreshed.revision
                    writeStoredRevision(settingsSyncState.revisionKey, refreshed.revision)
                }
                selectedSettings = refreshed.settings ? deepClone(refreshed.settings) : null
                for (const entry of orderedUniqueOutboxEntries(safeListOutboxEntries(outboxKey))) {
                    selectedSettings = applySettingsPatch(selectedSettings, entry.envelope.patch)
                }
                activeUserSettings = selectedSettings ? deepClone(selectedSettings) : {}
                activeEffectiveSettings = mergeStreamSettings(defaultSettings, activeUserSettings)
                if (selectedSettings === null) {
                    removeStoredSettings(storageKey)
                } else {
                    writeStoredSettings(storageKey, selectedSettings)
                }
            } catch (error) {
                console.warn("Saved pending settings but could not refresh them", error)
            }
        }
    } else if (serverSnapshot?.settings === null && selectedSettings !== null) {
        void queueServerSave(selectedSettings)
    }

    const effectiveSettings = mergeStreamSettings(defaultSettings, activeUserSettings)
    activeEffectiveSettings = deepClone(effectiveSettings)
    lastSubmittedEffectiveSettings = deepClone(effectiveSettings)
    // Language/theme modules execute before authentication. Mirror only the
    // active user's effective snapshot to the legacy bootstrap key; the owner
    // marker above prevents that key from migrating into another account.
    if (legacyClaimed) {
        writeStoredSettings(LEGACY_SETTINGS_KEY, effectiveSettings as unknown as UserSettings)
    }
    return effectiveSettings
}

export function getLocalStreamSettings(defaultSettings: Settings): Settings {
    // Before authentication (for example, the login modal), retain the old
    // best-effort behavior. Home and stream initialize an account cache first.
    if (activeEffectiveSettings !== null) {
        return deepClone(activeEffectiveSettings)
    }

    const settings = readStoredSettings(LEGACY_SETTINGS_KEY)
    return mergeStreamSettings(defaultSettings, settings)
}

export function setLocalStreamSettings(settings?: Settings): Promise<boolean> {
    if (!settings) {
        return Promise.resolve(false)
    }

    // This baseline follows what this tab's controls last submitted. It is
    // intentionally separate from `activeEffectiveSettings`, which may adopt
    // another tab's cache while this tab's form still displays older values.
    const previousSettings = lastSubmittedEffectiveSettings ?? activeEffectiveSettings
    const effectiveSnapshot = deepClone(settings)
    const patch = createSettingsPatch(previousSettings, effectiveSnapshot)
    activeEffectiveSettings = effectiveSnapshot
    lastSubmittedEffectiveSettings = deepClone(effectiveSnapshot)

    if (Object.keys(patch).length === 0) {
        return Promise.resolve(true)
    }

    if (settingsSyncState) {
        return queueServerSave(patch)
    } else {
        // Compatibility for pages that do not establish an authenticated sync
        // context (currently the administration page).
        const snapshot = deepClone(settings) as unknown as UserSettings
        activeUserSettings = snapshot
        writeStoredSettings(LEGACY_SETTINGS_KEY, snapshot)
        return Promise.resolve(true)
    }
}

function queueServerSave(patch: UserSettings, existingEntry?: OutboxEntry): Promise<boolean> {
    const state = settingsSyncState
    if (!state) {
        return Promise.resolve(false)
    }

    let queued: QueuedSettings
    if (existingEntry) {
        queued = { entry: existingEntry, persisted: true }
    } else {
        const candidates = safeListOutboxEntries(state.outboxKey)
        candidates.push(...state.pending.map(value => value.entry))
        if (state.retry) {
            candidates.push(state.retry.entry)
        }

        const newest = newestOutboxEntry(candidates)
        const baseRevision = newest?.envelope.baseRevision
            ?? readStoredRevision(state.revisionKey)
            ?? state.serverRevision
        const token = createSettingsToken()
        const preciseNow = typeof performance !== "undefined"
            && Number.isFinite(performance.timeOrigin)
            ? performance.timeOrigin + performance.now()
            : Date.now()
        const envelope = createOutboxEnvelope(
            deepClone(patch),
            baseRevision,
            candidates,
            token,
            preciseNow,
        )
        const entry = {
            key: `${state.outboxKey}:draft:${token}`,
            envelope,
        }
        queued = {
            entry,
            // Journal synchronously before returning. A user can change a
            // setting and navigate immediately without losing the edit while
            // another tab owns the network lock.
            persisted: safeWriteOutboxEnvelope(entry.key, envelope),
        }

        const latestSettings = readStoredSettings(state.storageKey) ?? activeUserSettings
        const mergedSettings = applySettingsPatch(latestSettings, patch)
        const effectiveSettings = mergeStreamSettings(state.defaultSettings, mergedSettings)
        activeUserSettings = deepClone(mergedSettings)
        activeEffectiveSettings = deepClone(effectiveSettings)
        const wroteUserCache = writeStoredSettings(state.storageKey, mergedSettings)
        const wroteBootstrapCache = !state.mirrorBootstrap
            || writeStoredSettings(
                LEGACY_SETTINGS_KEY,
                effectiveSettings as unknown as UserSettings,
            )
        if (!queued.persisted || !wroteUserCache || !wroteBootstrapCache) {
            notifySaveFailure(state, new Error("Browser storage is unavailable"))
        }
    }

    const saved = new Promise<boolean>(resolve => state.waiters.push(resolve))

    if (state.retry !== null) {
        state.pending.unshift(state.retry)
    }
    state.pending.push(queued)
    state.retry = null
    state.retryDelayMs = SAVE_RETRY_INITIAL_MS
    if (state.retryTimer !== null) {
        window.clearTimeout(state.retryTimer)
        state.retryTimer = null
    }
    void flushServerSaves(state)
    return saved
}

async function flushServerSaves(state: SettingsSyncState) {
    if (state.flushing) {
        return
    }

    state.flushing = true
    let attempted: QueuedSettings | null = null
    try {
        await withSettingsLock(state, async refreshFallbackLock => {
            while (true) {
                const entries = safeListOutboxEntries(state.outboxKey)
                const persistedTokens = new Set(entries.map(entry => entry.envelope.token))
                state.pending = state.pending.filter(value =>
                    !value.persisted || persistedTokens.has(value.entry.envelope.token)
                )
                if (
                    state.retry?.persisted
                    && !persistedTokens.has(state.retry.entry.envelope.token)
                ) {
                    state.retry = null
                }

                const memory = state.retry ? [state.retry, ...state.pending] : [...state.pending]
                const candidate = selectNextSaveCandidate(
                    memory,
                    entries,
                    state.blockedTokens,
                )
                if (!candidate) {
                    return
                }
                const candidateToken = candidate.entry.envelope.token
                state.pending = state.pending.filter(value => value.entry.envelope.token !== candidateToken)
                if (state.retry?.entry.envelope.token === candidateToken) {
                    state.retry = null
                }

                attempted = candidate
                refreshFallbackLock()
                const result = await state.save(
                    candidate.entry.envelope.patch,
                    candidate.entry.envelope.token,
                )
                state.serverRevision = result.revision
                writeStoredRevision(state.revisionKey, result.revision)
                state.retryDelayMs = SAVE_RETRY_INITIAL_MS
                state.failureNotified = false

                // Remove only this exact mutation. Each draft has its own
                // idempotency token and must reach the server in order; folding
                // an uncertain earlier request into a new token could reapply
                // stale values after the first request actually committed.
                for (const entry of safeListOutboxEntries(state.outboxKey)) {
                    if (entry.envelope.token === candidateToken) {
                        safeRemoveOutboxEnvelope(entry.key, entry.envelope.token)
                    }
                }

                attempted = null
                // Loop while a newer draft was created during the PATCH.
                if (
                    state.pending.length === 0
                    && state.retry === null
                    && safeListOutboxEntries(state.outboxKey).length === 0
                ) {
                    return
                }
            }
        })
    } catch (error) {
        // Prefer a newer edit that arrived while this request was in flight.
        const failedAttempt = attempted as QueuedSettings | null
        const retryable = state.isRetryable(error)
        state.retry = retryable ? failedAttempt ?? state.retry : null
        if (!retryable && failedAttempt !== null) {
            // Keep the journal for a future authenticated page load, but do
            // not let a permanent 4xx response poison every later edit in the
            // current tab.
            state.blockedTokens.add(failedAttempt.entry.envelope.token)
        }
        notifySaveFailure(state, error)
        settleSaveWaiters(state, false)
        if (retryable) {
            scheduleSaveRetry(state)
        }
    } finally {
        state.flushing = false
        // Close the small race where an edit arrives after the loop condition
        // but before `flushing` is reset.
        const hasUnblockedJournal = safeListOutboxEntries(state.outboxKey)
            .some(entry => !state.blockedTokens.has(entry.envelope.token))
        if (
            state.retry === null
            && (state.pending.length > 0 || hasUnblockedJournal)
        ) {
            void flushServerSaves(state)
        } else if (state.retry === null) {
            settleSaveWaiters(state, true)
        }
    }
}

function selectNextSaveCandidate(
    memory: QueuedSettings[],
    entries: OutboxEntry[],
    blockedTokens: Set<string>,
): QueuedSettings | null {
    const candidates = new Map<string, QueuedSettings>()
    for (const entry of entries) {
        if (blockedTokens.has(entry.envelope.token)) {
            continue
        }
        candidates.set(entry.envelope.token, {
            entry,
            persisted: true,
        })
    }
    for (const value of memory) {
        if (blockedTokens.has(value.entry.envelope.token)) {
            continue
        }
        if (!value.persisted || candidates.has(value.entry.envelope.token)) {
            candidates.set(value.entry.envelope.token, value)
        }
    }

    let oldest: QueuedSettings | null = null
    for (const candidate of candidates.values()) {
        if (
            oldest === null
            || compareOutboxEnvelopes(candidate.entry.envelope, oldest.entry.envelope) < 0
        ) {
            oldest = candidate
        }
    }
    return oldest
}

type LockRefresh = () => void

async function withSettingsLock<T>(
    state: SettingsSyncState,
    action: (refresh: LockRefresh) => Promise<T>,
): Promise<T> {
    return await withCrossTabSettingsLock(
        state.lockName,
        state.fallbackLockPrefix,
        action,
    )
}

async function withCrossTabSettingsLock<T>(
    lockName: string,
    fallbackLockPrefix: string,
    action: (refresh: LockRefresh) => Promise<T>,
): Promise<T> {
    const lockManager = (navigator as Navigator & {
        locks?: { request: Function },
    }).locks
    if (lockManager && typeof lockManager.request === "function") {
        return await lockManager.request(
            lockName,
            { mode: "exclusive" },
            () => action(() => { }),
        )
    }

    return await withFallbackSettingsLock(fallbackLockPrefix, action)
}

type FallbackTicket = {
    owner: string,
    ticket: number,
    expiresAt: number,
}

async function withFallbackSettingsLock<T>(
    fallbackLockPrefix: string,
    action: (refresh: LockRefresh) => Promise<T>,
): Promise<T> {
    const owner = createUniqueToken()
    const choosingKey = `${fallbackLockPrefix}choosing:${owner}`
    const ticketKey = `${fallbackLockPrefix}ticket:${owner}`
    let lockAcquired = false
    let heartbeatTimer: number | null = null

    try {
        cleanupExpiredFallbackLocks(fallbackLockPrefix)
        localStorage.setItem(choosingKey, JSON.stringify({
            expiresAt: Date.now() + FALLBACK_LOCK_EXPIRY_MS,
        }))

        const ticket = Math.max(0, ...readFallbackTickets(fallbackLockPrefix).map(value => value.ticket)) + 1
        let ownTicket: FallbackTicket = {
            owner,
            ticket,
            expiresAt: Date.now() + FALLBACK_LOCK_EXPIRY_MS,
        }
        localStorage.setItem(ticketKey, JSON.stringify(ownTicket))
        localStorage.removeItem(choosingKey)

        let lastRefresh = Date.now()
        while (true) {
            cleanupExpiredFallbackLocks(fallbackLockPrefix)
            const hasChoosingPeer = listStorageKeys(`${fallbackLockPrefix}choosing:`)
                .some(key => key !== choosingKey)
            const hasEarlierTicket = readFallbackTickets(fallbackLockPrefix).some(other =>
                other.owner !== owner
                && (other.ticket < ticket || (other.ticket === ticket && other.owner < owner))
            )

            if (!hasChoosingPeer && !hasEarlierTicket) {
                break
            }

            if (Date.now() - lastRefresh >= 5000) {
                ownTicket = { ...ownTicket, expiresAt: Date.now() + FALLBACK_LOCK_EXPIRY_MS }
                localStorage.setItem(ticketKey, JSON.stringify(ownTicket))
                lastRefresh = Date.now()
            }
            await waitForFallbackLock()
        }

        const refresh = () => {
            ownTicket = { ...ownTicket, expiresAt: Date.now() + FALLBACK_LOCK_EXPIRY_MS }
            localStorage.setItem(ticketKey, JSON.stringify(ownTicket))
        }
        refresh()
        lockAcquired = true
        heartbeatTimer = window.setInterval(() => {
            try {
                refresh()
            } catch {
                // The in-flight save still has its per-tab serialization. If
                // storage has disappeared, the fallback lock cannot be renewed.
            }
        }, Math.max(1000, Math.floor(FALLBACK_LOCK_EXPIRY_MS / 3)))
        return await action(refresh)
    } catch (error) {
        if (lockAcquired) {
            throw error
        }
        // Storage-disabled browsers cannot coordinate across tabs. The existing
        // per-tab queue still serializes locally and preserves the network retry.
        console.warn("Cross-tab settings lock unavailable; using this tab's queue", error)
        return await action(() => { })
    } finally {
        if (heartbeatTimer !== null) {
            window.clearInterval(heartbeatTimer)
        }
        try {
            localStorage.removeItem(choosingKey)
            localStorage.removeItem(ticketKey)
        } catch {
            // Storage was unavailable; there is no lock record to clean up.
        }
    }
}

function listStorageKeys(prefix: string): string[] {
    const keys: string[] = []
    for (let index = 0; index < localStorage.length; index++) {
        const key = localStorage.key(index)
        if (key?.startsWith(prefix)) {
            keys.push(key)
        }
    }
    return keys
}

function readFallbackTickets(prefix: string): FallbackTicket[] {
    const now = Date.now()
    const tickets: FallbackTicket[] = []
    for (const key of listStorageKeys(`${prefix}ticket:`)) {
        try {
            const ticket = JSON.parse(localStorage.getItem(key) ?? "null") as FallbackTicket | null
            if (
                ticket
                && typeof ticket.owner === "string"
                && Number.isSafeInteger(ticket.ticket)
                && ticket.ticket > 0
                && Number.isFinite(ticket.expiresAt)
                && ticket.expiresAt > now
            ) {
                tickets.push(ticket)
            }
        } catch {
            // Expired/malformed records are removed by cleanup below.
        }
    }
    return tickets
}

function cleanupExpiredFallbackLocks(prefix: string) {
    const now = Date.now()
    for (const key of listStorageKeys(prefix)) {
        try {
            const value = JSON.parse(localStorage.getItem(key) ?? "null") as { expiresAt?: unknown } | null
            if (!value || !Number.isFinite(value.expiresAt) || Number(value.expiresAt) <= now) {
                localStorage.removeItem(key)
            }
        } catch {
            localStorage.removeItem(key)
        }
    }
}

function waitForFallbackLock(): Promise<void> {
    return new Promise(resolve => window.setTimeout(resolve, FALLBACK_LOCK_POLL_MS))
}

function retryUnsavedSettings(event?: Event) {
    const state = settingsSyncState
    if (!state || state.retry === null) {
        return
    }

    if (event) {
        state.retryDelayMs = SAVE_RETRY_INITIAL_MS
    }
    if (state.retryTimer !== null) {
        window.clearTimeout(state.retryTimer)
        state.retryTimer = null
    }
    state.pending.unshift(state.retry)
    state.retry = null
    void flushServerSaves(state)
}

function scheduleSaveRetry(state: SettingsSyncState) {
    if (state.retryTimer !== null) {
        return
    }

    const delay = state.retryDelayMs
    state.retryDelayMs = Math.min(SAVE_RETRY_MAX_MS, delay * 2)
    state.retryTimer = window.setTimeout(() => {
        state.retryTimer = null
        retryUnsavedSettings()
    }, delay)
}

function notifySaveFailure(state: SettingsSyncState, error: unknown) {
    console.error("Failed to save account settings", error)
    if (!state.failureNotified) {
        state.failureNotified = true
        try {
            state.onFailure?.(error)
        } catch (notificationError) {
            console.error("Failed to show the settings save error", notificationError)
        }
    }
}

function settleSaveWaiters(state: SettingsSyncState, saved: boolean) {
    const waiters = state.waiters.splice(0)
    for (const resolve of waiters) {
        resolve(saved)
    }
}

export type StreamSettingsChangeListener = (event: ComponentEvent<StreamSettingsComponent>) => void

function makeSettingsValid(permissions: StreamPermissions, settings: Settings) {
    // This low-latency fork no longer auto-selects or exposes WebRTC. Migrate a
    // previously saved WebRTC choice to the new WebTransport-first Auto mode.
    if (settings.dataTransport == "webrtc") {
        settings.dataTransport = "auto"
    }

    if (!Number.isFinite(settings.bitrate) || settings.bitrate <= 0) {
        settings.bitrate = globalDefaultSettings().bitrate
    }
    settings.bitrate = Math.trunc(settings.bitrate)

    if (permissions.maximum_bitrate_kbps != null && permissions.maximum_bitrate_kbps < settings.bitrate) {
        settings.bitrate = permissions.maximum_bitrate_kbps
    }

    if (!Number.isFinite(settings.minimumBitrate)) {
        settings.minimumBitrate = globalDefaultSettings().minimumBitrate
    }
    const adaptiveFloor = Math.min(500, settings.bitrate)
    settings.minimumBitrate = Math.min(
        settings.bitrate,
        Math.max(adaptiveFloor, Math.trunc(settings.minimumBitrate)),
    )

    if (!permissions.allow_codec_av1 && settings.videoCodec == "av1") {
        settings.videoCodec = "h265"
    }
    if (!permissions.allow_codec_h265 && settings.videoCodec == "h265") {
        settings.videoCodec = "h264"
    }
    if (!permissions.allow_codec_h264 && settings.videoCodec == "h264") {
        settings.videoCodec = "auto"
    }

    if (!permissions.allow_hdr && settings.hdr) {
        settings.hdr = false
    }

    if (!permissions.allow_transport_websockets && settings.dataTransport == "websocket") {
        settings.dataTransport = "auto"
    }
    if (!permissions.allow_transport_websockets && settings.dataTransport == "webtransport") {
        settings.dataTransport = "auto"
    }

    if (!Number.isFinite(settings.localCursorSensitivity) || settings.localCursorSensitivity <= 0) {
        settings.localCursorSensitivity = globalDefaultSettings().localCursorSensitivity
    }
}

export class StreamSettingsComponent implements Component {

    private permissions: StreamPermissions

    private divElement: HTMLDivElement = document.createElement("div")

    private sidebarHeader: HTMLHeadingElement = document.createElement("h3")
    private sidebarEdge: SelectComponent

    private networkHeader: HTMLHeadingElement = document.createElement("h3")
    private streamHeader: HTMLHeadingElement = document.createElement("h3")
    private bitrate: InputComponent
    private adaptiveBitrate: InputComponent
    private minimumBitrate: InputComponent
    private fps: InputComponent
    private videoCodec: SelectComponent
    private forceVideoElementRenderer: InputComponent
    private canvasRenderer: InputComponent
    private canvasVsync: InputComponent
    private hdr: InputComponent

    private videoSize: SelectComponent
    private videoSizeWidth: InputComponent
    private videoSizeHeight: InputComponent

    private videoSampleQueueSize: InputComponent

    private audioHeader: HTMLHeadingElement = document.createElement("h3")
    private playAudioLocal: InputComponent
    private audioSampleQueueSize: InputComponent

    private mouseHeader: HTMLHeadingElement = document.createElement("h3")
    private mouseScrollMode: SelectComponent
    private mouseMode: SelectComponent
    private touchMode: SelectComponent
    private localCursorSensitivity: InputComponent

    private controllerHeader: HTMLHeadingElement = document.createElement("h3")
    private controllerInvertAB: InputComponent
    private controllerInvertXY: InputComponent
    private controllerSendIntervalOverride: InputComponent

    private otherHeader: HTMLHeadingElement = document.createElement("h3")
    private dataTransport: SelectComponent
    private securityHeader: HTMLHeadingElement = document.createElement("h3")
    private securityWarning: HTMLParagraphElement = document.createElement("p")
    private encryptHostVideo: InputComponent
    private encryptHostAudio: InputComponent
    private language: SelectComponent
    private enterFullscreenOnStreamStart: InputComponent
    private toggleFullscreenWithKeybind: InputComponent

    private pageStyle: SelectComponent

    private useSelectElementPolyfill: InputComponent

    constructor(permissions: StreamPermissions, settings: Settings) {
        // Sometimes the normal settings object doesn't have some values, because they change between versions.
        // Use those as fallback
        const defaultSettings_ = globalDefaultSettings()

        makeSettingsValid(permissions, defaultSettings_)
        makeSettingsValid(permissions, settings)

        this.permissions = permissions
        const language = normalizeLanguage(settings?.language ?? defaultSettings_.language)
        const translations = getTranslations(language)
        const i = translations.settings
        const streamI = translations.stream

        // Root div
        this.divElement.classList.add("settings")

        // Sidebar
        this.sidebarHeader.innerText = i.sidebar
        this.divElement.appendChild(this.sidebarHeader)

        this.sidebarEdge = new SelectComponent("sidebarEdge", [
            { value: "left", name: i.left },
            { value: "right", name: i.right },
            { value: "up", name: i.up },
            { value: "down", name: i.down },
        ], {
            displayName: i.sidebarEdge,
            preSelectedOption: settings?.sidebarEdge ?? defaultSettings_.sidebarEdge,
        })
        this.sidebarEdge.addChangeListener(this.onSettingsChange.bind(this))
        this.sidebarEdge.mount(this.divElement)

        // Network / performance
        this.networkHeader.innerText = i.networkPerformance
        this.divElement.appendChild(this.networkHeader)

        // Bitrate
        this.bitrate = new InputComponent("bitrate", "number", i.bitrate, {
            defaultValue: defaultSettings_.bitrate.toString(),
            value: settings?.bitrate?.toString(),
            step: "100",
            numberSlider: {
                range_min: Math.min(this.permissions.maximum_bitrate_kbps ?? 1000, 1000),
                range_max: this.permissions.maximum_bitrate_kbps ?? 10000,
            }
        })
        this.bitrate.addChangeListener(this.onSettingsChange.bind(this))
        this.bitrate.mount(this.divElement)

        this.adaptiveBitrate = new InputComponent("adaptiveBitrate", "checkbox", i.adaptiveBitrate, {
            checked: settings?.adaptiveBitrate ?? defaultSettings_.adaptiveBitrate
        })
        this.adaptiveBitrate.addChangeListener(this.onSettingsChange.bind(this))
        this.adaptiveBitrate.mount(this.divElement)

        this.minimumBitrate = new InputComponent("minimumBitrate", "number", i.minimumBitrate, {
            defaultValue: defaultSettings_.minimumBitrate.toString(),
            value: settings?.minimumBitrate?.toString(),
            step: "100",
            numberSlider: {
                range_min: Math.min(this.permissions.maximum_bitrate_kbps ?? 500, 500),
                range_max: this.permissions.maximum_bitrate_kbps ?? defaultSettings_.bitrate,
            }
        })
        this.minimumBitrate.addChangeListener(this.onSettingsChange.bind(this))
        this.minimumBitrate.mount(this.divElement)

        // Video
        this.streamHeader.innerText = i.video
        this.divElement.appendChild(this.streamHeader)

        // Fps
        this.fps = new InputComponent("fps", "number", i.fps, {
            defaultValue: defaultSettings_.fps.toString(),
            value: settings?.fps?.toString(),
            step: "100"
        })
        this.fps.addChangeListener(this.onSettingsChange.bind(this))
        this.fps.mount(this.divElement)

        // Video Size
        this.videoSize = new SelectComponent("videoSize",
            [
                { value: "720p", name: "720p" },
                { value: "1080p", name: "1080p" },
                { value: "1440p", name: "1440p" },
                { value: "4k", name: "4k" },
                { value: "native", name: i.native },
                { value: "custom", name: i.custom }
            ],
            {
                displayName: i.videoSize,
                preSelectedOption: settings?.videoSize || defaultSettings_.videoSize
            }
        )
        this.videoSize.addChangeListener(this.onSettingsChange.bind(this))
        this.videoSize.mount(this.divElement)

        this.videoSizeWidth = new InputComponent("videoSizeWidth", "number", i.videoWidth, {
            defaultValue: defaultSettings_.videoSizeCustom.width.toString(),
            value: settings?.videoSizeCustom?.width.toString()
        })
        this.videoSizeWidth.addChangeListener(this.onSettingsChange.bind(this))
        this.videoSizeWidth.mount(this.divElement)

        this.videoSizeHeight = new InputComponent("videoSizeHeight", "number", i.videoHeight, {
            defaultValue: defaultSettings_.videoSizeCustom.height.toString(),
            value: settings?.videoSizeCustom?.height.toString()
        })
        this.videoSizeHeight.addChangeListener(this.onSettingsChange.bind(this))
        this.videoSizeHeight.mount(this.divElement)

        // Video Sample Queue Size
        this.videoSampleQueueSize = new InputComponent("videoFrameQueueSize", "number", i.videoFrameQueueSize, {
            defaultValue: defaultSettings_.videoFrameQueueSize.toString(),
            value: settings?.videoFrameQueueSize?.toString()
        })
        this.videoSampleQueueSize.addChangeListener(this.onSettingsChange.bind(this))
        this.videoSampleQueueSize.mount(this.divElement)

        // Codec
        const allowedVideoCodecs = [
            { value: "auto", name: i.autoExperimental },
        ]
        if (this.permissions.allow_codec_h264) {
            allowedVideoCodecs.push(
                { value: "h264", name: "H264" },
            )
        }
        if (this.permissions.allow_codec_h265) {
            allowedVideoCodecs.push(
                { value: "h265", name: "H265" },
            )
        }
        if (this.permissions.allow_codec_av1) {
            allowedVideoCodecs.push(
                { value: "av1", name: i.av1Experimental }
            )
        }

        this.videoCodec = new SelectComponent("videoCodec", allowedVideoCodecs, {
            displayName: i.videoCodec,
            preSelectedOption: settings?.videoCodec ?? defaultSettings_.videoCodec
        })
        this.videoCodec.addChangeListener(this.onSettingsChange.bind(this))
        this.videoCodec.mount(this.divElement)

        // Force Video Element renderer
        this.forceVideoElementRenderer = new InputComponent("forceVideoElementRenderer", "checkbox", i.forceVideoElementRenderer, {
            checked: settings?.forceVideoElementRenderer ?? defaultSettings_.forceVideoElementRenderer
        })
        this.forceVideoElementRenderer.addChangeListener(this.onSettingsChange.bind(this))
        this.forceVideoElementRenderer.mount(this.divElement)

        // Use Canvas Renderer
        this.canvasRenderer = new InputComponent("canvasRenderer", "checkbox", i.useCanvasRenderer, {
            defaultValue: defaultSettings_.canvasRenderer.toString(),
            checked: settings === null || settings === void 0 ? void 0 : settings.canvasRenderer
        })
        this.canvasRenderer.addChangeListener(this.onSettingsChange.bind(this))
        this.canvasRenderer.mount(this.divElement)

        // Canvas VSync (Canvas only: sync draw to display refresh to reduce tearing; off = lower latency)
        this.canvasVsync = new InputComponent("canvasVsync", "checkbox", i.canvasVsync, {
            checked: settings?.canvasVsync ?? defaultSettings_.canvasVsync
        })
        this.canvasVsync.addChangeListener(this.onSettingsChange.bind(this))
        this.canvasVsync.mount(this.divElement)

        // HDR
        this.hdr = new InputComponent("hdr", "checkbox", i.enableHdr, {
            checked: settings?.hdr ?? defaultSettings_.hdr
        })
        this.hdr.addChangeListener(this.onSettingsChange.bind(this))
        this.hdr.mount(this.divElement)

        if (!this.permissions.allow_hdr) {
            this.hdr.setChecked(false)
            this.hdr.setEnabled(false)
        }

        // Audio local
        this.audioHeader.innerText = i.audio
        this.divElement.appendChild(this.audioHeader)

        this.playAudioLocal = new InputComponent("playAudioLocal", "checkbox", i.playAudioLocal, {
            checked: settings?.playAudioLocal
        })
        this.playAudioLocal.addChangeListener(this.onSettingsChange.bind(this))
        this.playAudioLocal.mount(this.divElement)

        // Audio Sample Queue Size
        this.audioSampleQueueSize = new InputComponent("audioSampleQueueSize", "number", i.audioSampleQueueSize, {
            defaultValue: defaultSettings_.audioSampleQueueSize.toString(),
            value: settings?.audioSampleQueueSize?.toString()
        })
        this.audioSampleQueueSize.addChangeListener(this.onSettingsChange.bind(this))
        this.audioSampleQueueSize.mount(this.divElement)

        // Mouse
        this.mouseHeader.innerText = i.mouse
        this.divElement.appendChild(this.mouseHeader)

        this.mouseScrollMode = new SelectComponent("mouseScrollMode",
            [
                { value: "highres", name: i.highRes },
                { value: "normal", name: i.normal }
            ],
            {
                displayName: i.scrollMode,
                preSelectedOption: settings?.mouseScrollMode || defaultSettings_.mouseScrollMode
            }
        )
        this.mouseScrollMode.addChangeListener(this.onSettingsChange.bind(this))
        this.mouseScrollMode.mount(this.divElement)

        this.mouseMode = new SelectComponent("mouseMode",
            [
                { value: "relative", name: streamI.relative },
                { value: "follow", name: streamI.follow },
                { value: "localCursor", name: streamI.localCursor },
                { value: "pointAndDrag", name: streamI.pointAndDrag }
            ],
            {
                displayName: i.startupMouseMode,
                preSelectedOption: settings?.mouseMode ?? defaultSettings_.mouseMode
            }
        )
        this.mouseMode.addChangeListener(this.onSettingsChange.bind(this))
        this.mouseMode.mount(this.divElement)

        this.touchMode = new SelectComponent("touchMode",
            [
                { value: "touch", name: streamI.touch },
                { value: "mouseRelative", name: streamI.relative },
                { value: "localCursor", name: streamI.localCursor },
                { value: "pointAndDrag", name: streamI.pointAndDrag }
            ],
            {
                displayName: i.startupTouchMode,
                preSelectedOption: settings?.touchMode ?? defaultSettings_.touchMode
            }
        )
        this.touchMode.addChangeListener(this.onSettingsChange.bind(this))
        this.touchMode.mount(this.divElement)

        this.localCursorSensitivity = new InputComponent("localCursorSensitivity", "number", i.localCursorSensitivity, {
            defaultValue: defaultSettings_.localCursorSensitivity.toString(),
            value: settings?.localCursorSensitivity?.toString(),
            step: "0.1",
            numberSlider: {
                range_min: 0.1,
                range_max: 3
            }
        })
        this.localCursorSensitivity.addChangeListener(this.onSettingsChange.bind(this))
        this.localCursorSensitivity.mount(this.divElement)

        // Controller
        if (window.isSecureContext) {
            this.controllerHeader.innerText = i.controller
        } else {
            this.controllerHeader.innerText = i.controllerDisabled
        }
        this.divElement.appendChild(this.controllerHeader)

        this.controllerInvertAB = new InputComponent("controllerInvertAB", "checkbox", i.invertAB, {
            checked: settings?.controllerConfig?.invertAB
        })
        this.controllerInvertAB.addChangeListener(this.onSettingsChange.bind(this))
        this.controllerInvertAB.mount(this.divElement)

        this.controllerInvertXY = new InputComponent("controllerInvertXY", "checkbox", i.invertXY, {
            checked: settings?.controllerConfig?.invertXY
        })
        this.controllerInvertXY.addChangeListener(this.onSettingsChange.bind(this))
        this.controllerInvertXY.mount(this.divElement)

        // Controller Send Interval
        this.controllerSendIntervalOverride = new InputComponent("controllerSendIntervalOverride", "number", i.overrideControllerInterval, {
            hasEnableCheckbox: true,
            defaultValue: "20",
            value: settings?.controllerConfig?.sendIntervalOverride?.toString(),
            numberSlider: {
                range_min: 10,
                range_max: 120
            }
        })
        this.controllerSendIntervalOverride.setEnabled(settings?.controllerConfig?.sendIntervalOverride != null)
        this.controllerSendIntervalOverride.addChangeListener(this.onSettingsChange.bind(this))
        this.controllerSendIntervalOverride.mount(this.divElement)

        if (!window.isSecureContext) {
            this.controllerInvertAB.setEnabled(false)
            this.controllerInvertXY.setEnabled(false)
        }

        // Other
        this.otherHeader.innerText = i.other
        this.divElement.appendChild(this.otherHeader)

        // Data Transport
        const allowedDataTransport = [
            { value: "auto", name: i.auto },
        ]
        if (this.permissions.allow_transport_websockets) {
            allowedDataTransport.push(
                { value: "webtransport", name: "WebTransport" },
                { value: "websocket", name: i.webSocket },
            )
        }

        this.language = new SelectComponent("language", getLanguageOptions(), {
            displayName: i.language,
            preSelectedOption: language
        })
        this.language.addChangeListener(this.onSettingsChange.bind(this))
        this.language.mount(this.divElement)

        this.dataTransport = new SelectComponent("transport", allowedDataTransport, {
            displayName: i.dataTransport,
            preSelectedOption: settings?.dataTransport ?? defaultSettings_.dataTransport
        })
        this.dataTransport.addChangeListener(this.onSettingsChange.bind(this))
        this.dataTransport.mount(this.divElement)

        // Advanced security / performance. These controls affect only the
        // inner host-to-streamer Moonlight hop; WebTransport cannot disable TLS.
        this.securityHeader.innerText = i.securityPerformance
        this.divElement.appendChild(this.securityHeader)

        this.securityWarning.innerText = i.securityPerformanceWarning
        this.securityWarning.classList.add("settings-warning")
        this.divElement.appendChild(this.securityWarning)

        this.encryptHostVideo = new InputComponent("encryptHostVideo", "checkbox", i.encryptHostVideo, {
            checked: settings?.encryptHostVideo ?? defaultSettings_.encryptHostVideo
        })
        this.encryptHostVideo.addChangeListener(this.onSettingsChange.bind(this))
        this.encryptHostVideo.mount(this.divElement)

        this.encryptHostAudio = new InputComponent("encryptHostAudio", "checkbox", i.encryptHostAudio, {
            checked: settings?.encryptHostAudio ?? defaultSettings_.encryptHostAudio
        })
        this.encryptHostAudio.addChangeListener(this.onSettingsChange.bind(this))
        this.encryptHostAudio.mount(this.divElement)

        this.enterFullscreenOnStreamStart = new InputComponent("enterFullscreenOnStreamStart", "checkbox", i.enterFullscreenOnStreamStart, {
            checked: settings?.enterFullscreenOnStreamStart ?? defaultSettings_.enterFullscreenOnStreamStart
        })
        this.enterFullscreenOnStreamStart.addChangeListener(this.onSettingsChange.bind(this))
        this.enterFullscreenOnStreamStart.mount(this.divElement)

        // Fullscreen Keybind
        this.toggleFullscreenWithKeybind = new InputComponent("toggleFullscreenWithKeybind", "checkbox", i.toggleFullscreenWithKeybind, {
            checked: settings?.toggleFullscreenWithKeybind
        })
        this.toggleFullscreenWithKeybind.addChangeListener(this.onSettingsChange.bind(this))
        this.toggleFullscreenWithKeybind.mount(this.divElement)

        // Page Style
        this.pageStyle = new SelectComponent("pageStyle", [
            { value: "standard", name: "Standard" },
            { value: "moonlight", name: "Moonlight" },
        ], {
            displayName: i.style,
            preSelectedOption: settings?.pageStyle ?? defaultSettings_.pageStyle
        })
        this.pageStyle.addChangeListener(this.onSettingsChange.bind(this))
        this.pageStyle.mount(this.divElement)

        // Custom Select Element
        this.useSelectElementPolyfill = new InputComponent("useSelectElementPolyfill", "checkbox", i.useCustomDropdown, {
            checked: settings?.useSelectElementPolyfill ?? defaultSettings_.useSelectElementPolyfill
        })
        this.useSelectElementPolyfill.addChangeListener(this.onSettingsChange.bind(this))
        this.useSelectElementPolyfill.mount(this.divElement)

        this.onSettingsChange()
    }

    private onSettingsChange() {
        if (this.videoSize.getValue() == "custom") {
            this.videoSizeWidth.setEnabled(true)
            this.videoSizeHeight.setEnabled(true)
        } else {
            this.videoSizeWidth.setEnabled(false)
            this.videoSizeHeight.setEnabled(false)
        }

        this.minimumBitrate.setEnabled(this.adaptiveBitrate.isChecked())

        this.divElement.dispatchEvent(new ComponentEvent("ml-settingschange", this))
    }

    addChangeListener(listener: StreamSettingsChangeListener) {
        this.divElement.addEventListener("ml-settingschange", listener as any)
    }
    removeChangeListener(listener: StreamSettingsChangeListener) {
        this.divElement.removeEventListener("ml-settingschange", listener as any)
    }

    getStreamSettings(): Settings {
        const settings = globalDefaultSettings()

        settings.sidebarEdge = this.sidebarEdge.getValue() as any
        settings.bitrate = parseInt(this.bitrate.getValue())
        settings.adaptiveBitrate = this.adaptiveBitrate.isChecked()
        settings.minimumBitrate = parseInt(this.minimumBitrate.getValue())
        settings.fps = parseInt(this.fps.getValue())
        settings.videoSize = this.videoSize.getValue() as any
        settings.videoSizeCustom = {
            width: parseInt(this.videoSizeWidth.getValue()),
            height: parseInt(this.videoSizeHeight.getValue())
        }
        settings.videoFrameQueueSize = parseInt(this.videoSampleQueueSize.getValue())
        settings.videoCodec = this.videoCodec.getValue() as any
        settings.forceVideoElementRenderer = this.forceVideoElementRenderer.isChecked()
        settings.canvasRenderer = this.canvasRenderer.isChecked()
        settings.canvasVsync = this.canvasVsync.isChecked()

        settings.playAudioLocal = this.playAudioLocal.isChecked()
        settings.encryptHostVideo = this.encryptHostVideo.isChecked()
        settings.encryptHostAudio = this.encryptHostAudio.isChecked()
        settings.audioSampleQueueSize = parseInt(this.audioSampleQueueSize.getValue())

        settings.mouseScrollMode = this.mouseScrollMode.getValue() as any
        settings.mouseMode = this.mouseMode.getValue() as MouseMode
        settings.touchMode = this.touchMode.getValue() as TouchMode
        settings.localCursorSensitivity = parseFloat(this.localCursorSensitivity.getValue())

        settings.controllerConfig.invertAB = this.controllerInvertAB.isChecked()
        settings.controllerConfig.invertXY = this.controllerInvertXY.isChecked()
        if (this.controllerSendIntervalOverride.isEnabled()) {
            settings.controllerConfig.sendIntervalOverride = parseInt(this.controllerSendIntervalOverride.getValue())
        } else {
            settings.controllerConfig.sendIntervalOverride = null
        }

        settings.dataTransport = this.dataTransport.getValue() as any
        settings.language = this.language.getValue() as Language

        settings.enterFullscreenOnStreamStart = this.enterFullscreenOnStreamStart.isChecked()
        settings.toggleFullscreenWithKeybind = this.toggleFullscreenWithKeybind.isChecked()

        settings.pageStyle = this.pageStyle.getValue() as any

        settings.hdr = this.hdr.isChecked()

        settings.useSelectElementPolyfill = this.useSelectElementPolyfill.isChecked()

        makeSettingsValid(this.permissions, settings)

        return settings
    }

    mountBefore(parent: HTMLElement, before: HTMLElement): void {
        parent.insertBefore(this.divElement, before)
    }
    mount(parent: HTMLElement): void {
        parent.appendChild(this.divElement)
    }
    unmount(parent: HTMLElement): void {
        parent.removeChild(this.divElement)
    }
}
