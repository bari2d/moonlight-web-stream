export type SettingsObject = Record<string, unknown>

export type SettingsOutboxEnvelope = {
    version: 2,
    token: string,
    revision: number,
    baseRevision: number,
    updatedAt: number,
    patch: SettingsObject,
}

export type OutboxEntry = {
    key: string,
    envelope: SettingsOutboxEnvelope,
}

export type StorageReader = Pick<Storage, "length" | "key" | "getItem">
export type StorageWriter = Pick<Storage, "getItem" | "setItem" | "removeItem">

export function isSettingsObject(value: unknown): value is SettingsObject {
    return value !== null && typeof value === "object" && !Array.isArray(value)
}

export function parseOutboxEnvelope(raw: string | null): SettingsOutboxEnvelope | null {
    if (!raw) {
        return null
    }

    try {
        const value = JSON.parse(raw) as Partial<SettingsOutboxEnvelope>
        if (
            value.version !== 2
            || typeof value.token !== "string"
            || value.token.length === 0
            || value.token.length > 128
            || !Number.isSafeInteger(value.revision)
            || (value.revision ?? 0) < 1
            || !Number.isSafeInteger(value.baseRevision)
            || (value.baseRevision ?? -1) < 0
            || !Number.isFinite(value.updatedAt)
            || !isSettingsObject(value.patch)
        ) {
            return null
        }

        return value as SettingsOutboxEnvelope
    } catch {
        return null
    }
}

export function compareOutboxEnvelopes(a: SettingsOutboxEnvelope, b: SettingsOutboxEnvelope): number {
    if (a.revision !== b.revision) {
        return a.revision - b.revision
    }
    if (a.updatedAt !== b.updatedAt) {
        return a.updatedAt - b.updatedAt
    }
    return a.token.localeCompare(b.token)
}

export function newestOutboxEntry(entries: Iterable<OutboxEntry>): OutboxEntry | null {
    let newest: OutboxEntry | null = null
    for (const entry of entries) {
        if (!newest || compareOutboxEnvelopes(entry.envelope, newest.envelope) > 0) {
            newest = entry
        }
    }
    return newest
}

export function listOutboxEntries(storage: StorageReader, outboxKey: string): OutboxEntry[] {
    const entries: OutboxEntry[] = []
    const draftPrefix = `${outboxKey}:draft:`

    for (let index = 0; index < storage.length; index++) {
        const key = storage.key(index)
        if (key !== outboxKey && !key?.startsWith(draftPrefix)) {
            continue
        }

        const envelope = parseOutboxEnvelope(storage.getItem(key))
        if (key && envelope) {
            entries.push({ key, envelope })
        }
    }

    return entries
}

export function createOutboxEnvelope(
    patch: SettingsObject,
    baseRevision: number,
    existing: Iterable<OutboxEntry>,
    token: string,
    updatedAt: number,
): SettingsOutboxEnvelope {
    const newest = newestOutboxEntry(existing)
    return {
        version: 2,
        token,
        revision: (newest?.envelope.revision ?? 0) + 1,
        baseRevision,
        updatedAt,
        patch,
    }
}

export function writeOutboxEnvelope(storage: StorageWriter, key: string, envelope: SettingsOutboxEnvelope): boolean {
    try {
        storage.setItem(key, JSON.stringify(envelope))
        return true
    } catch {
        return false
    }
}

/** Removes an entry only when that exact token is still stored at its key. */
export function removeOutboxEnvelope(
    storage: StorageWriter,
    key: string,
    expectedToken: string,
): boolean {
    try {
        const current = parseOutboxEnvelope(storage.getItem(key))
        if (!current || current.token !== expectedToken) {
            return false
        }
        storage.removeItem(key)
        return true
    } catch {
        return false
    }
}
