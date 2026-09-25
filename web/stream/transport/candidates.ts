const MAX_CANDIDATES = 6

export type WebTransportHostPreference = "auto" | "primary" | "alternate"
export type WebTransportPortPreference = "auto" | "443" | "8443" | "4443"

export function redactTransportError(error: unknown, url: string): string {
    const message = error instanceof Error ? error.message : String(error)
    try {
        const token = new URL(url).searchParams.get("token")
        return token ? message.split(token).join("<redacted>") : message
    } catch {
        return "Invalid WebTransport endpoint"
    }
}

/** Keep the legacy URL first; never log these bearer URLs. */
export function transportCandidates(primary: string, alternatives: readonly string[] = []): string[] {
    const candidates = new Set<string>()
    for (const value of [primary, ...alternatives]) {
        try {
            const url = new URL(value)
            if (url.protocol != "https:" || url.username || url.password || url.hash) continue
            candidates.add(url.href)
            if (candidates.size == MAX_CANDIDATES) break
        } catch { /* Ignore malformed optional candidates. */ }
    }
    return [...candidates]
}

export function filterTransportCandidates(
    candidates: readonly string[],
    hostPreference: WebTransportHostPreference,
    portPreference: WebTransportPortPreference,
): string[] {
    if (candidates.length == 0) return []
    let primaryHost: string
    try {
        primaryHost = new URL(candidates[0]).hostname.toLowerCase()
    } catch {
        return []
    }
    return candidates.filter(candidate => {
        try {
            const url = new URL(candidate)
            const hostname = url.hostname.toLowerCase()
            const port = url.port || "443"
            const hostMatches = hostPreference == "auto"
                || (hostPreference == "primary" && hostname == primaryHost)
                || (hostPreference == "alternate" && hostname != primaryHost)
            return hostMatches && (portPreference == "auto" || port == portPreference)
        } catch {
            return false
        }
    })
}

export interface ConnectionCandidate {
    connect(timeoutMs: number): Promise<void>
    close(): Promise<void>
}

/** Race a small ordered list, with a hard deadline for each entire connection.
 * Abort and losing attempts are closed even if connect() ignores cancellation.
 */
export function raceCandidates<T extends ConnectionCandidate>(
    urls: readonly string[],
    create: (url: string) => T,
    signal: AbortSignal,
    report: (url: string, result: "trying" | "connected" | "failed" | "timeout") => void,
    timeoutMs = 3000,
    staggerMs = 250,
): Promise<T | null> {
    return new Promise(resolve => {
        const candidates = urls.slice(0, MAX_CANDIDATES)
        const timers = new Set<ReturnType<typeof setTimeout>>()
        const active = new Set<T>()
        let settled = false
        let remaining = candidates.length
        const close = (candidate: T) => { void candidate.close().catch(() => undefined) }
        const finish = (winner: T | null) => {
            if (settled) return
            settled = true
            for (const timer of timers) clearTimeout(timer)
            timers.clear()
            signal.removeEventListener("abort", abort)
            for (const candidate of active) if (candidate !== winner) close(candidate)
            active.clear()
            resolve(winner)
        }
        const abort = () => finish(null)
        if (signal.aborted || candidates.length == 0) {
            finish(null)
            return
        }
        signal.addEventListener("abort", abort, { once: true })
        const start = (url: string) => {
            if (settled) return
            report(url, "trying")
            let candidate: T
            try {
                candidate = create(url)
            } catch {
                report(url, "failed")
                if (--remaining == 0) finish(null)
                return
            }
            active.add(candidate)
            let done = false
            const failed = (result: "failed" | "timeout") => {
                if (done || settled) return
                done = true
                clearTimeout(deadline)
                timers.delete(deadline)
                active.delete(candidate)
                close(candidate)
                report(url, result)
                if (--remaining == 0) finish(null)
            }
            const deadline = setTimeout(() => failed("timeout"), timeoutMs)
            timers.add(deadline)
            // The microtask also catches synchronous connect() errors.
            let started = false
            void Promise.resolve().then(() => {
                if (settled || done) return
                started = true
                return candidate.connect(timeoutMs)
            }).then(() => {
                if (!started) return
                if (settled || done) {
                    close(candidate)
                    return
                }
                done = true
                report(url, "connected")
                finish(candidate)
            }, () => failed("failed"))
        }
        candidates.forEach((url, index) => {
            if (index == 0) start(url)
            else timers.add(setTimeout(() => start(url), index * staggerMs))
        })
    })
}
