import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"
import ts from "typescript"

const source = await readFile(new URL("../web/stream/transport/candidates.ts", import.meta.url), "utf8")
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText
const { filterTransportCandidates, raceCandidates, transportCandidates, redactTransportError } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`)
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
const quiet = () => {}

test("browser errors cannot leak the endpoint's bearer token", () => {
    const url = "https://a.test/wt?v=4&token=secret-token"
    const text = redactTransportError(new Error(`Failed at ${url}; secret-token`), url)
    assert.equal(text.includes("secret-token"), false)
    assert.equal(text.includes("a.test"), true)
    assert.equal(redactTransportError("timeout", url), "timeout")
})

test("legacy URL first, canonical deduplication, HTTPS only, six-candidate cap", () => {
    const primary = "https://a.test:443/wt?v=4&token=secret"
    assert.deepEqual(transportCandidates(primary, [primary, "https://a.test/wt?v=4&token=secret", "http://b.test/wt", "invalid", "https://user@b.test/wt", "https://b.test/wt#frag", "https://b.test:8443/wt"]), ["https://a.test/wt?v=4&token=secret", "https://b.test:8443/wt"])
    assert.equal(transportCandidates(primary, Array.from({length: 10}, (_, i) => `https://h${i}.test/wt`)).length, 6)
    assert.equal(transportCandidates(primary).length, 1)
})

test("hostname and port preferences select exact WebTransport routes", () => {
    const candidates = [
        "https://primary.test/wt?v=4&token=x",
        "https://alternate.test/wt?v=4&token=x",
        "https://primary.test:8443/wt?v=4&token=x",
        "https://alternate.test:8443/wt?v=4&token=x",
        "https://primary.test:4443/wt?v=4&token=x",
        "https://alternate.test:4443/wt?v=4&token=x",
    ]
    assert.deepEqual(filterTransportCandidates(candidates, "auto", "auto"), candidates)
    assert.deepEqual(filterTransportCandidates(candidates, "primary", "auto"), [candidates[0], candidates[2], candidates[4]])
    assert.deepEqual(filterTransportCandidates(candidates, "alternate", "443"), [candidates[1]])
    assert.deepEqual(filterTransportCandidates(candidates, "primary", "8443"), [candidates[2]])
    assert.deepEqual(filterTransportCandidates(candidates, "alternate", "4443"), [candidates[5]])
})

test("blocked primary loses to reachable alternative and is closed", async () => {
    const closed = []
    const winner = await raceCandidates(["blocked", "working"], url => ({
        url,
        connect: () => url == "blocked" ? new Promise(() => {}) : Promise.resolve(),
        close: async () => { closed.push(url) },
    }), new AbortController().signal, quiet, 100, 5)
    assert.equal(winner.url, "working")
    assert.deepEqual(closed, ["blocked"])
})

test("working primary prevents later attempts", async () => {
    const attempted = []
    const winner = await raceCandidates(["first", "second"], url => {
        attempted.push(url)
        return { connect: async () => {}, close: async () => {} }
    }, new AbortController().signal, quiet, 100, 10)
    assert.ok(winner)
    await delay(20)
    assert.deepEqual(attempted, ["first"])
})

test("all blocked connections expire within the bounded race", async () => {
    let closed = 0
    const events = []
    const result = await raceCandidates(["a", "b", "c"], () => ({
        connect: () => new Promise(() => {}), close: async () => { closed++ },
    }), new AbortController().signal, (_, event) => events.push(event), 20, 3)
    assert.equal(result, null)
    assert.equal(closed, 3)
    assert.equal(events.filter(e => e == "timeout").length, 3)
})

test("abort closes pending connections and cancels scheduled alternatives", async () => {
    const controller = new AbortController()
    const attempted = [], closed = []
    const pending = raceCandidates(["first", "second"], url => {
        attempted.push(url)
        return { connect: () => new Promise(() => {}), close: async () => { closed.push(url) } }
    }, controller.signal, quiet, 100, 20)
    controller.abort()
    assert.equal(await pending, null)
    await delay(30)
    assert.deepEqual(attempted, ["first"])
    assert.deepEqual(closed, ["first"])
})

test("late success after timeout is closed and never selected", async () => {
    let finish, closed = 0
    const pending = raceCandidates(["late"], () => ({
        connect: () => new Promise(resolve => { finish = resolve }),
        close: async () => { closed++ },
    }), new AbortController().signal, quiet, 5, 1)
    assert.equal(await pending, null)
    finish()
    await delay(1)
    assert.equal(closed, 2)
})

test("construction and connection exceptions do not block fallback", async () => {
    const result = await raceCandidates(["construct", "connect"], url => {
        if (url == "construct") throw new Error("constructor failed")
        return { connect: () => { throw new Error("connect failed") }, close: async () => {} }
    }, new AbortController().signal, quiet, 100, 1)
    assert.equal(result, null)
})

test("already aborted or empty races never create connections", async () => {
    const controller = new AbortController()
    controller.abort()
    const create = () => { throw new Error("should never run") }
    assert.equal(await raceCandidates(["a"], create, controller.signal, quiet), null)
    assert.equal(await raceCandidates([], create, new AbortController().signal, quiet), null)
})
