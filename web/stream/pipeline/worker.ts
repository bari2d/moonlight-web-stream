import { LogMessageType } from "../../api_bindings.js"
import { Logger } from "../log.js"
import { andVideoCodecs } from "../video.js"
import { buildPipeline, getPipe, Pipe, PipeInfo, pipeName } from "./index.js"
import { WorkerOffscreenCanvasSendPipe } from "./worker_io.js"
import { WorkerReceiver } from "./worker_pipe.js"
import { ToMainMessage, ToWorkerMessage, WorkerMessage } from "./worker_types.js"

// Configure logger
const logger = new Logger()

function onLog(text: string, type: LogMessageType | null) {
    const message: ToMainMessage = {
        log: text,
        info: { type: type ?? undefined }
    }

    postMessage(message)
}

logger?.addInfoListener(onLog)

let pipelineErrored = false
let currentPipeline: WorkerReceiver | null = null
let canvasPipe: WorkerOffscreenCanvasSendPipe | null = null
let idrPollInterval: ReturnType<typeof setInterval> | null = null

// Decoder pipes inside the worker can demand a key frame (for example after
// a backlog reset). The main thread cannot poll them directly, so relay the
// request as an output message that the WorkerPipe reports from its own
// pollRequestIdr.
const WORKER_IDR_POLL_INTERVAL_MS = 50
function startIdrRelay() {
    if (idrPollInterval != null) {
        clearInterval(idrPollInterval)
    }
    idrPollInterval = setInterval(() => {
        const pipeline = currentPipeline as (WorkerReceiver & { pollRequestIdr?: () => boolean }) | null
        if (!pipeline || pipelineErrored || typeof pipeline.pollRequestIdr != "function") {
            return
        }
        try {
            if (pipeline.pollRequestIdr() === true) {
                const message: ToMainMessage = { output: { requestIdr: true } }
                postMessage(message)
            }
        } catch (error) {
            logger.debug(`Failed to poll the worker pipeline for an IDR request: ${String(error)}`)
        }
    }, WORKER_IDR_POLL_INTERVAL_MS)
}

class WorkerMessageSender implements WorkerReceiver {
    static readonly type: string = "workerinput"

    readonly implementationName: string = "worker_output"

    constructor(logger?: Logger) {
    }

    onWorkerMessage(output: WorkerMessage, transferable?: Transferable[]): void {
        const message: ToMainMessage = { output }

        postMessage(message, { transfer: transferable ?? [] })
    }

    getBase(): Pipe | null {
        return null
    }
}

async function onMessage(message: ToWorkerMessage) {
    if ("checkSupport" in message) {
        const pipeline = message.checkSupport

        const pipelineInfo: PipeInfo = {
            environmentSupported: true
        }

        for (const pipeRaw of pipeline.pipes) {
            const pipe = getPipe(pipeRaw)
            if (!pipe) {
                logger.debug(`Failed to find pipe "${pipeName(pipeRaw)}"`)
                pipelineInfo.environmentSupported = false
                break
            }
            const pipeInfo = await pipe.getInfo()

            if (!pipeInfo.environmentSupported) {
                pipelineInfo.environmentSupported = false
                break
            }

            if ("supportedVideoCodecs" in pipeInfo && pipeInfo.supportedVideoCodecs) {
                if (pipelineInfo.supportedVideoCodecs) {
                    pipelineInfo.supportedVideoCodecs = andVideoCodecs(pipelineInfo.supportedVideoCodecs, pipeInfo.supportedVideoCodecs)
                } else {
                    pipelineInfo.supportedVideoCodecs = pipeInfo.supportedVideoCodecs
                }
            }
        }

        const response: ToMainMessage = {
            checkSupport: pipelineInfo
        }
        postMessage(response)
    } else if ("createPipeline" in message) {
        logger.debug(`Trying to build pipeline in worker, Pipes: ${JSON.stringify(message.createPipeline.pipes)}`)

        const pipeline = message.createPipeline

        const newPipeline = buildPipeline(WorkerMessageSender, pipeline, logger, pipeline.options)
        if (newPipeline && "onWorkerMessage" in newPipeline && typeof newPipeline.onWorkerMessage == "function") {
            currentPipeline = newPipeline as WorkerReceiver
        } else {
            logger.debug("Failed to build worker pipeline!", { type: "fatal" })
        }
        logger.debug(`Successfully build pipeline in worker: ${currentPipeline?.implementationName}`)
        startIdrRelay()

        let base = newPipeline
        let newBase = newPipeline?.getBase()
        while ((newBase = base?.getBase()) && !(newBase instanceof WorkerMessageSender)) {
            base = newBase
        }

        if (base && base instanceof WorkerOffscreenCanvasSendPipe) {
            canvasPipe = base
            logger.debug("Found WorkerOffscreenCanvasSendPipe in worker pipeline")
        }
    } else if ("input" in message) {
        if (pipelineErrored) {
            return
        }

        if ("canvas" in message.input) {
            // Filter out the canvas, the last pipe needs that
            if (canvasPipe) {
                logger.debug("Received OffscreenCanvas in worker")
                canvasPipe.setContext(message.input.canvas)
            } else {
                pipelineErrored = true
                logger.debug("Failed to set OffscreenCanvas in the worker because the worker doesn't contain a compatible pipe at the end of the pipeline!", { type: "fatal" })
            }
        } else {
            if (currentPipeline) {
                currentPipeline.onWorkerMessage(message.input)
            } else {
                pipelineErrored = true
                logger.debug(`Failed to submit worker pipe input because the worker wasn't assigned a pipeline! Message: ${JSON.stringify(message)}`)
            }
        }

    }
}

onmessage = (event) => {
    const message = event.data as ToWorkerMessage
    onMessage(message)
}
