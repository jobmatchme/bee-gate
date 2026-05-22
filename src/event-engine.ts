import { ConversationQueue } from "./queue.js";
import { newTurnId } from "./session.js";
import type {
	BeeResolvedTurn,
	BeeRunEvent,
	BeeTurnRequest,
	BeeWorkerClient,
	BeeWorkerTargetConfig,
	GateLogger,
} from "./types.js";

interface ActiveStreamTurn {
	turnId: string;
	controller: AbortController;
	worker: BeeWorkerTargetConfig;
	threadId?: string;
}

export interface BeeEventStreamContext {
	input: BeeResolvedTurn;
	request: BeeTurnRequest;
}

export type BeeEventStreamHandler = (event: BeeRunEvent, context: BeeEventStreamContext) => Promise<void> | void;

export interface BeeEventStreamErrorContext {
	input: BeeResolvedTurn;
	turnId: string;
}

export type BeeEventStreamErrorHandler = (error: unknown, context: BeeEventStreamErrorContext) => Promise<void> | void;

export interface BeeEventStreamDispatchResult {
	turnId: string;
	queued: boolean;
}

export interface BeeEventStreamEngineOptions {
	workerClient: BeeWorkerClient;
	logger?: GateLogger;
}

/**
 * Event-oriented Bee gateway engine for adapters that want to handle Bee Dance
 * events themselves instead of rendering them through a TransportSink.
 */
export class BeeEventStreamEngine {
	private queues = new Map<string, ConversationQueue>();
	private activeTurns = new Map<string, ActiveStreamTurn>();

	constructor(private options: BeeEventStreamEngineOptions) {}

	dispatch(
		input: BeeResolvedTurn,
		onEvent: BeeEventStreamHandler,
		onError?: BeeEventStreamErrorHandler,
	): BeeEventStreamDispatchResult {
		const queue = this.getQueue(input.sessionId);
		const queued = this.activeTurns.has(input.sessionId) || queue.isProcessing || queue.length > 0;
		const turnId = newTurnId();

		queue.enqueue(async () => {
			await this.process(input, turnId, onEvent, onError);
		});

		return { turnId, queued };
	}

	async cancelActiveRun(sessionId: string): Promise<boolean> {
		const active = this.activeTurns.get(sessionId);
		if (!active) return false;

		try {
			await this.options.workerClient.cancelTurn(active.worker, {
				sessionId,
				threadId: active.threadId,
				turnId: active.turnId,
			});
		} catch (error) {
			this.options.logger?.warn?.(`Bee cancel publish failed for session ${sessionId}: ${String(error)}`);
		}

		active.controller.abort();
		return true;
	}

	private getQueue(key: string): ConversationQueue {
		let queue = this.queues.get(key);
		if (!queue) {
			queue = new ConversationQueue();
			this.queues.set(key, queue);
		}
		return queue;
	}

	private async process(
		input: BeeResolvedTurn,
		turnId: string,
		onEvent: BeeEventStreamHandler,
		onError?: BeeEventStreamErrorHandler,
	): Promise<void> {
		const controller = new AbortController();
		this.activeTurns.set(input.sessionId, {
			turnId,
			controller,
			worker: input.worker,
			threadId: input.threadId,
		});

		try {
			const request = this.buildRequest(input, turnId);
			await this.options.workerClient.streamTurn(
				input.worker,
				request,
				async (event) => {
					await onEvent(event, { input, request });
				},
				{
					signal: controller.signal,
				},
			);
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			this.options.logger?.error?.(`Bee event stream dispatch failed for session ${input.sessionId}`, message);
			await onError?.(error, { input, turnId });
		} finally {
			this.activeTurns.delete(input.sessionId);
		}
	}

	private buildRequest(input: BeeResolvedTurn, turnId: string): BeeTurnRequest {
		return {
			sessionId: input.sessionId,
			threadId: input.threadId,
			turnId,
			conversation: input.conversation,
			actor: input.actor,
			message: input.message,
			attachments: input.attachments,
		};
	}
}
