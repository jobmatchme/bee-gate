import { describe, expect, it, vi } from "vitest";
import { BeeEventStreamEngine } from "../src/event-engine.js";
import type {
	BeeResolvedTurn,
	BeeRunEvent,
	BeeTurnRequest,
	BeeWorkerClient,
	BeeWorkerTargetConfig,
} from "../src/types.js";

const worker: BeeWorkerTargetConfig = { subject: "bee.agent.test" };

function createInput(overrides: Partial<BeeResolvedTurn> = {}): BeeResolvedTurn {
	return {
		sessionId: "session-1",
		worker,
		conversation: {
			conversationId: "web:conversation-1",
			transport: "web",
		},
		actor: {
			userId: "web:operator-1",
			displayName: "Operator 1",
		},
		message: {
			text: "hello",
		},
		output: {},
		...overrides,
	};
}

function createEvent(name: "run.started" | "run.completed", request: BeeTurnRequest): BeeRunEvent {
	return {
		id: `msg-${name}`,
		type: "event",
		name,
		time: new Date().toISOString(),
		sessionId: request.sessionId,
		threadId: request.threadId,
		turnId: request.turnId,
		from: { kind: "agent", id: "bee.agent.test" },
		to: { kind: "human", id: request.actor.userId },
		replyTo: null,
		payload:
			name === "run.started" ? { eventType: "run.started" } : { eventType: "run.completed", stopReason: "done" },
	} as BeeRunEvent;
}

function deferred<T = void>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((resolveFn, rejectFn) => {
		resolve = resolveFn;
		reject = rejectFn;
	});
	return { promise, resolve, reject };
}

describe("BeeEventStreamEngine", () => {
	it("streams Bee events to the dispatch callback", async () => {
		const seen: string[] = [];
		const client: BeeWorkerClient = {
			async streamTurn(_target, request, onEvent) {
				await onEvent(createEvent("run.started", request));
				await onEvent(createEvent("run.completed", request));
			},
			async cancelTurn() {},
		};
		const engine = new BeeEventStreamEngine({ workerClient: client });

		const result = engine.dispatch(createInput(), (event, context) => {
			seen.push(`${event.name}:${context.request.turnId}`);
		});

		expect(result.queued).toBe(false);
		await vi.waitFor(() => {
			expect(seen).toEqual([`run.started:${result.turnId}`, `run.completed:${result.turnId}`]);
		});
	});

	it("queues turns for the same session", async () => {
		const releases = [deferred(), deferred()];
		const started: string[] = [];
		let active = 0;
		let maxActive = 0;
		const client: BeeWorkerClient = {
			async streamTurn(_target, request) {
				active += 1;
				maxActive = Math.max(maxActive, active);
				started.push(request.message.text);
				const index = started.length - 1;
				await releases[index]!.promise;
				active -= 1;
			},
			async cancelTurn() {},
		};
		const engine = new BeeEventStreamEngine({ workerClient: client });

		const first = engine.dispatch(createInput({ message: { text: "first" } }), () => {});
		const second = engine.dispatch(createInput({ message: { text: "second" } }), () => {});

		expect(first.queued).toBe(false);
		expect(second.queued).toBe(true);
		await vi.waitFor(() => {
			expect(started).toEqual(["first"]);
		});

		releases[0]!.resolve();
		await vi.waitFor(() => {
			expect(started).toEqual(["first", "second"]);
		});
		releases[1]!.resolve();
		await vi.waitFor(() => {
			expect(maxActive).toBe(1);
		});
	});

	it("cancels only the active turn", async () => {
		const started = deferred<AbortSignal>();
		const cancelled: Array<Pick<BeeTurnRequest, "sessionId" | "threadId" | "turnId">> = [];
		const client: BeeWorkerClient = {
			async streamTurn(_target, _request, _onEvent, options) {
				started.resolve(options!.signal!);
				await new Promise<void>((resolve) => {
					options!.signal!.addEventListener("abort", () => resolve(), { once: true });
				});
			},
			async cancelTurn(_target, request) {
				cancelled.push(request);
			},
		};
		const engine = new BeeEventStreamEngine({ workerClient: client });

		const result = engine.dispatch(createInput(), () => {});
		const signal = await started.promise;
		const stopped = await engine.cancelActiveRun("session-1");

		expect(stopped).toBe(true);
		expect(signal.aborted).toBe(true);
		expect(cancelled).toEqual([{ sessionId: "session-1", threadId: undefined, turnId: result.turnId }]);
	});
});
