import type { ItemPart } from "@jobmatchme/bee-dance-core";
import { describe, expect, it, vi } from "vitest";
import { BeeGatewayEngine } from "../src/engine.js";
import type {
	BeeResolvedTurn,
	BeeRunEvent,
	BeeTurnRequest,
	BeeTurnStreamOptions,
	BeeWorkerClient,
	TransportSink,
} from "../src/types.js";

let eventNumber = 0;

function event(name: string, payload: BeeRunEvent["payload"]): BeeRunEvent {
	eventNumber += 1;
	return {
		id: `event-${eventNumber}`,
		type: "event",
		name,
		time: new Date().toISOString(),
		sessionId: "session-1",
		turnId: "turn-1",
		from: { kind: "agent", id: "agent" },
		payload,
	};
}

function started(): BeeRunEvent {
	return event("run.started", { eventType: "run.started" });
}

function completed(): BeeRunEvent {
	return event("run.completed", { eventType: "run.completed" });
}

function message(text: string, id = "answer-1"): BeeRunEvent {
	return event("item.appended", {
		eventType: "item.appended",
		item: { id, kind: "message", role: "assistant", parts: [{ kind: "text", text }] },
	});
}

function messageUpdate(text: string, id = "answer-1"): BeeRunEvent {
	return event("item.updated", {
		eventType: "item.updated",
		itemId: id,
		appendParts: [{ kind: "text", text }],
	});
}

function action(id: string, parts: ItemPart[]): BeeRunEvent {
	return event("item.appended", {
		eventType: "item.appended",
		item: { id, kind: "action", role: "tool", parts },
	});
}

function actionUpdate(id: string, parts: ItemPart[]): BeeRunEvent {
	return event("item.updated", { eventType: "item.updated", itemId: id, appendParts: parts });
}

class SequenceWorkerClient implements BeeWorkerClient {
	constructor(private events: BeeRunEvent[]) {}

	async streamTurn(
		_target: never,
		_request: BeeTurnRequest,
		onEvent: (event: BeeRunEvent) => Promise<void> | void,
	): Promise<void> {
		for (const runEvent of this.events) await onEvent(runEvent);
	}

	async cancelTurn(): Promise<void> {}
}

function input(streaming = false): BeeResolvedTurn {
	return {
		sessionId: "session-1",
		worker: { subject: "worker.test" },
		conversation: { conversationId: "conversation-1" },
		actor: { userId: "user-1" },
		message: { text: "help" },
		output: { channelId: "channel-1", threadId: "thread-1" },
		streaming: streaming
			? {
					enabled: true,
					routeId: "pilot-route",
					presentation: "timeline",
					context: { recipientUserId: "user-1", recipientTeamId: "team-1" },
				}
			: undefined,
	};
}

function legacySink(): TransportSink<string, string> & {
	postMessage: ReturnType<typeof vi.fn>;
	updateMessage: ReturnType<typeof vi.fn>;
} {
	return {
		postMessage: vi.fn().mockResolvedValue("message-1"),
		updateMessage: vi.fn().mockResolvedValue(undefined),
	};
}

function streamingSink(): Required<Pick<TransportSink<string, string>, "startStream" | "updateStream" | "stopStream">> &
	TransportSink<string, string> & {
		postMessage: ReturnType<typeof vi.fn>;
		updateMessage: ReturnType<typeof vi.fn>;
		startStream: ReturnType<typeof vi.fn>;
		updateStream: ReturnType<typeof vi.fn>;
		stopStream: ReturnType<typeof vi.fn>;
		prepareStreamText: ReturnType<typeof vi.fn>;
	} {
	return {
		postMessage: vi.fn().mockResolvedValue("message-1"),
		updateMessage: vi.fn().mockResolvedValue(undefined),
		startStream: vi.fn().mockResolvedValue("stream-1"),
		updateStream: vi.fn().mockResolvedValue(undefined),
		stopStream: vi.fn().mockResolvedValue(undefined),
		prepareStreamText: vi.fn((text: string) => text),
	};
}

async function dispatchAndWait(
	events: BeeRunEvent[],
	sink: TransportSink<string, string>,
	turn: BeeResolvedTurn,
	assertion: () => void,
): Promise<void> {
	new BeeGatewayEngine({ sink, workerClient: new SequenceWorkerClient(events) }).dispatch(turn);
	await vi.waitFor(assertion);
}

describe("BeeGatewayEngine streaming delivery", () => {
	it("keeps old sinks and non-opted-in turns on the legacy working-message flow", async () => {
		const sink = legacySink();
		await dispatchAndWait([started(), message("Final answer"), completed()], sink, input(), () => {
			expect(sink.updateMessage).toHaveBeenCalledWith(input().output, "message-1", "Final answer");
		});

		expect(sink.postMessage).toHaveBeenCalledTimes(1);
		expect(sink.postMessage).toHaveBeenCalledWith(input().output, "_Working..._");
	});

	it("preserves legacy assistant append and update rendering", async () => {
		const sink = legacySink();
		await dispatchAndWait(
			[started(), message("First"), messageUpdate(" and final"), completed()],
			sink,
			input(),
			() => expect(sink.updateMessage).toHaveBeenCalledWith(input().output, "message-1", "First and final"),
		);

		expect(sink.updateMessage).toHaveBeenNthCalledWith(1, input().output, "message-1", "First");
		expect(sink.updateMessage).toHaveBeenNthCalledWith(2, input().output, "message-1", "First and final");
	});

	it("falls back to legacy when an opted-in sink does not implement streaming", async () => {
		const sink = legacySink();
		await dispatchAndWait([started(), message("Compatible answer"), completed()], sink, input(true), () => {
			expect(sink.updateMessage).toHaveBeenCalledWith(input(true).output, "message-1", "Compatible answer");
		});

		expect(sink.postMessage).toHaveBeenCalledWith(input(true).output, "_Working..._");
	});

	it("never renders action items as legacy answer text", async () => {
		const sink = legacySink();
		await dispatchAndWait(
			[
				started(),
				action("tool-1", [
					{ kind: "text", text: "Datei lesen" },
					{ kind: "text", text: "Konfiguration prüfen" },
					{ kind: "status", status: "in_progress" },
				]),
				actionUpdate("tool-1", [{ kind: "status", status: "complete" }]),
				message("Done"),
				completed(),
			],
			sink,
			input(),
			() => expect(sink.updateMessage).toHaveBeenCalledWith(input().output, "message-1", "Done"),
		);

		expect(sink.updateMessage).toHaveBeenCalledTimes(1);
	});

	it("starts an opted-in stream with generic route and recipient context and stops it with the final answer", async () => {
		const sink = streamingSink();
		const turn = input(true);
		await dispatchAndWait([started(), message("**Final**"), completed()], sink, turn, () => {
			expect(sink.stopStream).toHaveBeenCalledWith(turn.output, "stream-1", {
				text: "**Final**",
				outcome: "complete",
			});
		});

		expect(sink.startStream).toHaveBeenCalledWith(
			turn.output,
			expect.objectContaining({
				runId: expect.any(String),
				routeId: "pilot-route",
				presentation: "timeline",
				context: { recipientUserId: "user-1", recipientTeamId: "team-1" },
			}),
		);
		expect(sink.postMessage).not.toHaveBeenCalled();
		expect(sink.updateMessage).not.toHaveBeenCalled();
	});

	it("closes a stream when a run completes without an assistant item", async () => {
		const sink = streamingSink();
		await dispatchAndWait([started(), completed()], sink, input(true), () => {
			expect(sink.stopStream).toHaveBeenCalledWith(input(true).output, "stream-1", {
				text: "_Run completed without a final response._",
				outcome: "complete",
			});
		});

		expect(sink.postMessage).not.toHaveBeenCalled();
	});

	it("uses ordered text fields, allows terminal replacement and rejects duplicate or in-progress regressions", async () => {
		const sink = streamingSink();
		await dispatchAndWait(
			[
				started(),
				action("tool-1", [
					{ kind: "text", text: "Datei lesen" },
					{ kind: "text", text: "Erste Datei" },
					{ kind: "status", status: "ignored" },
					{ kind: "status", status: "in_progress" },
				]),
				actionUpdate("tool-1", [{ kind: "status", status: "complete" }]),
				actionUpdate("tool-1", [{ kind: "status", status: "error" }]),
				actionUpdate("tool-1", [{ kind: "status", status: "error" }]),
				actionUpdate("tool-1", [{ kind: "status", status: "in_progress" }]),
				message("Done"),
				completed(),
			],
			sink,
			input(true),
			() => expect(sink.stopStream).toHaveBeenCalled(),
		);

		expect(sink.updateStream).toHaveBeenCalledTimes(3);
		expect(sink.updateStream).toHaveBeenNthCalledWith(1, input(true).output, "stream-1", {
			id: "tool-1",
			title: "Datei lesen",
			details: "Erste Datei",
			status: "in_progress",
		});
		expect(sink.updateStream).toHaveBeenNthCalledWith(
			2,
			input(true).output,
			"stream-1",
			expect.objectContaining({ id: "tool-1", status: "complete" }),
		);
		expect(sink.updateStream).toHaveBeenNthCalledWith(
			3,
			input(true).output,
			"stream-1",
			expect.objectContaining({ id: "tool-1", status: "error" }),
		);
	});

	it("buffers an action status update that arrives before its append metadata", async () => {
		const sink = streamingSink();
		await dispatchAndWait(
			[
				started(),
				actionUpdate("tool-1", [{ kind: "status", status: "complete" }]),
				action("tool-1", [
					{ kind: "text", text: "Datei lesen" },
					{ kind: "text", text: "Verspätete Metadaten" },
					{ kind: "status", status: "in_progress" },
				]),
				message("Done"),
				completed(),
			],
			sink,
			input(true),
			() => expect(sink.stopStream).toHaveBeenCalled(),
		);

		expect(sink.updateStream).toHaveBeenCalledTimes(1);
		expect(sink.updateStream).toHaveBeenCalledWith(input(true).output, "stream-1", {
			id: "tool-1",
			title: "Datei lesen",
			details: "Verspätete Metadaten",
			status: "complete",
		});
	});

	it("locks the run to legacy delivery when stream start fails", async () => {
		const sink = streamingSink();
		sink.startStream.mockRejectedValueOnce(new Error("not_allowed"));
		sink.prepareStreamText.mockImplementation((text: string) =>
			text === "Fallback answer" ? "Fallback answer [limited]" : text,
		);
		await dispatchAndWait(
			[
				started(),
				action("tool-1", [
					{ kind: "text", text: "Datei lesen" },
					{ kind: "status", status: "in_progress" },
				]),
				message("Fallback answer"),
				completed(),
			],
			sink,
			input(true),
			() =>
				expect(sink.updateMessage).toHaveBeenCalledWith(
					input(true).output,
					"message-1",
					"Fallback answer [limited]",
				),
		);

		expect(sink.startStream).toHaveBeenCalledTimes(1);
		expect(sink.updateStream).not.toHaveBeenCalled();
		expect(sink.stopStream).not.toHaveBeenCalled();
		expect(sink.postMessage).toHaveBeenCalledWith(input(true).output, "_Working..._");
	});

	it("degrades after an action update failure, suppresses later tasks, closes best effort and posts the final normally", async () => {
		const sink = streamingSink();
		sink.updateStream.mockRejectedValueOnce(new Error("rate_limited"));
		sink.prepareStreamText.mockImplementation((text: string) =>
			text === "Reliable answer" ? "Reliable answer [limited]" : text,
		);
		await dispatchAndWait(
			[
				started(),
				action("tool-1", [
					{ kind: "text", text: "Datei lesen" },
					{ kind: "status", status: "in_progress" },
				]),
				action("tool-2", [
					{ kind: "text", text: "Datei schreiben" },
					{ kind: "status", status: "in_progress" },
				]),
				message("Reliable answer"),
				completed(),
			],
			sink,
			input(true),
			() => expect(sink.postMessage).toHaveBeenCalledWith(input(true).output, "Reliable answer [limited]"),
		);

		expect(sink.updateStream).toHaveBeenCalledTimes(1);
		expect(sink.stopStream).toHaveBeenCalledWith(input(true).output, "stream-1", {
			text: "_Live updates ended. The final response follows separately._",
			outcome: "complete",
		});
		expect(sink.postMessage).toHaveBeenCalledTimes(1);
	});

	it("posts the final answer normally when stopping the stream fails", async () => {
		const sink = streamingSink();
		sink.stopStream.mockRejectedValueOnce(new Error("timeout"));
		sink.prepareStreamText.mockImplementation((text: string) =>
			text === "Still delivered" ? "Still delivered [limited]" : text,
		);
		await dispatchAndWait([started(), message("Still delivered"), completed()], sink, input(true), () => {
			expect(sink.postMessage).toHaveBeenCalledWith(input(true).output, "Still delivered [limited]");
		});

		expect(sink.stopStream).toHaveBeenCalledTimes(1);
	});

	it("marks multiple open actions as errors and terminates a failed run", async () => {
		const sink = streamingSink();
		await dispatchAndWait(
			[
				started(),
				action("tool-1", [
					{ kind: "text", text: "Befehl ausführen" },
					{ kind: "status", status: "in_progress" },
				]),
				action("tool-2", [
					{ kind: "text", text: "Datei lesen" },
					{ kind: "status", status: "in_progress" },
				]),
				event("run.failed", { eventType: "run.failed", error: "Worker unavailable" }),
			],
			sink,
			input(true),
			() =>
				expect(sink.stopStream).toHaveBeenCalledWith(input(true).output, "stream-1", {
					text: "_Error: Worker unavailable_",
					outcome: "error",
				}),
		);

		expect(sink.updateStream).toHaveBeenCalledWith(
			input(true).output,
			"stream-1",
			expect.objectContaining({ id: "tool-1", status: "error" }),
		);
		expect(sink.updateStream).toHaveBeenLastCalledWith(
			input(true).output,
			"stream-1",
			expect.objectContaining({ id: "tool-2", status: "error" }),
		);
	});

	it("closes an active stream when the run is aborted before an assistant response", async () => {
		const sink = streamingSink();
		const client: BeeWorkerClient = {
			cancelTurn: vi.fn().mockResolvedValue(undefined),
			async streamTurn(
				_target: never,
				_request: BeeTurnRequest,
				onEvent: (event: BeeRunEvent) => Promise<void> | void,
				options?: BeeTurnStreamOptions,
			): Promise<void> {
				await onEvent(started());
				await new Promise<void>((resolve) => {
					if (options?.signal?.aborted) resolve();
					else options?.signal?.addEventListener("abort", () => resolve(), { once: true });
				});
			},
		};
		const engine = new BeeGatewayEngine({ sink, workerClient: client });
		engine.dispatch(input(true));
		await vi.waitFor(() => expect(sink.startStream).toHaveBeenCalled());
		expect(await engine.stopActiveRun("session-1")).toBe(true);
		await vi.waitFor(() =>
			expect(sink.stopStream).toHaveBeenCalledWith(input(true).output, "stream-1", {
				text: "_Run stopped before a final response._",
				outcome: "error",
			}),
		);
	});
});
