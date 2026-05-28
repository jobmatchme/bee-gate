import { describe, expect, it, vi } from "vitest";
import { BeeGatewayEngine } from "../src/engine.js";
import type { BeeRunEvent, BeeWorkerClient, TransportSink } from "../src/types.js";

class FakeWorkerClient implements BeeWorkerClient {
	constructor(private event: BeeRunEvent) {}

	async streamTurn(
		_target: never,
		_request: never,
		onEvent: (event: BeeRunEvent) => Promise<void> | void,
	): Promise<void> {
		await onEvent({
			id: "msg-start",
			type: "event",
			name: "run.started",
			time: new Date().toISOString(),
			sessionId: "session-1",
			turnId: "turn-1",
			from: { kind: "agent", id: "agent" },
			payload: { eventType: "run.started" },
		});
		await onEvent(this.event);
		await onEvent({
			id: "msg-complete",
			type: "event",
			name: "run.completed",
			time: new Date().toISOString(),
			sessionId: "session-1",
			turnId: "turn-1",
			from: { kind: "agent", id: "agent" },
			payload: { eventType: "run.completed" },
		});
	}

	async cancelTurn(): Promise<void> {}
}

describe("BeeGatewayEngine artifacts", () => {
	it("publishes artifact refs through the transport sink", async () => {
		const publishArtifact = vi.fn().mockResolvedValue(undefined);
		const sink: TransportSink<string> = {
			postMessage: vi.fn().mockResolvedValue("message-1"),
			updateMessage: vi.fn().mockResolvedValue(undefined),
			publishArtifact,
		};
		const engine = new BeeGatewayEngine({
			sink,
			workerClient: new FakeWorkerClient({
				id: "msg-artifact",
				type: "event",
				name: "item.appended",
				time: new Date().toISOString(),
				sessionId: "session-1",
				turnId: "turn-1",
				from: { kind: "agent", id: "agent" },
				payload: {
					eventType: "item.appended",
					item: {
						id: "item-artifact",
						kind: "artifact",
						role: "assistant",
						parts: [
							{
								kind: "artifactRef",
								artifactId: "artifact-1",
								name: "report.csv",
								uri: "data:text/csv;base64,Zm9vCg==",
							},
						],
					},
				},
			}),
		});

		engine.dispatch({
			sessionId: "session-1",
			worker: { subject: "worker.test" },
			conversation: { conversationId: "conversation-1" },
			actor: { userId: "user-1" },
			message: { text: "make report" },
			output: { channelId: "C123" },
		});

		await vi.waitFor(() => {
			expect(publishArtifact).toHaveBeenCalledWith(
				{ channelId: "C123" },
				expect.objectContaining({
					artifactId: "artifact-1",
					name: "report.csv",
					uri: "data:text/csv;base64,Zm9vCg==",
				}),
			);
		});
	});

	it("posts an explicit warning when an artifact ref has no transferable payload", async () => {
		const publishArtifact = vi.fn().mockResolvedValue(undefined);
		const postMessage = vi.fn().mockResolvedValue("message-1");
		const sink: TransportSink<string> = {
			postMessage,
			updateMessage: vi.fn().mockResolvedValue(undefined),
			publishArtifact,
		};
		const engine = new BeeGatewayEngine({
			sink,
			workerClient: new FakeWorkerClient({
				id: "msg-artifact",
				type: "event",
				name: "item.appended",
				time: new Date().toISOString(),
				sessionId: "session-1",
				turnId: "turn-1",
				from: { kind: "agent", id: "agent" },
				payload: {
					eventType: "item.appended",
					item: {
						id: "item-artifact",
						kind: "artifact",
						role: "assistant",
						parts: [
							{
								kind: "artifactRef",
								artifactId: "artifact-1",
								name: "report.csv",
							},
						],
					},
				},
			}),
		});

		engine.dispatch({
			sessionId: "session-1",
			worker: { subject: "worker.test" },
			conversation: { conversationId: "conversation-1" },
			actor: { userId: "user-1" },
			message: { text: "make report" },
			output: { channelId: "C123" },
		});

		await vi.waitFor(() => {
			expect(publishArtifact).not.toHaveBeenCalled();
			expect(postMessage).toHaveBeenCalledWith(
				{ channelId: "C123" },
				expect.stringContaining("Artifact upload skipped: artifact reference has neither inline URI nor blob key"),
			);
		});
	});
});
