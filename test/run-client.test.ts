import { describe, expect, it } from "vitest";
import { createBeeTurnStartEnvelope } from "../src/run-client.js";

describe("createBeeTurnStartEnvelope", () => {
	it("preserves transport and W3C telemetry hints", () => {
		const envelope = createBeeTurnStartEnvelope(
			{ subject: "fabee.agent.pi.default" },
			{
				sessionId: "session-1",
				threadId: "thread-1",
				turnId: "turn-1",
				conversation: { conversationId: "conversation-1", transport: "slack" },
				actor: { userId: "user-1" },
				message: { text: "hello" },
				telemetry: {
					traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					tracestate: "vendor=value",
					baggage: "tenant=test",
				},
			},
		);

		expect(envelope.payload).toMatchObject({
			hints: {
				transport: "slack",
				telemetry: {
					traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					tracestate: "vendor=value",
					baggage: "tenant=test",
				},
			},
		});
	});
});
