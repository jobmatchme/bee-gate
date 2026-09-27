import { beforeEach, describe, expect, it, vi } from "vitest";

const { closeMock, connectMock, drainMock } = vi.hoisted(() => {
	const drainMock = vi.fn(async () => undefined);
	const closeMock = vi.fn(async () => undefined);
	return {
		drainMock,
		closeMock,
		connectMock: vi.fn(async () => ({ drain: drainMock, close: closeMock })),
	};
});

vi.mock("nats", async (importOriginal) => ({
	...(await importOriginal<typeof import("nats")>()),
	connect: connectMock,
}));

import { createBeeTurnStartEnvelope, createNatsBeeClient } from "../src/run-client.js";

describe("createNatsBeeClient", () => {
	beforeEach(() => {
		connectMock.mockClear();
		drainMock.mockReset().mockResolvedValue(undefined);
		closeMock.mockReset().mockResolvedValue(undefined);
	});

	it("reconnects indefinitely by default", async () => {
		await createNatsBeeClient({
			servers: "nats://localhost:4222",
			name: "bee-slack",
		});

		expect(connectMock).toHaveBeenLastCalledWith({
			servers: "nats://localhost:4222",
			name: "bee-slack",
			maxReconnectAttempts: -1,
		});
	});

	it("preserves an explicit reconnect limit, including zero", async () => {
		await createNatsBeeClient({
			servers: ["nats://one:4222", "nats://two:4222"],
			maxReconnectAttempts: 0,
		});

		expect(connectMock).toHaveBeenLastCalledWith({
			servers: ["nats://one:4222", "nats://two:4222"],
			name: undefined,
			maxReconnectAttempts: 0,
		});
	});

	it("force-closes the connection when graceful drain fails", async () => {
		const drainError = new Error("disconnected during drain");
		drainMock.mockRejectedValueOnce(drainError);
		const client = await createNatsBeeClient({ servers: "nats://localhost:4222" });

		await expect(client.close()).rejects.toBe(drainError);
		expect(closeMock).toHaveBeenCalledOnce();
	});
});

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
