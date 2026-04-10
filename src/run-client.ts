import {
	assertValidEnvelope,
	createProtocolHello,
	createTurnStart,
	type Envelope,
	type ProtocolCapabilities,
} from "@jobmatchme/bee-dance-core";
import { JSONCodec, type NatsConnection, type Subscription } from "nats";
import type {
	BeeRunEvent,
	BeeTurnRequest,
	BeeTurnStreamOptions,
	BeeWorkerClient,
	BeeWorkerTargetConfig,
	GatewayActor,
} from "./types.js";
import { BEE_DANCE_PROTOCOL_VERSION } from "./types.js";

const codec = JSONCodec<Envelope>();

const DEFAULT_CAPABILITIES: ProtocolCapabilities = {
	coreVersions: [BEE_DANCE_PROTOCOL_VERSION],
	inputParts: ["text"],
	outputParts: ["text", "status", "artifactRef", "approval"],
	events: [
		"run.started",
		"run.completed",
		"run.failed",
		"item.appended",
		"item.updated",
		"item.completed",
		"approval.requested",
	],
	actions: [],
	extensions: {},
	streaming: true,
};

export interface NatsConnectionOptions {
	servers: string | string[];
	name?: string;
}

export interface BeeSubjectSet {
	protocol: string;
	command: string;
	sessionEvents: string;
}

export function buildBeeProtocolSubject(baseSubject: string): string {
	return `${trimSubject(baseSubject)}.protocol`;
}

export function buildBeeCommandSubject(baseSubject: string): string {
	return `${trimSubject(baseSubject)}.command`;
}

export function buildBeeSessionEventsSubject(baseSubject: string, sessionId: string): string {
	return `${trimSubject(baseSubject)}.session.${sanitizeSubjectToken(sessionId)}.event`;
}

export function buildBeeSubjects(target: BeeWorkerTargetConfig, sessionId: string): BeeSubjectSet {
	return {
		protocol: buildBeeProtocolSubject(target.subject),
		command: buildBeeCommandSubject(target.subject),
		sessionEvents: buildBeeSessionEventsSubject(target.subject, sessionId),
	};
}

export class NatsBeeClient implements BeeWorkerClient {
	private gatewayActor: GatewayActor = {
		userId: "gateway:bee-gate",
		displayName: "bee-gate",
	};

	constructor(private connection: NatsConnection) {}

	async streamTurn(
		target: BeeWorkerTargetConfig,
		request: BeeTurnRequest,
		onEvent: (event: BeeRunEvent) => Promise<void> | void,
		options?: BeeTurnStreamOptions,
	): Promise<void> {
		const subjects = buildBeeSubjects(target, request.sessionId);
		await this.performHandshake(subjects, request, options?.signal);

		const subscription = this.connection.subscribe(subjects.sessionEvents);
		let completionSettled = false;
		let eventTimeoutHandle: ReturnType<typeof setTimeout> | undefined;
		const completion = createDeferred<void>();
		const eventTimeoutMs = target.eventTimeoutMs ?? target.timeoutMs ?? 30_000;
		const clearEventTimeout = () => {
			if (eventTimeoutHandle !== undefined) {
				clearTimeout(eventTimeoutHandle);
				eventTimeoutHandle = undefined;
			}
		};
		const scheduleEventTimeout = (phase: "start" | "completion") => {
			clearEventTimeout();
			eventTimeoutHandle = setTimeout(() => {
				if (completionSettled) return;
				const label = phase === "start" ? "start event" : "completion event";
				completion.reject(new Error(`Timed out waiting for bee ${label} on subject ${subjects.sessionEvents}`));
			}, eventTimeoutMs);
		};

		const consumeTask = this.consumeTurnEvents(subscription, subjects.sessionEvents, request.turnId, onEvent, {
			onTerminal: () => {
				completionSettled = true;
				clearEventTimeout();
				completion.resolve();
			},
			onError: (error) => {
				completionSettled = true;
				clearEventTimeout();
				completion.reject(error);
			},
			onEvent: (event) => {
				if (event.name === "run.started") {
					scheduleEventTimeout("completion");
				}
			},
		});

		try {
			this.connection.publish(
				subjects.command,
				codec.encode(
					createTurnStart({
						sessionId: request.sessionId,
						threadId: request.threadId,
						turnId: request.turnId,
						from: toActorRef(this.gatewayActor),
						to: { kind: "agent", id: target.subject },
						replyTo: null,
						payload: {
							input: [{ kind: "text", text: request.message.text }],
							hints: {
								conversationId: request.conversation.conversationId,
								attachments: request.attachments || [],
								actor: request.actor,
							},
						},
					}),
				),
			);
			scheduleEventTimeout("start");

			if (options?.signal) {
				options.signal.addEventListener(
					"abort",
					() => {
						void this.cancelTurn(target, request);
					},
					{ once: true },
				);
			}

			await completion.promise;
		} finally {
			completionSettled = true;
			clearEventTimeout();
			subscription.unsubscribe();
			await consumeTask;
		}
	}

	async cancelTurn(
		target: BeeWorkerTargetConfig,
		request: Pick<BeeTurnRequest, "sessionId" | "threadId" | "turnId">,
	): Promise<void> {
		const subjects = buildBeeSubjects(target, request.sessionId);
		const cancel: Envelope<{ reason: string }> = {
			id: `msg_${crypto.randomUUID()}`,
			type: "command",
			name: "turn.cancel",
			time: new Date().toISOString(),
			sessionId: request.sessionId,
			threadId: request.threadId,
			turnId: request.turnId,
			from: toActorRef(this.gatewayActor),
			to: { kind: "agent", id: target.subject },
			replyTo: null,
			payload: {
				reason: "user_requested",
			},
		};
		this.connection.publish(subjects.command, codec.encode(cancel));
	}

	async close(): Promise<void> {
		await this.connection.drain();
	}

	private async performHandshake(
		subjects: BeeSubjectSet,
		request: BeeTurnRequest,
		signal?: AbortSignal,
	): Promise<void> {
		if (signal?.aborted) {
			throw new Error("Request aborted");
		}
		const hello = createProtocolHello({
			sessionId: request.sessionId,
			threadId: request.threadId,
			turnId: request.turnId,
			from: toActorRef(this.gatewayActor),
			to: { kind: "agent", id: subjects.command.replace(/\.command$/, "") },
			replyTo: null,
			capabilities: DEFAULT_CAPABILITIES,
		});
		const message = await this.connection.request(subjects.protocol, codec.encode(hello), {
			timeout: 10_000,
		});
		const response = codec.decode(message.data);
		try {
			assertValidEnvelope(response);
		} catch (error) {
			throw new Error(
				`Invalid bee handshake response on ${subjects.protocol}: ${error instanceof Error ? error.message : String(error)}; raw=${JSON.stringify(response)}`,
			);
		}
		if (response.name !== "protocol.welcome") {
			throw new Error(`Expected protocol.welcome, got ${response.name}`);
		}
	}

	private async consumeTurnEvents(
		subscription: Subscription,
		eventSubject: string,
		turnId: string,
		onEvent: (event: BeeRunEvent) => Promise<void> | void,
		callbacks: {
			onTerminal: () => void;
			onError: (error: unknown) => void;
			onEvent: (event: BeeRunEvent) => void;
		},
	): Promise<void> {
		try {
			for await (const message of subscription) {
				const raw = codec.decode(message.data);
				try {
					assertValidEnvelope(raw);
				} catch (error) {
					throw new Error(
						`Invalid bee event envelope on ${eventSubject}: ${error instanceof Error ? error.message : String(error)}; raw=${JSON.stringify(raw)}`,
					);
				}
				if (raw.type !== "event") continue;
				if (raw.turnId !== turnId) continue;
				const event = raw as BeeRunEvent;
				callbacks.onEvent(event);
				await onEvent(event);
				if (event.name === "run.completed" || event.name === "run.failed") {
					callbacks.onTerminal();
					return;
				}
			}
			callbacks.onError(new Error("Bee event subscription ended before terminal event"));
		} catch (error) {
			callbacks.onError(error);
		}
	}
}

export async function createNatsBeeClient(options: NatsConnectionOptions): Promise<NatsBeeClient> {
	const { connect } = await import("nats");
	const connection = await connect({
		servers: options.servers,
		name: options.name,
	});
	return new NatsBeeClient(connection);
}

function trimSubject(subject: string): string {
	return subject.replace(/\.+/g, ".").replace(/^\./, "").replace(/\.$/, "");
}

function sanitizeSubjectToken(value: string): string {
	return value.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function toActorRef(actor: GatewayActor): { kind: "human"; id: string } {
	return {
		kind: "human",
		id: actor.userId,
	};
}

function createDeferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((resolveFn, rejectFn) => {
		resolve = resolveFn;
		reject = rejectFn;
	});
	return { promise, resolve, reject };
}
