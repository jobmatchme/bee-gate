import type { Envelope, ItemPart } from "@jobmatchme/bee-dance-core";

export const BEE_GATE_SPEC_VERSION = "2026-04-08";
export const BEE_DANCE_PROTOCOL_VERSION = "2026-04-02";

export interface BeeWorkerTargetConfig {
	subject: string;
	timeoutMs?: number;
	eventTimeoutMs?: number;
}

export interface AttachmentRef {
	attachmentId: string;
	blobKey: string;
	name?: string;
	title?: string;
	mimeType?: string;
	sizeBytes?: number;
}

export interface ArtifactRef {
	artifactId: string;
	blobKey?: string;
	name?: string;
	title?: string;
	mimeType?: string;
	uri?: string;
	sizeBytes?: number;
}

export interface GatewayActor {
	userId: string;
	userName?: string;
	displayName?: string;
}

export interface GatewayMessage {
	text: string;
}

export interface ConversationContext {
	conversationId: string;
	transport?: string;
}

export interface TransportOutputTarget {
	channelId?: string;
	threadId?: string;
}

export type DeliveryMode = "legacy" | "streaming" | "degraded";

export type ActionStatus = "in_progress" | "complete" | "error";

/** Transport-neutral representation of one action item's latest state. */
export interface ActionUpdate {
	id: string;
	title: string;
	details?: string;
	status: ActionStatus;
}

export interface TransportStreamStart {
	runId: string;
	routeId?: string;
	presentation?: string;
	context?: Record<string, unknown>;
}

export interface TransportStreamResult {
	text: string;
	outcome: "complete" | "error";
}

export interface TransportStreamingPreference {
	enabled: boolean;
	routeId?: string;
	presentation?: string;
	context?: Record<string, unknown>;
}

export interface TransportSink<MessageRef = string, StreamRef = MessageRef> {
	postMessage(target: TransportOutputTarget, text: string): Promise<MessageRef>;
	updateMessage(target: TransportOutputTarget, ref: MessageRef, text: string): Promise<void>;
	publishArtifact?(target: TransportOutputTarget, artifact: ArtifactRef): Promise<void>;
	startStream?(target: TransportOutputTarget, start: TransportStreamStart): Promise<StreamRef>;
	updateStream?(target: TransportOutputTarget, ref: StreamRef, action: ActionUpdate): Promise<void>;
	stopStream?(target: TransportOutputTarget, ref: StreamRef, result: TransportStreamResult): Promise<void>;
	/** Applies transport limits consistently to native and fallback final delivery. */
	prepareStreamText?(text: string): string;
}

export interface GateLogger {
	info?(message: string): void;
	warn?(message: string): void;
	error?(message: string, error?: string): void;
}

export type BlobPutInput =
	| {
			namespace: string;
			name?: string;
			title?: string;
			mimeType?: string;
			data: Uint8Array;
	  }
	| {
			namespace: string;
			name?: string;
			title?: string;
			mimeType?: string;
			filePath: string;
	  };

export interface BlobMaterializedFile {
	path: string;
	filename: string;
	cleanup?: () => Promise<void>;
}

export interface BlobStore {
	put(input: BlobPutInput): Promise<AttachmentRef>;
	putArtifact(input: BlobPutInput): Promise<ArtifactRef>;
	materialize(ref: AttachmentRef | ArtifactRef): Promise<BlobMaterializedFile>;
}

export interface BeeTelemetryCarrier {
	traceparent?: string;
	tracestate?: string;
	baggage?: string;
}

export interface BeeResolvedTurn {
	sessionId: string;
	threadId?: string;
	worker: BeeWorkerTargetConfig;
	conversation: ConversationContext;
	actor: GatewayActor;
	message: GatewayMessage;
	attachments?: AttachmentRef[];
	/** Optional transport-neutral request for richer run delivery. */
	streaming?: TransportStreamingPreference;
	output: TransportOutputTarget;
	telemetry?: BeeTelemetryCarrier;
}

export interface BeeTurnRequest {
	sessionId: string;
	threadId?: string;
	turnId: string;
	conversation: ConversationContext;
	actor: GatewayActor;
	message: GatewayMessage;
	attachments?: AttachmentRef[];
	telemetry?: BeeTelemetryCarrier;
}

export type BeeRunEvent = Envelope<
	| { eventType: "run.started"; workspaceDir?: string }
	| { eventType: "run.completed"; stopReason?: string }
	| { eventType: "run.failed"; error: string }
	| { eventType: "item.appended"; item: { id: string; kind: string; role: string; parts: ItemPart[] } }
	| { eventType: "item.updated"; itemId: string; appendParts?: ItemPart[] }
	| { eventType: "item.completed"; itemId: string }
	| { approvalId: string; scope: string; summary: string; details?: Record<string, unknown> }
>;

export interface BeeTurnStreamOptions {
	signal?: AbortSignal;
}

export interface BeeWorkerClient {
	streamTurn(
		target: BeeWorkerTargetConfig,
		request: BeeTurnRequest,
		onEvent: (event: BeeRunEvent) => Promise<void> | void,
		options?: BeeTurnStreamOptions,
	): Promise<void>;
	cancelTurn(
		target: BeeWorkerTargetConfig,
		request: Pick<BeeTurnRequest, "sessionId" | "threadId" | "turnId">,
	): Promise<void>;
	close?(): Promise<void>;
}
