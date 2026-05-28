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

export interface TransportSink<MessageRef = string> {
	postMessage(target: TransportOutputTarget, text: string): Promise<MessageRef>;
	updateMessage(target: TransportOutputTarget, ref: MessageRef, text: string): Promise<void>;
	publishArtifact?(target: TransportOutputTarget, artifact: ArtifactRef): Promise<void>;
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

export interface BeeResolvedTurn {
	sessionId: string;
	threadId?: string;
	worker: BeeWorkerTargetConfig;
	conversation: ConversationContext;
	actor: GatewayActor;
	message: GatewayMessage;
	attachments?: AttachmentRef[];
	output: TransportOutputTarget;
}

export interface BeeTurnRequest {
	sessionId: string;
	threadId?: string;
	turnId: string;
	conversation: ConversationContext;
	actor: GatewayActor;
	message: GatewayMessage;
	attachments?: AttachmentRef[];
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
