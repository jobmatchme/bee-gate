import type { ItemPart } from "@jobmatchme/bee-dance-core";
import { ConversationQueue } from "./queue.js";
import { newTurnId } from "./session.js";
import type {
	ArtifactRef,
	BeeResolvedTurn,
	BeeRunEvent,
	BeeTurnRequest,
	BeeWorkerClient,
	BeeWorkerTargetConfig,
	GateLogger,
	TransportSink,
} from "./types.js";

interface ActiveTurn {
	turnId: string;
	controller: AbortController;
	worker: BeeWorkerTargetConfig;
	threadId?: string;
}

export interface BeeGatewayEngineOptions<MessageRef = string> {
	sink: TransportSink<MessageRef>;
	workerClient: BeeWorkerClient;
	logger?: GateLogger;
}

export class BeeGatewayEngine<MessageRef = string> {
	private queues = new Map<string, ConversationQueue>();
	private activeTurns = new Map<string, ActiveTurn>();

	constructor(private options: BeeGatewayEngineOptions<MessageRef>) {}

	dispatch(input: BeeResolvedTurn): void {
		this.getQueue(input.sessionId).enqueue(async () => {
			await this.process(input);
		});
	}

	async stopActiveRun(sessionId: string): Promise<boolean> {
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

	private async process(input: BeeResolvedTurn): Promise<void> {
		const controller = new AbortController();
		const turnId = newTurnId();
		this.activeTurns.set(input.sessionId, {
			turnId,
			controller,
			worker: input.worker,
			threadId: input.threadId,
		});

		let statusRef: MessageRef | undefined;
		let latestText = "_Working..._";
		const itemTexts = new Map<string, string>();

		try {
			const request = this.buildRequest(input, turnId);
			await this.options.workerClient.streamTurn(
				input.worker,
				request,
				async (event) => {
					await this.handleEvent(input, event, itemTexts, {
						getStatusRef: () => statusRef,
						setStatusRef: (value) => {
							statusRef = value;
						},
						getLatestText: () => latestText,
						setLatestText: (value) => {
							latestText = value;
						},
					});
				},
				{
					signal: controller.signal,
				},
			);
		} catch (error) {
			const messageText = error instanceof Error ? error.message : String(error);
			await this.options.sink.postMessage(input.output, `_Gateway error: ${messageText}_`);
			this.options.logger?.error?.(`Bee gateway dispatch failed for session ${input.sessionId}`, messageText);
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

	private async handleEvent(
		input: BeeResolvedTurn,
		event: BeeRunEvent,
		itemTexts: Map<string, string>,
		state: {
			getStatusRef: () => MessageRef | undefined;
			setStatusRef: (value: MessageRef | undefined) => void;
			getLatestText: () => string;
			setLatestText: (value: string) => void;
		},
	): Promise<void> {
		if (event.name === "run.started") {
			state.setStatusRef(await this.options.sink.postMessage(input.output, state.getLatestText()));
			return;
		}

		if (event.name === "run.completed") {
			if (!state.getStatusRef()) {
				state.setStatusRef(await this.options.sink.postMessage(input.output, state.getLatestText()));
			}
			return;
		}

		if (event.name === "run.failed") {
			const errorText = `_Error: ${asRunFailedPayload(event).error}_`;
			if (state.getStatusRef()) {
				await this.options.sink.updateMessage(input.output, state.getStatusRef()!, errorText);
			} else {
				state.setStatusRef(await this.options.sink.postMessage(input.output, errorText));
			}
			return;
		}

		if (event.name === "approval.requested") {
			await this.options.sink.postMessage(
				input.output,
				`Approval requested: ${asApprovalRequestedPayload(event).summary}`,
			);
			return;
		}

		if (event.name === "item.appended") {
			const payload = asItemAppendedPayload(event);
			const text = renderParts(payload.item.parts);
			itemTexts.set(payload.item.id, text);
			if (payload.item.kind === "artifact") {
				const artifact = firstArtifactRef(payload.item.parts);
				if (artifact && this.options.sink.publishArtifact) {
					if (!artifactHasPayload(artifact)) {
						const messageText = "artifact reference has neither inline URI nor blob key";
						this.options.logger?.warn?.(`Bee artifact publish skipped: ${messageText}`);
						await this.options.sink.postMessage(input.output, `${text}\n_Artifact upload skipped: ${messageText}_`);
						return;
					}
					try {
						await this.options.sink.publishArtifact(input.output, artifact);
						return;
					} catch (error) {
						const messageText = error instanceof Error ? error.message : String(error);
						this.options.logger?.warn?.(`Bee artifact publish failed: ${messageText}`);
						await this.options.sink.postMessage(
							input.output,
							`${text}\n_Artifact upload failed: ${messageText}_`,
						);
						return;
					}
				}
				await this.options.sink.postMessage(input.output, text);
				return;
			}
			state.setLatestText(text);
			if (state.getStatusRef()) {
				await this.options.sink.updateMessage(input.output, state.getStatusRef()!, text);
			} else {
				state.setStatusRef(await this.options.sink.postMessage(input.output, text));
			}
			return;
		}

		if (event.name === "item.updated") {
			const payload = asItemUpdatedPayload(event);
			const current = itemTexts.get(payload.itemId) || "";
			const appended = renderParts(payload.appendParts || []);
			const next = current ? `${current}${appended}` : appended;
			itemTexts.set(payload.itemId, next);
			state.setLatestText(next);
			if (state.getStatusRef()) {
				await this.options.sink.updateMessage(input.output, state.getStatusRef()!, next);
			} else {
				state.setStatusRef(await this.options.sink.postMessage(input.output, next));
			}
		}
	}
}

function firstArtifactRef(parts: ItemPart[]): ArtifactRef | undefined {
	const part = parts.find((entry) => entry.kind === "artifactRef");
	if (!part || part.kind !== "artifactRef") return undefined;
	const legacyPart = part as typeof part & { blobKey?: string };
	return {
		artifactId: part.artifactId,
		blobKey: legacyPart.blobKey,
		name: part.name,
		title: part.title,
		mimeType: part.mimeType,
		uri: part.uri,
		sizeBytes: part.sizeBytes,
	};
}

function artifactHasPayload(artifact: ArtifactRef): boolean {
	return !!(artifact.uri || artifact.blobKey);
}

function renderParts(parts: ItemPart[]): string {
	return parts
		.map((part) => {
			if (part.kind === "text") return part.text;
			if (part.kind === "status") return part.status;
			if (part.kind === "artifactRef") return `Artifact: ${part.title || part.name || part.artifactId}`;
			if (part.kind === "approval") return `${part.title}\n${part.summary || ""}`.trim();
			if (part.kind === "choice") return `${part.title}\n${part.summary || ""}`.trim();
			if (part.kind === "form") return part.title;
			if (part.kind === "log") return part.text;
			if (part.kind === "patch" || part.kind === "diff") {
				return part.files.map((entry) => `File: ${entry.path}`).join("\n");
			}
			return JSON.stringify(part.value);
		})
		.filter(Boolean)
		.join("\n");
}

type BeeRunFailedPayload = Extract<BeeRunEvent["payload"], { eventType: "run.failed" }>;
type BeeApprovalRequestedPayload = Extract<BeeRunEvent["payload"], { summary: string }>;
type BeeItemAppendedPayload = Extract<BeeRunEvent["payload"], { eventType: "item.appended" }>;
type BeeItemUpdatedPayload = Extract<BeeRunEvent["payload"], { eventType: "item.updated" }>;

function asRunFailedPayload(event: BeeRunEvent): BeeRunFailedPayload {
	return event.payload as BeeRunFailedPayload;
}

function asApprovalRequestedPayload(event: BeeRunEvent): BeeApprovalRequestedPayload {
	return event.payload as BeeApprovalRequestedPayload;
}

function asItemAppendedPayload(event: BeeRunEvent): BeeItemAppendedPayload {
	return event.payload as BeeItemAppendedPayload;
}

function asItemUpdatedPayload(event: BeeRunEvent): BeeItemUpdatedPayload {
	return event.payload as BeeItemUpdatedPayload;
}
