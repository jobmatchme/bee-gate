import type { ItemPart } from "@jobmatchme/bee-dance-core";
import { ConversationQueue } from "./queue.js";
import { newTurnId } from "./session.js";
import type {
	ActionStatus,
	ActionUpdate,
	ArtifactRef,
	BeeResolvedTurn,
	BeeRunEvent,
	BeeTurnRequest,
	BeeWorkerClient,
	BeeWorkerTargetConfig,
	DeliveryMode,
	GateLogger,
	TransportSink,
	TransportStreamResult,
} from "./types.js";

const DEGRADED_STREAM_NOTICE = "_Live updates ended. The final response follows separately._";

interface ActiveTurn {
	turnId: string;
	controller: AbortController;
	worker: BeeWorkerTargetConfig;
	threadId?: string;
}

interface ActionState {
	title: string;
	details?: string;
	status?: ActionStatus;
}

interface ItemState {
	kind: string;
	role: string;
}

interface RunDeliveryState<MessageRef, StreamRef> {
	mode: DeliveryMode;
	started: boolean;
	terminal: boolean;
	finalDelivered: boolean;
	statusRef?: MessageRef;
	streamRef?: StreamRef;
	streamTerminal: boolean;
	latestText: string;
	fallbackReason?: string;
	items: Map<string, ItemState>;
	itemTexts: Map<string, string>;
	pendingActionStatuses: Map<string, ActionStatus>;
	actions: Map<string, ActionState>;
}

export interface BeeGatewayEngineOptions<MessageRef = string, StreamRef = MessageRef> {
	sink: TransportSink<MessageRef, StreamRef>;
	workerClient: BeeWorkerClient;
	logger?: GateLogger;
}

export class BeeGatewayEngine<MessageRef = string, StreamRef = MessageRef> {
	private queues = new Map<string, ConversationQueue>();
	private activeTurns = new Map<string, ActiveTurn>();

	constructor(private options: BeeGatewayEngineOptions<MessageRef, StreamRef>) {}

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
		const state: RunDeliveryState<MessageRef, StreamRef> = {
			mode: "legacy",
			started: false,
			terminal: false,
			finalDelivered: false,
			streamTerminal: false,
			latestText: "_Working..._",
			items: new Map(),
			itemTexts: new Map(),
			pendingActionStatuses: new Map(),
			actions: new Map(),
		};
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
					await this.handleEvent(input, turnId, event, state);
				},
				{ signal: controller.signal },
			);
			if (controller.signal.aborted && !state.terminal) {
				await this.failRun(input, turnId, state, "_Run stopped before a final response._", "aborted");
			}
		} catch (error) {
			const messageText = error instanceof Error ? error.message : String(error);
			if (!state.terminal) {
				const userText = controller.signal.aborted
					? "_Run stopped before a final response._"
					: `_Gateway error: ${messageText}_`;
				try {
					await this.failRun(
						input,
						turnId,
						state,
						userText,
						controller.signal.aborted ? "aborted" : "gateway_error",
					);
				} catch (deliveryError) {
					this.options.logger?.error?.(
						`Bee gateway failure delivery failed for session ${input.sessionId}`,
						String(deliveryError),
					);
				}
			}
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
		turnId: string,
		event: BeeRunEvent,
		state: RunDeliveryState<MessageRef, StreamRef>,
	): Promise<void> {
		if (event.name === "run.started") {
			await this.startDelivery(input, turnId, state);
			return;
		}

		if (event.name === "run.completed") {
			state.terminal = true;
			if (!state.finalDelivered && (state.mode === "streaming" || state.mode === "degraded")) {
				await this.deliverFinal(input, turnId, state, "_Run completed without a final response._", "complete");
			} else if (!state.statusRef && state.mode === "legacy") {
				state.statusRef = await this.options.sink.postMessage(input.output, state.latestText);
			}
			return;
		}

		if (event.name === "run.failed") {
			const errorText = `_Error: ${asRunFailedPayload(event).error}_`;
			await this.failRun(input, turnId, state, errorText, "run_failed");
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
			if (payload.item.kind === "action") {
				await this.appendAction(input, turnId, state, payload.item.id, payload.item.parts);
				return;
			}
			state.pendingActionStatuses.delete(payload.item.id);

			const text = renderParts(payload.item.parts);
			state.items.set(payload.item.id, { kind: payload.item.kind, role: payload.item.role });
			state.itemTexts.set(payload.item.id, text);
			if (payload.item.kind === "artifact") {
				await this.publishArtifact(input, payload.item.parts, text);
				return;
			}
			if (state.finalDelivered) return;
			state.latestText = text;
			if (payload.item.kind === "message" && payload.item.role === "assistant" && state.mode !== "legacy") {
				await this.deliverFinal(input, turnId, state, text, "complete");
			} else if (state.mode === "legacy") {
				await this.deliverLegacyText(input, state, text);
			}
			return;
		}

		if (event.name === "item.updated") {
			const payload = asItemUpdatedPayload(event);
			if (state.actions.has(payload.itemId)) {
				await this.updateAction(input, turnId, state, payload.itemId, payload.appendParts || []);
				return;
			}
			if (
				!state.items.has(payload.itemId) &&
				this.bufferActionStatus(state, payload.itemId, payload.appendParts || [])
			) {
				return;
			}
			if (state.finalDelivered) return;
			const current = state.itemTexts.get(payload.itemId) || "";
			const appended = renderParts(payload.appendParts || []);
			const next = current ? `${current}${appended}` : appended;
			state.itemTexts.set(payload.itemId, next);
			state.latestText = next;
			const item = state.items.get(payload.itemId);
			if (item?.kind === "message" && item.role === "assistant" && state.mode !== "legacy") {
				await this.deliverFinal(input, turnId, state, next, "complete");
			} else if (state.mode === "legacy") {
				await this.deliverLegacyText(input, state, next);
			}
		}
	}

	private async startDelivery(
		input: BeeResolvedTurn,
		turnId: string,
		state: RunDeliveryState<MessageRef, StreamRef>,
	): Promise<void> {
		if (state.started) return;
		state.started = true;
		const sink = this.options.sink;
		const requested = input.streaming?.enabled === true;
		const supported = !!(sink.startStream && sink.updateStream && sink.stopStream);

		if (requested && supported) {
			try {
				state.streamRef = await sink.startStream!(input.output, {
					runId: turnId,
					routeId: input.streaming?.routeId,
					presentation: input.streaming?.presentation,
					context: input.streaming?.context,
				});
				state.mode = "streaming";
				this.logDelivery(input, turnId, state, "stream_started");
				return;
			} catch (error) {
				state.fallbackReason = "start_failed";
				this.logDelivery(input, turnId, state, "stream_start_failed", error);
			}
		} else if (requested) {
			state.fallbackReason = "streaming_unsupported";
			this.logDelivery(input, turnId, state, "streaming_unsupported");
		}

		state.mode = "legacy";
		state.statusRef = await sink.postMessage(input.output, state.latestText);
	}

	private async appendAction(
		input: BeeResolvedTurn,
		turnId: string,
		state: RunDeliveryState<MessageRef, StreamRef>,
		itemId: string,
		parts: ItemPart[],
	): Promise<void> {
		const parsed = actionFromParts(itemId, parts);
		const existing = state.actions.get(itemId);
		if (existing) {
			if (parsed.status) await this.emitAction(input, turnId, state, itemId, existing, parsed.status);
			return;
		}

		const action: ActionState = {
			title: parsed.title,
			details: parsed.details,
		};
		state.actions.set(itemId, action);
		const pendingStatus = state.pendingActionStatuses.get(itemId);
		state.pendingActionStatuses.delete(itemId);
		if (pendingStatus || parsed.status) {
			await this.emitAction(input, turnId, state, itemId, action, pendingStatus || parsed.status);
		}
	}

	private bufferActionStatus(
		state: RunDeliveryState<MessageRef, StreamRef>,
		itemId: string,
		parts: ItemPart[],
	): boolean {
		const status = lastActionStatus(parts);
		if (!status) return false;
		const current = state.pendingActionStatuses.get(itemId);
		if (current === status || (isTerminalStatus(current) && status === "in_progress")) return true;
		state.pendingActionStatuses.set(itemId, status);
		return true;
	}

	private async updateAction(
		input: BeeResolvedTurn,
		turnId: string,
		state: RunDeliveryState<MessageRef, StreamRef>,
		itemId: string,
		parts: ItemPart[],
	): Promise<void> {
		const action = state.actions.get(itemId);
		if (!action) return;
		const status = lastActionStatus(parts);
		if (!status) return;
		await this.emitAction(input, turnId, state, itemId, action, status);
	}

	private async emitAction(
		input: BeeResolvedTurn,
		turnId: string,
		state: RunDeliveryState<MessageRef, StreamRef>,
		itemId: string,
		action: ActionState,
		status: ActionStatus,
	): Promise<void> {
		if (action.status === status || (isTerminalStatus(action.status) && status === "in_progress")) return;
		action.status = status;
		if (state.mode !== "streaming" || state.streamRef === undefined || state.streamTerminal) return;

		const update: ActionUpdate = {
			id: itemId,
			title: action.title,
			details: action.details,
			status,
		};
		try {
			await this.options.sink.updateStream!(input.output, state.streamRef, update);
			this.logDelivery(input, turnId, state, "action_updated");
		} catch (error) {
			state.mode = "degraded";
			state.fallbackReason = "action_update_failed";
			this.logDelivery(input, turnId, state, "stream_degraded", error);
		}
	}

	private async markOpenActionsFailed(
		input: BeeResolvedTurn,
		turnId: string,
		state: RunDeliveryState<MessageRef, StreamRef>,
	): Promise<void> {
		for (const [itemId, action] of state.actions) {
			if (!isTerminalStatus(action.status)) {
				await this.emitAction(input, turnId, state, itemId, action, "error");
			}
		}
	}

	private async deliverLegacyText(
		input: BeeResolvedTurn,
		state: RunDeliveryState<MessageRef, StreamRef>,
		text: string,
	): Promise<void> {
		const deliveryText = input.streaming?.enabled ? (this.options.sink.prepareStreamText?.(text) ?? text) : text;
		if (state.statusRef) {
			await this.options.sink.updateMessage(input.output, state.statusRef, deliveryText);
		} else {
			state.statusRef = await this.options.sink.postMessage(input.output, deliveryText);
		}
	}

	private async deliverFinal(
		input: BeeResolvedTurn,
		turnId: string,
		state: RunDeliveryState<MessageRef, StreamRef>,
		text: string,
		outcome: TransportStreamResult["outcome"],
	): Promise<void> {
		if (state.finalDelivered) return;
		const finalText = input.streaming?.enabled ? (this.options.sink.prepareStreamText?.(text) ?? text) : text;
		state.latestText = finalText;

		if (state.mode === "legacy" || state.streamRef === undefined) {
			if (state.statusRef) {
				await this.options.sink.updateMessage(input.output, state.statusRef, finalText);
			} else {
				state.statusRef = await this.options.sink.postMessage(input.output, finalText);
			}
			state.finalDelivered = true;
			return;
		}

		const wasDegraded = state.mode === "degraded";
		const streamText = wasDegraded ? DEGRADED_STREAM_NOTICE : finalText;
		let stopFailed = false;
		if (!state.streamTerminal) {
			try {
				await this.options.sink.stopStream!(input.output, state.streamRef, { text: streamText, outcome });
			} catch (error) {
				stopFailed = true;
				state.mode = "degraded";
				state.fallbackReason = "stop_failed";
				this.logDelivery(input, turnId, state, "stream_stop_failed", error);
			} finally {
				state.streamTerminal = true;
			}
		}

		if (wasDegraded || stopFailed) {
			await this.options.sink.postMessage(input.output, finalText);
		}
		state.finalDelivered = true;
		this.logDelivery(input, turnId, state, "final_delivered");
	}

	private async failRun(
		input: BeeResolvedTurn,
		turnId: string,
		state: RunDeliveryState<MessageRef, StreamRef>,
		text: string,
		reason: string,
	): Promise<void> {
		if (state.terminal && state.finalDelivered) return;
		state.terminal = true;
		state.fallbackReason ||= reason;
		await this.markOpenActionsFailed(input, turnId, state);
		await this.deliverFinal(input, turnId, state, text, "error");
	}

	private async publishArtifact(input: BeeResolvedTurn, parts: ItemPart[], text: string): Promise<void> {
		const artifact = firstArtifactRef(parts);
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
				await this.options.sink.postMessage(input.output, `${text}\n_Artifact upload failed: ${messageText}_`);
				return;
			}
		}
		await this.options.sink.postMessage(input.output, text);
	}

	private logDelivery(
		input: BeeResolvedTurn,
		turnId: string,
		state: RunDeliveryState<MessageRef, StreamRef>,
		event: string,
		error?: unknown,
	): void {
		const entry = {
			event,
			runId: turnId,
			routeId: input.streaming?.routeId,
			deliveryMode: state.mode,
			streamRef: state.streamRef === undefined ? undefined : String(state.streamRef),
			streamMessageTs: state.streamRef === undefined ? undefined : String(state.streamRef),
			taskCount: state.actions.size,
			fallbackReason: state.fallbackReason,
			error: error === undefined ? undefined : String(error),
		};
		const message = JSON.stringify(entry);
		if (error === undefined) this.options.logger?.info?.(message);
		else this.options.logger?.warn?.(message);
	}
}

function actionFromParts(id: string, parts: ItemPart[]): ActionUpdate {
	const texts = parts.filter((part): part is Extract<ItemPart, { kind: "text" }> => part.kind === "text");
	return {
		id,
		title: texts[0]?.text || "Action",
		details:
			texts.length > 1
				? texts
						.slice(1)
						.map((part) => part.text)
						.join("\n")
				: undefined,
		status: lastActionStatus(parts) || "in_progress",
	};
}

function lastActionStatus(parts: ItemPart[]): ActionStatus | undefined {
	for (let index = parts.length - 1; index >= 0; index -= 1) {
		const part = parts[index];
		if (part.kind === "status" && isActionStatus(part.status)) return part.status;
	}
	return undefined;
}

function isActionStatus(status: string): status is ActionStatus {
	return status === "in_progress" || status === "complete" || status === "error";
}

function isTerminalStatus(status: ActionStatus | undefined): boolean {
	return status === "complete" || status === "error";
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
