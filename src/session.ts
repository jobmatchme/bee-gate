import { randomUUID } from "crypto";

export function sanitizeIdentifier(value: string): string {
	return value.replace(/[^a-zA-Z0-9:_-]/g, "_");
}

export function buildConversationId(parts: string[]): string {
	return sanitizeIdentifier(parts.filter(Boolean).join(":"));
}

export function buildSessionKey(prefix: string, conversationId: string): string {
	return sanitizeIdentifier(`${prefix}:${conversationId}`);
}

export function newTurnId(): string {
	return `turn_${randomUUID()}`;
}
