import { describe, expect, it } from "vitest";
import { buildBeeCommandSubject, buildBeeProtocolSubject, buildBeeSessionEventsSubject } from "../src/run-client.js";
import { buildConversationId, buildSessionKey } from "../src/session.js";

describe("bee-gate helpers", () => {
	it("builds sanitized session identifiers", () => {
		const conversationId = buildConversationId(["slack", "C123", "171234"]);
		expect(conversationId).toBe("slack:C123:171234");
		expect(buildSessionKey("bee", conversationId)).toBe("bee:slack:C123:171234");
	});

	it("builds bee protocol subjects", () => {
		expect(buildBeeProtocolSubject("bee.agent.main")).toBe("bee.agent.main.protocol");
		expect(buildBeeCommandSubject("bee.agent.main")).toBe("bee.agent.main.command");
		expect(buildBeeSessionEventsSubject("bee.agent.main", "sess:1")).toBe("bee.agent.main.session.sess_1.event");
	});
});
