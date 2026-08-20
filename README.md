# `@jobmatchme/bee-gate`

`bee-gate` is a transport-neutral gateway runtime for systems that want to send
user input into Bee Dance compatible agents and turn the resulting event stream
back into UI-facing updates.

It sits above `@jobmatchme/bee-dance-core` and below adapter packages such as
Slack, web chat, or other channel-specific integrations.

## What this package does

`bee-gate` provides the runtime pieces that are specific to gateway behavior:

- a normalized gateway contract for inbound user turns
- a NATS-backed Bee Dance client
- a generic engine that processes Bee Dance events
- rendering support for streamed `item.*` updates
- session helpers for turn and conversation tracking
- a local blob store abstraction for attachments and artifacts

## Why this package exists

UI adapters and agent runtimes should not have to agree on every transport and
orchestration detail directly. A gateway layer gives them a stable middle
ground:

- adapters normalize inbound messages into one gateway input model
- gateways speak the Bee Dance protocol to backend workers or sidecars
- adapters only have to implement a small output sink for visible messages and
  artifacts

That keeps the chat surface thin while still allowing richer turn lifecycle
handling than a simple request/response API.

## Design intent

`bee-gate` is intentionally not Slack-specific, web-specific, or agent-specific.
It owns gateway concerns:

- turn submission
- protocol handshake
- event stream consumption
- timeout handling
- cancellation
- UI-facing message progression

It does not own the chat transport itself and it does not execute agent logic.

## Typical usage

Applications usually pair this package with:

- a Bee Dance capable backend reachable over NATS
- a chat or UI adapter that maps user messages into the gateway contract
- a transport sink that knows how to post and update messages in the target UI

## Optional streaming lifecycle

Adapters can opt a resolved turn into richer run delivery without introducing
transport-specific types into the gateway:

```ts
const turn: BeeResolvedTurn = {
  // normal turn fields...
  streaming: {
    enabled: true,
    routeId: "pilot-route",
    presentation: "timeline",
    context: { recipientUserId: "U123", recipientTeamId: "T123" },
  },
};
```

A transport that supports this preference implements all three optional sink
methods: `startStream`, `updateStream`, and `stopStream`. Existing sinks remain
Transports may additionally implement `prepareStreamText` so native limits are
applied consistently to stream completion and normal fallback delivery.
valid and use the legacy `postMessage`/`updateMessage` flow. Stream start
failure also locks that run to legacy delivery. An action update failure moves
the run to degraded delivery: no more action updates are attempted, the stream
is closed best effort with a short generic notice, and the final answer is
posted exactly once as a normal message. A stop failure likewise falls back to
a normal final message.

Bee Dance `action` items are converted to transport-neutral `ActionUpdate`
values. The item id is the action id; ordered text parts provide the title and
optional details; and the last recognized status part (`in_progress`,
`complete`, or `error`) is authoritative. Updates append a new status to the
same item. Duplicate states and regressions from a terminal state back to
`in_progress` are ignored; a later terminal state replaces an earlier one.
Action items are never rendered as normal answer text. Status updates received
before their action metadata are buffered and merged when the item arrives.

The gateway keeps stream calls serialized with the worker event stream.
Streaming delivery state is run-scoped and intentionally not persisted across
process restarts.

## Publishing

The package is intended for public npm publication from GitHub Actions using npm
Trusted Publishing via GitHub OIDC.

## License

MIT
