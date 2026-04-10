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

## Publishing

The package is intended for public npm publication from GitHub Actions using npm
Trusted Publishing via GitHub OIDC.

## License

MIT
