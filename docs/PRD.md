# simple-im — Product Requirements Document

> **Status:** DRAFT — reconstructed from the "Messaging" sub-session decision. Review for fidelity before implementation.
>
> **Pre-release design note:** This document was written around an Owner→Governor→Agent hierarchy that was superseded before the first public release. There is no owner role and no admin token. The governor is now optional and obtained by election/claim (see README §7 and `skills/governor/SKILL.md`). Without a governor, grants are established by recipient consent alone. References to "owner", `SIMPLE_IM_ADMIN_TOKEN`, `/governors/mint`, and `/admin/rotate-token` below are historical and do not reflect the current implementation.

## Problem & context

`simple-im` was built to give agent fleets reliable agent-to-agent messaging: a name registry, live 1:1 delivery, and a lightweight trust layer. The mechanism it replaced was file-based per-pod inbox/outbox — no name registry, no fan-out, brittle remote delivery.

`simple-im` is a standalone project: a self-hosted HTTP-based instant-messaging hub for agents, independent of any third-party broker or service — a clean service anyone could run.

## Goals

- Give registered agents a simple, reliable way to send 1:1 messages to each other by name.
- A clear trust model with delegated administration.
- Self-hostable and publishable as a standalone service.
- Minimal, correct implementation — no speculative features.

## Trust model

> **Note:** The owner role and admin token described in the original design no longer exist. The current model is described here for reference; see README §3 for the authoritative description.

Two roles:
- **Governor (optional)** — elected by active agents via `POST /governors/claim`; approves grants between agent pairs, mediates messages, and blocks/unblocks pairs. Zero or one governor per hub.
- **Agent** — a registered participant that can send and receive messages within its approved grants.

Without a governor, grants are established by **recipient consent alone** — no third party required. With a governor present, grant requests require governor approval first, then recipient approval.

**Pairwise connection approval:** a grant authorizes messaging between a specific pair of agents (e.g. "agent A ↔ agent B") and carries a temporary or permanent expiry. With a governor, only the governor can issue or revoke grants. Without one, the recipient's `PATCH /grants/requests/{id}` approval establishes the grant directly.

## Functional requirements

- R1. An agent registers with the hub under a **name**. The hub enforces **unique identity** — a name held by a live registration cannot be claimed by another.
- R2. A registered agent MAY send a **1:1 message to another registered agent by name**.
- R3. Delivery is **online-only**: if the recipient is registered and connected, the message is delivered; if offline/unregistered, the sender receives a **clean, explicit failure** — never silent loss.
- R4. Access is token-gated: a governor issues a token (governor-set expiry) that an agent presents to register/act. Expired or invalid tokens are rejected.
- R5. On hub restart, **live session state** (presence, in-flight messages) is invalidated; agents **re-register/announce** to resume. Trust state (tokens, grants) is persisted and survives restart.

## Out of scope

- **No offline / durable delivery** — messages are not buffered for offline recipients (contrast the current file-inbox model, which does buffer).
- **No broadcast / group messaging** — 1:1 only.
- **No durable message delivery** — undelivered messages and live presence reset on restart; re-registration is the recovery path. (Trust state — tokens/grants — is persisted; see the README.)
- **No transport/technology mandate** — the PRD does not specify HTTP framing, storage, or language. That is the implementer's call. The only fixed constraints: HTTP-based, self-hosted, no third-party service.

## Acceptance criteria

- AC1. Two agents register under distinct names; a second registration on an in-use name is rejected.
- AC2. Agent A sends a message to online Agent B by name; B receives it.
- AC3. Agent A sends to an offline/unknown name; A gets an explicit failure response, and nothing is silently dropped or buffered.
- AC4. A token with a short expiry, when used after that expiry, is rejected.
- AC5. After a hub restart, a previously-registered agent must re-register before it can send/receive.

## Open questions (for design / implementer)

- OQ1. Transport — long-poll, SSE, WebSocket, or request/response? (Online-only delivery implies a live-connection model.) *Resolved: SSE.*
- OQ2. Token format; how governors obtain tokens; bootstrap. *Resolved: agents self-issue via `POST /listen`; governors claim via `POST /governors/claim`.*
- OQ3. Identity lifecycle — how a name is released (explicit deregister, connection-drop timeout?). *Resolved: both paths implemented.*
- OQ4. How "online" is determined (heartbeat vs connection liveness?). *Resolved: SSE liveness window (`--liveness-window-secs`).*
- OQ5. Transition strategy. *Out of scope for the hub build itself.*

## Migration / relationship to current model

The file-based per-pod inbox/outbox is the incumbent. `simple-im` adds a name registry + live 1:1 delivery the file model lacks, but DROPS offline buffering (which the file model has). Transition strategy (parallel-run vs cutover) is an open question (OQ5), not decided here.
