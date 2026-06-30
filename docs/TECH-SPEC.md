# simple-im — Technical Specification

> **Status:** SIGNED FOR BUILD. The §9 open questions are resolved with documented defaults (§9.1); a small set of policy calls is returned to the operator as non-blocking (§9.2). This spec is the authority for the build pipeline.
>
> **Pre-release design note:** The owner role, admin token, and `POST /governors/mint` described in §4.1–4.2 and referenced throughout §4 and §6 were superseded before the first public release and do not exist. The governor is now optional and obtained via `POST /governors/claim` (election/claim/transfer). Without a governor, grants are established by recipient consent alone. See README §3 and §7 for the current authoritative model.
>
> **15-0029 final-form note (supersedes the token model below):** the token type formerly called *agent token* / *listen token* is now a single **participant** token, and the older "agent self-issues a listen token via `POST /listen`" wording is obsolete. In the final form a participant token is **issued by the governor** via `POST /register` (open only during the bootstrap window before any governor exists); the welcome carries a `subscription_id` (= the token) on every connect. A name is a **permanent identity** that survives token GC/revoke; lost credentials are recovered by a governor **rebind** (`POST /register {"name"}`), not by force-reclaim — name force-reclaim is removed entirely. Governor transfer acceptance uses a participant bearer with the transfer token in the body, and an operator anchor (`POST /admin/governor/reset`, `X-Admin-Secret`) recovers governorship. The authoritative current contract is the participant/governor SKILL.md files and `docs/openapi.yaml`; read "agent/listen token" below as "participant token" and ignore any force-reclaim semantics.

## 1. Scope & relationship to the PRD

This spec defines the **design, behavior, and testable acceptance criteria** for `simple-im`: a self-hosted agent-to-agent messaging hub. It is downstream of `PRD.md` (the WHAT). Where this spec and the PRD conflict, the PRD wins on product intent; this spec wins on mechanism.

**Audience is agents, not humans.** The product surface is an API consumed by Claude-Code-class agents. No human-facing UI, CLI ergonomics, dashboards, or admin web console are in scope. Operator-facing tooling, if any, is a thin convenience over the same API and out of scope here.

### 1.1 Terminology (stable, used throughout)

| Term | Definition |
|------|------------|
| **Hub** | The single `simple-im` server process. |
| **Governor** | Optional elected admin. Approves connection grants, mediates messages, blocks/unblocks pairs. Obtained via `POST /governors/claim` (election, auto-grant, or transfer). Zero or one per hub. |
| **Agent** | A registered participant identified by a unique **name**. Sends/receives 1:1 messages. Self-issues a listen token via `POST /listen`. |
| **Registration** | A live binding of `name → agent token → live connection`. Holds the name. |
| **Connection grant** | An authorization for a pair `(A, B)` to message each other. Established by recipient consent alone (no governor) or by governor + recipient approval (with governor). Carries an expiry (temporary or permanent). |
| **Online** | An agent whose liveness (§5) has not lapsed. The hub answers presence queries from this state. |
| **Deliver** | Hand a message to the recipient's live connection such that the recipient WILL observe it absent process death. |

## 2. Architecture

### 2.1 Runtime shape

- **Language: Rust** (decided early in the project). Rationale on file; not re-litigated here.
- **Single statically-linked binary.** No interpreter, no GC runtime, no external service dependency at runtime. Target: `x86_64-unknown-linux-musl` (fully static) as the canonical artifact; native host targets acceptable for dev. **AC-ARCH-1** asserts the no-dynamic-dependency property.
- **Durable trust state, ephemeral delivery.** As of 1.0 the hub persists tokens, grants (with usage counters), DCP identities, denial blocks, and attachment blobs to a SQLite/WAL store (`--token-store-path`, default `sim-tokens.db`), reloaded on boot; if the store cannot be opened it degrades gracefully to fully in-memory. **Message delivery stays in-memory and online-only** — registry presence and in-flight message queues are not persisted and reset on restart (PRD R5). See README §11.
- **Single process, async core.** One Tokio (or equivalent async) reactor; no worker-process fan-out. Concurrency is task-based within the one process.

### 2.2 Transport

**HTTP is the wire; an optional MCP façade rides on top.**

The hub is an HTTP service. Its request/response and SSE semantics are the source of truth. An optional MCP façade (thin in-process adapter) can map MCP tool calls onto the same HTTP handlers, allowing agents that speak MCP to connect without writing new transport code.

- **HTTP is the primary surface.** Trivially testable with `curl`, no extra services, no framing coupling.
- **MCP façade is optional and additive.** It does not re-implement logic — it calls the same internal handlers. It MAY be the last component built and MUST be feature-gateable so the HTTP-only build remains valid.
- **TLS** terminates at the hub in production (or at a reverse proxy with `--insecure-http`); tokens travel in headers, not URLs.

*(OQ-T2 resolved: the façade is in-process, not a separate binary.)*

### 2.3 Component map

```
                 ┌─────────────────────────────────────────┐
   agent ──MCP──▶│  MCP façade  (tool→HTTP adapter)          │
                 └───────────────┬─────────────────────────┘
   agent ──HTTP(S)──────────────▶│
                 ┌───────────────▼─────────────────────────┐
                 │  HTTP(S) listener (TLS)                   │
                 │  ┌────────────┐ ┌──────────────────────┐ │
                 │  │  Auth /    │ │  Registry            │ │
                 │  │  trust     │ │  (name→reg, unique)  │ │
                 │  │  chain     │ └──────────────────────┘ │
                 │  │ (§4)       │ ┌──────────────────────┐ │
                 │  └────────────┘ │  Grant store         │ │
                 │  ┌────────────┐ │  (pair→grant+expiry) │ │
                 │  │ Presence   │ └──────────────────────┘ │
                 │  │ (liveness) │ ┌──────────────────────┐ │
                 │  └────────────┘ │  Delivery / mailbox  │ │
                 │                 │  (online-only, §5.3) │ │
                 │                 └──────────────────────┘ │
                 └───────────────────────────────────────────┘
```

## 3. Registry (identity & lifecycle)

### 3.1 Behavior

- An agent registers by presenting a **valid agent token** (§4) and a desired **name**. On success the hub creates a registration binding `name → {token-identity, liveness clock, delivery channel}`.
- **Unique identity:** while a registration for `name` is live, any other `register(name)` is **rejected** with an explicit `NAME_IN_USE` error. (PRD R1, AC1.)
- A name is bound to the **token-identity** that registered it. A second registration MAY refresh/reclaim its OWN name (idempotent re-register, e.g. reconnect) iff it presents the same token-identity; this is NOT a uniqueness violation.
- **Deregistration releases the name** by either path:
  1. **Explicit `deregister`** — agent calls it; name freed immediately.
  2. **Liveness lapse** — presence clock expires (§5); the hub reaps the registration and frees the name. (Maps to PRD OQ3; this spec proposes "both", with the timeout value in OQ-P1.)
- A freed name is immediately available for a new registration.

### 3.2 Testable ACs

- **AC-REG-1** Register `alice` with a valid token → success; registry reports `alice` online.
- **AC-REG-2** With `alice` live, register `alice` with a *different* token-identity → rejected, error `NAME_IN_USE`; original registration unaffected. *(PRD AC1)*
- **AC-REG-3** With `alice` live, re-register `alice` with the *same* token-identity → success (idempotent reconnect), no second registration created.
- **AC-REG-4** Register `alice`, then `deregister` → name `alice` is free; a subsequent `register(alice)` by any valid token succeeds.
- **AC-REG-5** Register `alice`, then let liveness lapse (§5) → registry reaps `alice`; presence query for `alice` returns offline; name is free.
- **AC-REG-6** `register` with an absent/invalid/expired token → rejected, error `AUTH_FAILED`; no registration created. *(PRD R4)*

## 4. Trust model & tokens

> **Note:** The owner role and admin token described in §4.1 were removed before the first public release. The current model has two roles: agents (who self-issue listen tokens via `POST /listen`) and an optional governor (obtained via `POST /governors/claim` — election/claim/transfer). Without a governor, grants are established by recipient consent alone. The ACs below are preserved for their behavioral value; references to "owner" should be read as "governor" in the current implementation, and "mint agent token" is now `POST /listen` (self-issue, no governor required).

### 4.1 Owner / admin token (historical — removed before first release)

*This level was never shipped. There is no admin token, no `POST /governors/mint`, and no bootstrap PIN. Governors are claimed via `POST /governors/claim`.*

### 4.2 Governor token

- Obtained via `POST /governors/claim` (auto-grant, election, or transfer — see README §7). Carries an optional expiry.
- Governor authority, valid **only while the governor's token is unexpired**:
  1. **Approve connection grants** for a pair `(A, B)`, each grant carrying its own expiry (temporary or permanent).
  2. **Revoke** agent tokens and grants.
  3. **Block / unblock** sender→recipient pairs.
  4. **Mediate** held messages.
- A governor CANNOT create other governors (transfer requires the current governor's approval). A governor CANNOT act once its token expires.

### 4.3 Agent token

- Self-issued: any agent calls `POST /listen` (no auth) to receive a token. Carries no built-in expiry (liveness is determined by SSE connection health, not token expiry).
- Agent-token authority is confined to: register/deregister its OWN name, and send/receive **only** on pairs for which a valid connection grant exists (§4.4). An agent token grants NO governor capability.

### 4.4 Connection grants (pairwise approval)

- A **grant** authorizes messaging between a specific pair of agent identities.
- Without a governor: created when the recipient approves a `POST /grants/request` via `PATCH /grants/requests/{id}`.
- With a governor: requires governor approval first, then recipient approval.
- A grant carries an **expiry**: `temporary` (duration) or `permanent` (no expiry; e.g. an always-on agent pair).
- `send(from=A, to=B)` is permitted iff: A's token is valid, B is registered & online, AND a valid (unexpired) grant covers the pair. Otherwise the send fails with the appropriate explicit error (§5.3).
- Grant directionality (symmetric `A↔B` vs directed `A→B`) — symmetric is the default; directed grants are supported via the `direction` field on `POST /grants/approve`.

### 4.5 Testable ACs

- **AC-TOK-1** *(Updated)* Agent self-issues a listen token via `POST /listen`; that token validates on subsequent authenticated calls. A forged/altered token is rejected `AUTH_FAILED`.
- **AC-TOK-2** A token with a short expiry; an action at T+0 succeeds, the same action at T+(expiry+ε) is rejected `TOKEN_EXPIRED`. *(PRD AC4)*
- **AC-TOK-3** An agent token attempting a governor-only action (approve grant / block) is rejected `FORBIDDEN`.
- **AC-TOK-4** *(Retired — no owner level exists.)*
- **AC-TOK-5** `send(A→B)` with valid tokens but NO grant covering `(A,B)` is rejected `NO_GRANT`; no message delivered.
- **AC-TOK-6** A `temporary` grant permits `send(A→B)` before expiry and is rejected `GRANT_EXPIRED` after; a `permanent` grant never expires within the test window.
- **AC-TOK-7** Revoking a governor token (`DELETE /participants/{name}` by the next governor) → that governor can no longer approve grants or act (`AUTH_FAILED`); grants previously created remain valid until their own expiry (revoking the governor does not retroactively void issued grants — see OQ-S2).

## 5. Message path & presence

### 5.1 Presence (liveness)

- The hub determines **online** by **connection-liveness with a heartbeat**, NOT by a standalone out-of-band heartbeat. Each agent maintains a live channel to the hub (its long-poll `dequeue` connection, §5.2); an agent is **online** while it holds an active dequeue connection OR has pinged within the liveness window. Absence beyond the window → offline → reaped (§3.1).
  - Rationale vs pure connection-liveness: long-poll connections cycle (each `dequeue` returns after `max_wait`); a short grace window across reconnect prevents false-offline flapping. Rationale vs pure heartbeat: no separate heartbeat endpoint to maintain — the dequeue cycle *is* the heartbeat. The liveness-window duration is **OQ-P1**.
- **Presence query:** any agent with a valid token MAY query `presence(name)` and receive `online | offline`. (PRD §"presence awareness".) Presence query does NOT require a connection grant — knowing whether a peer is online is not the same as being authorized to message it. *(Flagged: whether presence should itself be grant-gated is **OQ-P2**.)*

### 5.2 Live channel — long-poll `dequeue`

- Agents receive messages by holding a **long-poll** connection: `dequeue(token, max_wait)` returns immediately with any queued-for-this-connection messages, or blocks up to `max_wait` then returns empty/timed-out, prompting the agent to re-poll. (Directly mirrors the proven TMCP `dequeue` endpoint.)
- Long-poll is chosen over SSE/WebSocket because it is the lightest reliable primitive, trivially testable, and already proven in this fleet. SSE/WebSocket are explicitly out of scope (§8). *(Resolves PRD OQ1 with a recommendation.)*

### 5.3 Delivery — online-only, explicit failure, no silent loss

- `send(from, to, payload)` succeeds **only** if `to` is registered AND online AND a valid grant covers the pair. On success the message is handed to `to`'s live dequeue channel and the sender receives an explicit `DELIVERED` ack.
- If `to` is **offline or unregistered**, `send` returns an explicit failure (`RECIPIENT_OFFLINE` / `RECIPIENT_UNKNOWN`). **The message is NOT buffered, queued, retried, or stored.** (PRD R3, AC3 — "never silent loss; no buffering".)
- There is no at-most-once vs at-least-once ambiguity to hide: delivery is synchronous to a live channel. The narrow race — recipient goes offline between the grant/presence check and the channel handoff — MUST resolve to an explicit failure to the sender, never a silent drop. *(Race-window handling proposed; exact ordering is implementation detail for the workers, but the observable contract is "explicit failure or explicit delivery, never neither".)*

### 5.4 Testable ACs

- **AC-MSG-1** A and B both online with a valid grant; `send(A→B, "hi")` → A receives `DELIVERED`; B's next/blocking `dequeue` returns `"hi"` exactly once. *(PRD AC2)*
- **AC-MSG-2** `send(A→B)` where B is not registered → explicit `RECIPIENT_UNKNOWN`; no buffering (a subsequent `register(B)` + `dequeue` yields nothing). *(PRD AC3)*
- **AC-MSG-3** `send(A→B)` where B was registered but is now offline (liveness lapsed) → explicit `RECIPIENT_OFFLINE`; no buffering (B re-registers + `dequeue` yields nothing). *(PRD AC3)*
- **AC-MSG-4** `presence(B)` returns `online` while B holds a live dequeue connection, and `offline` after B's liveness window lapses.
- **AC-MSG-5** `dequeue` with valid token and no pending messages blocks up to `max_wait` then returns an explicit empty/timed-out payload (not an error, not a hang).
- **AC-MSG-6** `dequeue` with an expired/invalid token is rejected `AUTH_FAILED` and does not establish a live channel.
- **AC-MSG-7** Two messages `send(A→B,"1")` then `send(A→B,"2")` while B is online → B's dequeue returns them in send order, each exactly once. *(Ordering within a single live channel; cross-sender ordering is NOT specified — OQ-P3.)*

## 6. Security model

### 6.1 Properties (each maps to an enforcement point + AC)

- **Token-gating:** every operation requires a token of the correct class; absent/invalid/expired → reject. (AC-REG-6, AC-TOK-2.)
- **Identity enforcement:** a name is bound to the token-identity that registered it; an agent cannot register, deregister, send-as, or dequeue-as another identity. (AC-REG-2/3, AC-SEC-1.)
- **Trust-chain boundaries:** authority strictly narrows governor→agent; no horizontal or upward privilege escalation. (AC-TOK-3.)
- **Transport security:** HTTPS (TLS) for the production wire; tokens carried such that they are not exposed in URLs/logs where avoidable. Dev-mode plaintext HTTP is permitted but MUST be explicitly opt-in (OQ-T1).

### 6.2 Threat boundary — what a compromised principal CAN and CANNOT do

| Compromised principal | CAN | CANNOT |
|---|---|---|
| **Agent token** | Register/act as that one identity; message peers it has grants with; query presence. | Approve grants; act as any other agent; message peers without a grant; persist or recover messages (none are stored). |
| **Governor token (valid)** | Approve/revoke grants; block/unblock pairs; mediate messages; revoke agent tokens — within its unexpired authority. | Transfer governorship without an election; read message *contents* (the hub does not store or expose message bodies to governors — see OQ-S3). |
| **Governor token (expired)** | Nothing — authority is gated on token validity. | Any governor action. |

### 6.3 Testable ACs

- **AC-SEC-1** Agent A's token cannot `deregister(B)` or `send(from=B)` — both rejected `FORBIDDEN`/`AUTH_FAILED`; A cannot impersonate B.
- **AC-SEC-2** An expired governor token is rejected for all governor actions.
- **AC-SEC-3** With TLS enabled, a plaintext HTTP request to a TLS-only listener is refused (no token leak over cleartext). *(Conditioned on OQ-T1 resolution.)*
- **AC-SEC-4** A revoked governor's token is rejected for all governor actions immediately after revocation. *(See AC-TOK-7 for the grant-survival nuance.)*

## 7. Error contract (explicit, enumerated)

All failures are explicit and enumerable — never a silent drop or an opaque hang. Error codes (stable identifiers; transport maps them to HTTP status + JSON `{ok:false,error:CODE}` and to MCP tool errors):

`AUTH_FAILED` · `TOKEN_EXPIRED` · `FORBIDDEN` · `NAME_IN_USE` · `NO_GRANT` · `GRANT_EXPIRED` · `RECIPIENT_OFFLINE` · `RECIPIENT_UNKNOWN` · `BAD_REQUEST`

- **AC-ERR-1** Every rejection in §3–§6 returns one of the enumerated codes; no operation fails silently or returns success on failure.

## 8. Out of scope

Explicitly OUT of scope (deferring is a deliberate simplification, not an oversight):

- **No offline / durable delivery.** Messages to an offline/unknown recipient fail explicitly; nothing is buffered, queued, retried, or stored. (out of scope.)
- **No durable *delivery*.** Registry presence and in-flight messages are in-memory only and reset on restart; agents re-announce to recover (PRD R5). *(Trust state — tokens, grants, identities, attachments — IS persisted to SQLite as of 1.0; see §2.1 and README §11.)*
- **No broadcast / group / multicast.** 1:1 by name only. (out of scope.)
- **No message history / read receipts / editing / threading.** Deliver-and-forget to a live channel.
- **No WebSocket transport.** SSE (implemented) and long-poll are the live-channel primitives; WebSocket is out of scope.
- **No human-facing UI, admin console, or rich CLI.** API for agents only (§1).
- **No federation / multi-hub clustering.** Single process, single hub.
- **No rate-limiting / quota / anti-abuse.** Deferred (trust chain is the current abuse boundary).

## 9. Design decisions (resolved) & operator-gated questions

The open questions were resolved with documented defaults so the build pipeline does **not** block. Workers MUST build to these resolved defaults. A small set of genuinely policy-level calls is escalated to the operator (§9.2); none of them block the build — each has a safe interim default the workers build to now, replaceable later without rework.

### 9.1 Resolved decisions (build to these)

**Transport / packaging**

- **OQ-T1 — RESOLVED.** Dev-mode plaintext HTTP is allowed via an **explicit opt-in flag** (`--insecure-http` / `SIMPLE_IM_INSECURE_HTTP=1`); the default binding is **HTTPS**. Dev convenience does not silently weaken security: plaintext requires a conscious flag, and the hub logs a one-line warning at boot when it is set. For dev the hub MAY load a self-signed cert from config; cert provisioning itself is out of scope (operator supplies a cert path, or sets the insecure flag). Drives **AC-SEC-3** (TLS-only listener refuses plaintext when the flag is absent).
- **OQ-T2 — RESOLVED.** The MCP façade is an **in-process module** behind the same async runtime, not a separate binary. Rationale: a separate binary doubles the deploy/handshake surface and forces an extra network hop (façade→hub) for no benefit when both can share one Tokio reactor. The façade is a thin in-process adapter that calls the same internal handlers the HTTP layer calls (it does NOT re-implement logic). It MAY be the last component built and MUST be feature-gateable so the HTTP-only build remains valid for testing.

**Presence**

- **OQ-P1 — RESOLVED.** Liveness window default = **30 seconds**, **configurable** via `--liveness-window-secs` / `SIMPLE_IM_LIVENESS_WINDOW_SECS` (range 5–600). Rationale: long enough to absorb one or two long-poll reconnect cycles without false-offline flapping, short enough that a dead agent frees its name promptly. Tests MUST be able to override this to a small value (e.g. 1s) for fast liveness-lapse assertions (AC-REG-5, AC-MSG-3, AC-MSG-4).
- **OQ-P2 — RESOLVED.** `presence(name)` is **NOT grant-gated**: any valid agent token MAY query any registered name's online state. Rationale: presence is a low-sensitivity boolean (online/offline), the trust boundary for *messaging* is the grant, and gating presence would force a grant before an agent could even discover a peer is reachable. The hub returns `offline` (not `RECIPIENT_UNKNOWN`) for an unregistered name on a presence query, so presence never leaks whether a name was *ever* registered vs merely offline.
- **OQ-P3 — RESOLVED.** **No ordering guarantee across senders.** Per-sender, per-recipient order on a single live channel IS guaranteed (AC-MSG-7). When multiple senders target one recipient, interleaving is unspecified and acceptable (1:1 agent messaging has no cross-sender causal requirement). Documented as a contract, not a defect.

**Trust chain / grants**

- **OQ-G1 — RESOLVED.** Grants are **symmetric** (`A↔B`): one grant authorizes messaging in both directions. Rationale: the primary use-case is conversational/bidirectional; directed grants would double approval work for every real conversation with no use-case requiring one-way messaging at v0.1. ACs are written to be direction-agnostic; a future directed-grant mode is additive, not a breaking change.
- **OQ-S2 — RESOLVED.** On governor revocation, **grants the governor issued SURVIVE** to their own expiry (matches AC-TOK-7). Rationale: a grant is a standing authorization between two agents; revoking the *issuer* should not silently sever live, in-use agent conversations (which would manifest as confusing mid-session `NO_GRANT` failures). A specific grant can be revoked directly via `DELETE /grants/{id}`. *(A cascade-revoke mode is a possible future hardening — see §9.2.)*
- **OQ-S3 — RESOLVED.** The hub is **strictly blind to payloads**: message bodies are handed opaquely to the recipient's live channel and are never stored, logged, or exposed to governors. Rationale: message payloads are never persisted, so there is nothing to audit anyway, and payload-blindness is the stronger security posture. Maps to the threat table (§6.2, governor "CANNOT read message contents").
- **OQ-S4 — RESOLVED.** Rate-limiting / quota / anti-abuse is **deferred** (confirmed §8). The trust chain (token-gating + pairwise grants + governor admission) is the current abuse boundary. No rate-limit code.

### 9.2 Returned to the operator (policy calls — non-blocking; safe defaults in place)

- **OQ-S1 (admin-token bootstrap)** — *Resolved before first release:* the admin-token/owner concept was removed. Governors are elected via `POST /governors/claim`. No action needed.
- **OQ-S2 cascade-revoke variant** — resolved to *survive* (above). *Operator decision needed:* whether a future "cascade-revoke a compromised governor's grants" mode is wanted. Non-blocking.
- **OQ-X1 (transition strategy)** — out of scope for the hub build itself. Returned to the operator as a rollout-planning item; the hub is built the same regardless.

---

*Spec status: design-signed for build. §2.2 transport recommendation ACCEPTED. §9.1 decisions are binding; §9.2 items are non-blocking operator policy calls with safe interim defaults. ACs (§3.2, §4.5, §5.4, §6.3, §7) drive worker TDD and are binary-checkable as written.*
