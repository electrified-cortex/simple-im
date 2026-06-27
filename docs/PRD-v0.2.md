# simple-im — Product Requirements Document (v0.2)

> **Status:** DRAFT — synthesized from operator direction. v0.1 is the baseline; this document specifies the delta.
>
> **Pre-release design note:** The owner role, admin token, and `POST /governors/mint` endpoint referenced in this document were superseded before the first public release and do not exist. The governor is now optional and obtained by election/claim (`POST /governors/claim`). Grants can be established by recipient consent alone when no governor is present. Section 9 ("Setup code") describes a bootstrap mechanism that has been removed. See README §3 and §7 for the current model.

---

## 1. Purpose

simple-im v0.1 shipped a minimal, correct agent-to-agent messaging hub: a trust chain, governor-issued **symmetric** connection grants, strictly online-only delivery with explicit failure (never silent loss), an SSE notification channel, and a presence boolean visible to any authenticated agent. The trust boundary for messaging is the pairwise grant; the hub is blind to payloads and stores nothing.

v0.2 makes the grant the **expressive control surface for governed conversation** without abandoning that minimalism. Today a grant is an all-or-nothing, symmetric, ungoverned, until-expiry license: once approved, two agents talk freely in both directions and the governor is out of the loop. v0.2 lets a governor express *direction* (one-way vs two-way), *budget* (unlimited vs N messages), and *oversight* (deliver freely / inspect each message / be notified after the fact) on a per-grant basis; lets a single message be authorized without any standing grant (one-time grants); lets a recipient reply to an unsolicited message within a bounded window without the governor pre-approving the reverse direction (reply windows); and lets an agent scope who can observe its presence. The unifying theme: **finer-grained, governor-mediated authorization, with the message-send event — not just the grant-approval event — as a first-class control point.**

---

## 2. Out of scope (v0.2 keeps these out)

- **No durable delivery.** In-memory queue only — messages do not survive server restart. Durable queue is a future enhancement. Inspect-mode holds are transient with a bounded TTL; no store-and-forward beyond process lifetime.
- **Ephemeral session state.** Reply windows, mediation holds, presence scopes, and in-flight messages are in-memory and reset on restart. *(Grants and their usage counters are persisted to SQLite as of v1.0 — see README §10.)*
- **No broadcast / group messaging.** 1:1 by name only.
- **No message history, read receipts, editing, or threading.**
- **No federation / multi-hub clustering.**
- **No general rate-limiting.** Message-count grants provide per-grant budgets, not a global limiter.
- **No payload transformation by the hub or governor.** The hub never rewrites payloads on its own, and governors approve or deny only — payload rewriting is out of scope.
- **No governor-readable message archive.** Inspect/notify are live and transient.
- **No multi-governor mediation arbitration.** A message is mediated by at most one governor (the grant's issuer).

---

## 3. Architecture overview

### 3.1 Where the new concepts attach

All v0.2 features land as extensions of existing state stores inside `HubInner`. No new top-level component, no new transport.

| Feature | Attaches to | Change |
|---|---|---|
| Directional grants (R1) | `Grant` struct + `check_grant` | Add `direction` field; make `check_grant` direction-aware |
| Mediation modes (R2) | `Grant` struct + `send` path | Add `mediation` field; `send` branches on bypass/inspect/notify |
| One-time grants (R3) | `Grant` store | `max_messages = 1`; generalizes to R6's counted grants |
| Presence scoping (R4) | `Registry` + `presence` read path | Add per-registration `presence_scope`; presence becomes querier-dependent |
| Reply windows (R5) | New transient store in `HubInner` | On delivery, open implicit `(recipient→sender, TTL, count=1)` authorization |
| Counted grants (R6) | `Grant` struct | Add `max_messages` + `messages_used`; decrement on delivery; expire at zero |

### 3.2 The grant becomes the central policy object

```
Grant {
    identity_a, identity_b,
    expires:      Option<Instant>,              // unchanged
    direction:    Symmetric | AToB | BToA,      // R1
    max_messages: Option<u64>,                  // R6; None = unlimited; Some(1) = one-time (R3)
    messages_used: u64,                         // R6
    mediation:    Bypass | Inspect | Notify,    // R2
    governor_id:  String,                       // issuer — mediating governor for Inspect/Notify
}
```

### 3.3 The send decision pipeline (v0.2)

`send(from_token, to_name, payload)` evaluates in order — first failure short-circuits:

1. **Auth** — token valid & unexpired (unchanged).
2. **Recipient liveness** — online and registered (unchanged).
3. **Authorization** — first match in priority order:
   a. Standing grant with `direction` permitting `from→to`, not expired, with budget remaining.
   b. Open reply window for `(from→to)`.
   c. `NO_GRANT` / `GRANT_EXPIRED` / `GRANT_EXHAUSTED`.
4. **Mediation** — branch on chosen authorization's mode:
   - **Bypass** → deliver now; decrement budget; fire `kick`; open reply window; return `DELIVERED`.
   - **Inspect** → hold; route to governor; return `PENDING_MEDIATION`. Delivery on governor approve.
   - **Notify** → deliver now (same as bypass) + fire async notify event to governor. Sender sees `DELIVERED`.

---

## 4. Requirements

### R1 — Directional grants

A grant MUST carry `direction`: `symmetric` (default), `a_to_b`, or `b_to_a`, where a/b refer to `identity_a`/`identity_b` in the order supplied to `approve_grant`.

- `symmetric` authorizes `a→b` and `b→a` (v0.1 behavior; default preserves backward compatibility).
- `a_to_b` authorizes `a→b` only. A send from `b` to `a` covered only by this grant returns `NO_GRANT`.
- `b_to_a` is the mirror.
- `check_grant` becomes direction-aware (ordered `from→to`). Multiple grants between the same pair MAY coexist.
- A directed grant MUST NOT leak reverse-direction authorization; reply windows (R5) are the intended conversational relief valve.

### R2 — Per-message governor inspection / mediation

A grant MUST carry `mediation`: `bypass` (default), `inspect`, or `notify`.

- **bypass:** Delivery immediate, governor not involved. V0.1 behavior.
- **inspect:** On an authorized `send`, the hub MUST NOT deliver immediately. It places the message in a transient **mediation hold** (keyed by `mediation_id`), emits a mediation event `{mediation_id, from, to, payload, grant_id}` to the issuing governor, and returns `PENDING_MEDIATION` to the sender. The governor resolves via `POST /governors/mediate` with `approve` or `block`. On approve: normal delivery effects fire. On block: no delivery, no budget decrement.
- **notify:** Delivers immediately (as bypass) AND emits a notify event `{from, to, payload, grant_id}` to the governor. Fire-and-forget; failure to notify MUST NOT affect delivery.
- If the issuing governor is offline when an inspect event would fire: resolve the hold to `MEDIATION_UNAVAILABLE` rather than holding indefinitely.
- All holds MUST have a bounded TTL (see OQ-7). On TTL expiry the hold resolves to `MEDIATION_UNAVAILABLE`.

### R3 — One-time grants

A one-time grant is a grant with `max_messages = 1` (generalized by R6). On the first successful delivery, the grant MUST be exhausted. Budget is not consumed on failed preflight checks or on a governor block (see OQ-3).

### R4 — Presence visibility scoping

Each registration MUST carry `presence_scope`: `public`, `grant_scoped` (DEFAULT), or `hidden`.

- **public:** Any authenticated querier sees true online/offline status (v0.1 behavior).
- **grant_scoped (DEFAULT in v0.2):** Querier sees true status iff a grant exists between querier and target (in either direction). Otherwise returns `offline`. **This is a breaking change from v0.1's public default (see OQ-5).**
- **hidden:** Always appears `offline` to every presence query. Does NOT affect messageability — agents with grants can still send and receive; the send path consults `is_online` internally, unaffected by scope.
- All scoped/hidden responses MUST be byte-identical to a genuinely-offline response (`{"ok": true, "status": "offline"}`). No timing oracle.
- An agent querying its own presence always sees its true status.
- Scope is set at `register` time (new optional field) and MAY be updated via `POST /agents/{name}/presence-scope`.

### R5 — Reply windows

On successful delivery from S to R, the hub MUST open an implicit, transient, single-use authorization permitting `R→S` for a configurable TTL (`reply_ttl`).

- Opens only on successful delivery (bypass/notify immediately; inspect on governor approve).
- Authorizes exactly one `R→S` send; consumed on first delivery or expires at TTL.
- Consulted as a fallback (step 3b) only when no standing grant already authorizes `R→S`.
- Messages authorized by a reply window are delivered **bypass** (governor not in the loop).
- Reply windows reset on hub restart and on either party's liveness lapse.
- A reply to a reply opens a new window, enabling conversational back-and-forth (see OQ-1 for governor control over this).

### R6 — Enhanced grant semantics

`approve_grant` MUST accept three new optional fields, all defaulting to v0.1 behavior:

- `direction`: `symmetric` (default) | `a_to_b` | `b_to_a` (R1).
- `max_messages`: `null`/absent = unlimited (default) | N > 0. `max_messages = 1` = one-time grant (R3).
- `mediation`: `bypass` (default) | `inspect` | `notify` (R2).
- `opens_reply_window`: `true` (default) | `false` — controls whether a delivery opens a reply window (R5, OQ-1).

The grant dies when ANY budget is exhausted: time expired → `GRANT_EXPIRED`; count exhausted → `GRANT_EXHAUSTED`. Both checked on every authorization. Counter incremented atomically under the hub lock (no TOCTOU).

---

## 5. API changes

### 5.1 Modified endpoints

**`POST /grants/approve`** — new optional fields:
```jsonc
{
  "identity_a": "id-alice",
  "identity_b": "id-bob",
  "expiry_secs": 3600,
  "direction": "a_to_b",           // NEW; default "symmetric"
  "max_messages": 1,               // NEW; default null (unlimited)
  "mediation": "inspect",          // NEW; default "bypass"
  "opens_reply_window": true       // NEW; default true
}
```
Response MUST include `grant_id` (new). Example: `{"ok": true, "grant_id": "g-42", "direction": "a_to_b", "max_messages": 1, "mediation": "inspect"}`.

**`POST /messages/send`** — body unchanged. New response outcomes:
- `{"ok": true, "status": "delivered"}` — bypass/notify/approved
- `{"ok": true, "status": "pending_mediation", "mediation_id": "med-42"}` — inspect, awaiting governor
- `{"ok": false, "error": "BLOCKED"}` — governor blocked
- New errors: `GRANT_EXHAUSTED`, `MEDIATION_UNAVAILABLE`

**`POST /agents/register`** — new optional field:
```jsonc
{"name": "alice", "presence_scope": "grant_scoped"}
```

**`GET /agents/{name}/presence`** — response shape unchanged; behavior now querier-dependent (R4).

### 5.2 New endpoints

**`POST /agents/{name}/presence-scope`** — agent updates its own scope. Body: `{"presence_scope": "hidden"}`. Auth: own token.

**`GET /governors/events`** — governor SSE stream. Emits mediation and notify events. Requires a valid governor token.

**`POST /governors/mediate`** — resolve an inspect hold:
```jsonc
{
  "mediation_id": "med-42",
  "decision": "approve"       // "approve" | "block"
}
```
Auth: must be the issuing governor. Returns delivered/blocked/RECIPIENT_OFFLINE/BAD_REQUEST.

> **Operator decision (2026-06-03):** The governor approves or denies only. Payload rewriting is out of scope.

### 5.3 New error codes

| Code | HTTP | Meaning |
|---|---|---|
| `GRANT_EXHAUSTED` | 403 | Grant's message budget spent |
| `BLOCKED` | 403 | Governor blocked an inspected message |
| `MEDIATION_UNAVAILABLE` | 409 | Issuing governor offline or hold TTL expired |

---

## 6. Open questions (decisions needed before implementation)

| # | Question | Default recommendation |
|---|---|---|
| OQ-1 | Do reply windows defeat a one-way / mediated grant's intent? | Add `opens_reply_window` per-grant flag (default true); governors set false for strict one-way. |
| OQ-2 | Do governors receive mediation events on a dedicated `/governors/events` SSE, or reuse the agent channel? | Dedicated `/governors/events` stream. |
| OQ-3 | Does a governor-blocked message consume grant budget? | No — a blocked message didn't happen. |
| OQ-4 | Default `reply_ttl`? | 120 seconds, configurable via env. |
| OQ-5 | Presence default flip to `grant_scoped` is breaking. Intended? | Yes — call it out in changelog; add `--legacy-public-presence` boot flag for migration. |
| OQ-6 | Presence scope per-registration (resets on re-register) or per-identity (sticky)? | Per-registration with update endpoint. |
| OQ-7 | Inspect hold mechanics: hold TTL, recipient-goes-offline during hold, sync vs async resolution? | TTL = 60s; recipient-offline-during-hold → `RECIPIENT_OFFLINE`; async resolution (sender gets `PENDING_MEDIATION`, polls/streams for outcome). |
| OQ-8 | Multiple grants between same pair: which is selected? | Most-specific/most-restrictive first (directed > symmetric; mediated > bypass; oldest non-exhausted among ties). |
| OQ-9 | Inspect/notify governors see payloads — acceptable relaxation of v0.1 payload-blindness? | Yes, scoped to opt-in grants. Confirm no logging/persistence of payloads. |
| OQ-10 | Should inspect holds survive a brief governor reconnect (grace window), or fail immediately on governor liveness lapse? | Grant governor a liveness-window grace (reuse existing mechanism). |

---

## 7. Implementation order (recommended)

1. **R1 + R6 + R3** — extend `Grant` struct and `check_grant` in `src/trust.rs`. Pure trust-chain changes, fully unit-testable without any HTTP changes.
2. **R4** — presence scoping in `src/registry.rs` + `src/http.rs` `handle_presence`. Requires querier identity derivation from token.
3. **R5** — reply-window transient store in `HubInner`. New state, minimal new async surface.
4. **R2** — mediation/inspect/notify in `src/delivery.rs` + `src/http.rs`. The only genuinely new async surface; save for last.

## 8. Journey: Authorization-by-Brief

> **Status:** CLOSED — operator-resolved 2026-06-03. All 4 decisions made.

This section documents a specific message journey that the v0.2 authorization model must support: **unauthorized contact with governor-mediated brief**. It represents the "edge case path" — the 99% of messaging happens via established grants (bypass mode); this is the 1% where no grant exists.

### 8.1 The journey

**Actors:** Bob (sender, no grant to Alice), Alice (recipient), Governor (the hub authority)

**Trigger:** Bob attempts to send to Alice with no valid covering grant.

**Flow:**

`
Bob  --  POST /messages/send {to: "alice", payload: "..."}
Hub  ->  {"ok": false, "error": "BRIEF_REQUIRED",
           "hint": "No authorization covers this message. Resend with a 'reason' field."}

Bob  --  POST /messages/send {to: "alice", payload: "...", reason: "I need to discuss the task handoff"}
Hub  -->  [holds message, routes to governor]
Hub  ->  Governor  SSE/dequeue: {type: "brief_request",
                                  mediation_id: "med-X",
                                  from: "bob", to: "alice",
                                  reason: "I need to discuss the task handoff",
                                  payload: "..."}        ← payload present but governor
                                                            may ignore to stay context-clean
Governor -- POST /governors/mediate {mediation_id: "med-X", decision: "approve"}
Hub  ->  Alice: {from: "bob", reason: "I need to discuss the task handoff", payload: "..."}
Hub  ->  Bob:  {"ok": true, "status": "delivered"}

  ---- OR if governor denies ----

Governor -- POST /governors/mediate {mediation_id: "med-X", decision: "block"}
Hub  ->  Bob:  {"ok": false, "error": "BLOCKED"}

  ---- OR if Bob is smart enough to include reason on first send ----

Bob  --  POST /messages/send {to: "alice", payload: "...", reason: "I need to discuss the task handoff"}
Hub  -->  [holds message, routes to governor immediately — no BRIEF_REQUIRED round-trip]
          [same flow from here]
`

### 8.2 Operator decisions (all resolved)

| Decision | Answer |
|---|---|
| When does the brief path activate? | Automatically on any send with no valid covering grant. No opt-in required. |
| What does Alice receive? | {from, reason, payload} — reason prepended to the message envelope. |
| What does the governor see? | The 
eason field prominently, and the payload in a separate field. Governor can ignore payload to stay context-clean. |
| API shape for Bob's resend? | Same POST /messages/send with an added optional 
eason field. If Bob includes reason on first attempt, no BRIEF_REQUIRED round-trip needed. |
| TTL expiry behavior (governor doesn't respond)? | Auto-deny (MEDIATION_UNAVAILABLE). Never auto-approve. |

### 8.3 API changes for this journey

**POST /messages/send** — adds one new optional field:

`jsonc
{
  "to": "alice",
  "payload": "...",
  "reason": "I need to discuss the task handoff"  // NEW; omit for standard sends
}
`

New response code:
- BRIEF_REQUIRED (HTTP 403): no grant covers rom→to and no 
eason was supplied. Hub invites resend with reason.

**Governor mediation event** (delivered via GET /governors/events SSE):

`jsonc
{
  "type": "brief_request",
  "mediation_id": "med-X",
  "from": "bob",
  "to": "alice",
  "reason": "I need to discuss the task handoff",  // primary decision input
  "payload": "..."                                  // available but governor may ignore
}
`

Governor resolves via POST /governors/mediate (same as inspect mode in R2).

**Alice's received message** (from POST /messages/dequeue):

`jsonc
{
  "ok": true,
  "payload": "...",
  "from": "bob",
  "reason": "I need to discuss the task handoff"  // NEW; present when message was brief-authorized
}
`

### 8.4 Interaction with existing grant model

- Once the governor approves a brief-request, the **message is delivered but no standing grant is created**. If Bob sends again, he goes through the brief process again (unless the governor subsequently issues a standing grant via pprove_grant).
- Brief-authorization is NOT counted against any grant budget (there is no covering grant to decrement).
- If Alice has presence_scope: hidden, Bob cannot even know Alice exists. In that case, the send returns RECIPIENT_UNKNOWN (indistinguishable from never-registered) — the brief path is not reachable for hidden agents. Hidden agents are truly hidden.
- If a directed grant covers ob→alice, the send goes through that grant directly (no BRIEF_REQUIRED). Brief is only for the no-grant case.

### 8.5 Design notes

- **99% of sends are unaffected.** This path only activates when no grant covers the pair. Established agent relationships with valid grants bypass this entirely.
- **Governor stays payload-blind by default.** The reason is the decision surface; the payload is available but the governor's implementation should only fetch it if necessary.
- **The reason field is free-text, not structured.** No schema enforced by the hub. Agents self-describe their intent.
- **Brief-approval is one-shot, not a grant.** This is intentional — it prevents brief-authorization from being a backdoor to ongoing ungoverned communication. The governor must explicitly issue a grant via pprove_grant if ongoing communication is desired.

## 9. Setup code (historical — superseded before first release)

> This section described a `data/admin_code.txt` bootstrap PIN and `POST /governors/mint` endpoint. Both have been removed. Governors are now obtained via `POST /governors/claim` — no pre-shared secret is required. See README §7 for the current election/claim flow.

## 10. Grant conditions (relationship rules)

> **Operator decisions (2026-06-03):** All questions resolved.

### 10.1 Purpose

A grant can carry a conditions field — free-text natural language that the governor reads when mediating a message between the pair. Conditions describe what kinds of messages are acceptable for this specific relationship. The governor (as an AI agent) reads the conditions alongside the message/reason and decides approve/deny accordingly.

Example condition: "Alice only accepts messages from Bob if Bob is doing work for Alice, not requesting that Alice do work for Bob."

### 10.2 Design decisions

| Decision | Answer |
|---|---|
| Who writes conditions? | Governance chain: the receiver can set them, the governor can add/override, the operator can delegate through the governor. Ownership is separate from the conditions content. |
| Who sees them? | The governor sees the grant's conditions in every mediation event they receive for that grant. |
| Format | Free-text (natural language). The governor is an AI agent and interprets them as instructions. |
| Scope | Per-grant (specific to one sender-receiver relationship). No global conditions in v0.2. |
| Interaction with reason | If a reason is provided that clearly meets the conditions, the governor may approve without reading the payload (stays payload-blind). If the reason is weak or absent, the governor reads the payload to check against conditions. |
| Can they be copied? | Yes — agents/governors may clone conditions across similar grants. No special API needed; it's a string copy. |

### 10.3 API changes (additive)

**POST /grants/approve** — new optional field:
`jsonc
{
  "identity_a": "alice",
  "identity_b": "bob",
  "conditions": "Bob may only message Alice about work he is doing for Alice, not work requests directed at Alice."
}
`

**Governor mediation event** — conditions field added:
`jsonc
{
  "type": "mediation",
  "mediation_id": "...",
  "from": "bob",
  "to": "alice",
  "reason": "...",
  "payload": "...",
  "grant_id": "...",
  "conditions": "Bob may only message Alice about work he is doing for Alice..."
}
`

### 10.4 Implementation note

conditions: Option<String> on Grant struct. Populated from pprove_grant request. Included verbatim in all mediation events (inspect + notify + brief_request) for that grant. The hub does not interpret or evaluate conditions — that is the governor's job.