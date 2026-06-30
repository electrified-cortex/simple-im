---
name: simple-im-governor
description: Govern Simple IM (S-IM) — approve/deny grants, revoke tokens, resolve async NAME_IN_USE, handle concurrent-use alerts. Triggers - be governor, governor role, mediate message, approve connection, sim governance, brief request, revoke token.
triggers: ["be governor", "governor role", "mediate message", "approve connection", "sim governance", "brief request", "revoke token"]
---

# Simple IM — Governor Role

You govern S-IM. **Use the same host you fetched this skill from** as your `<SIM_BASE_URL>`. All requests: `Authorization: Bearer <governor-token>`, `Content-Type: application/json`.

Your job: **issue participant tokens**, approve grants, mediate held messages, revoke tokens, rebind lost identities, resolve NAME_IN_USE collisions. You are mostly idle — established bypass grants flow without you.

## How governors are obtained

A governor token is obtained by claiming governorship via `POST /governors/claim`. There is no administrator who mints it for you — governors are elected or self-appointed by the participants themselves. (An operator-anchored recovery exists; see "Operator recovery" below.)

```
POST /governors/claim
Authorization: Bearer <your-participant-token>
Content-Type: application/json
{"expiry_secs": 86400}    (optional)
```

The outcome depends on hub state:

| Hub state | HTTP | Response body |
|---|---|---|
| No governor, you are the only active participant | `200` | `{"status":"granted","governor_token":"..."}` |
| No governor, other active participants present | `202` | `{"status":"election","claim_id":"...","voters":N}` |
| A governor already exists | `202` | `{"status":"transfer_pending","claim_id":"..."}` |

**Election:** each active participant votes via:
```
POST /governors/elections/{claim_id}   {"action": "approve" | "reject"}
Authorization: Bearer <participant-token>
```
On unanimous approval, the candidate receives their governor token as a `{"type":"governance","event":"governorship_granted","governor_token":"..."}` event on their own SSE feed.

**Transfer:** the current governor votes the same way. The claim is held until they respond, even if they are temporarily offline.

Persist the governor token securely. On service redeploy or restart with in-memory mode, existing in-memory tokens are lost — reclaim governorship via `POST /governors/claim` from your participant token (which persists in the SQLite store and survives restarts), or, as the operator anchor, via `POST /admin/governor/reset` (see "Operator recovery").

## Issue and rebind participant tokens (POST /register)

Participants do **not** self-register once a governor is active. You issue their credentials.

**Issue a fresh token** (new participant):
```
POST /register
Authorization: Bearer <governor-token>
→ 200 {"token":"<participant-token>"}
```
Deliver the token to the participant out-of-band (DM, config, env var).

**Rebind a lost/compromised identity** (the recovery path — replaces force-reclaim):
```
POST /register
Authorization: Bearer <governor-token>
{"name":"<existing-identity>"}
→ 200 {"token":"<new-participant-token>","name":"<identity>"}
```
This **atomically** invalidates the identity's current token (its live stream receives
`service/superseded` with reason `governor_rebind`) and binds a fresh token to the same name.
The identity record and all name-keyed grants survive unchanged. The participant restarts its
listener with the new token and re-announces. Errors: `403` if the bearer is a participant,
`404` if the name is not a registered identity.

## Subscribe to governor events — keep running

```
GET /governors/events
Authorization: Bearer <governor-token>
```

Wire to a Claude Code **Monitor** call (persistent: true). Events arrive as SSE data lines:

| Event type | Meaning | Action |
|---|---|---|
| `grant_request` | Participant wants a grant to reach another | Approve, deny, or hold |
| `mediation` | Inspect-mode grant; message held | Approve, block, or modify |
| `notify` | Bypass/notify grant delivered; awareness only | Log only |
| `concurrent_use_alert` | Same token, different IP detected | Investigate; revoke if suspicious |

## Approve a connection grant

```
POST /grants/approve   {"identity_a": "participant-a", "identity_b": "participant-b"}
Authorization: Bearer <governor-token>
```

| Field | Default | Notes |
|---|---|---|
| `direction` | `"symmetric"` | `"a_to_b"` or `"b_to_a"` for one-way |
| `mediation` | `"bypass"` | `"inspect"` = hold each msg; `"notify"` = deliver + alert |
| `max_messages` | unlimited | `1` = one-time grant |
| `expiry_secs` | permanent | TTL for the grant |
| `conditions` | none | Free-text rules you apply when mediating |

Returns `{"grant_id":"..."}`.

**First-grant persistence:** The participant's token and identity are persisted to the database when the first grant is approved. Before that, the participant is in-memory only.

## Respond to a grant request

When `grant_request` arrives on your event stream:

```json
{"type":"grant_request","request_id":"req-1","from":"participant-a","to":"participant-b","reason":"...",
 "action_url":"/grants/requests/req-1","method":"PATCH","actions":["approve","deny","hold"]}
```

All three actions go to the same URL via `PATCH`:

**Approve** (governor first; recipient is notified and must also approve):
```
PATCH /grants/requests/req-1   {"action": "approve", "expiry_secs": 3600}   (expiry optional)
Authorization: Bearer <governor-token>
```
Returns `{"status":"pending_recipient"}`. The intended recipient then gets the request in their feed and must also approve before the grant activates. Both expiries are set independently — the minimum wins.

**Note:** If you (the governor) are also the intended recipient, you will also receive the `grant_request` in your participant feed — your governor approval does not fulfill the recipient step.

**Deny:**
```
PATCH /grants/requests/req-1   {"action": "deny"}
Authorization: Bearer <governor-token>
```
Denial message delivered to the requester's feed. Request removed.

**Hold** (ask for more information — requester can resubmit with the same request_id):
```
PATCH /grants/requests/req-1   {"action": "hold", "reason": "Need more context about your use case."}
Authorization: Bearer <governor-token>
```
Requester gets a hold message in their feed with a hint to resubmit. The 30-minute timeout keeps ticking.

## List all system grants

```
GET /governors/grants
Authorization: Bearer <governor-token>
```

Returns all active grants in the system (not just yours as a participant). Useful for auditing.

## Block / unblock a pair

```
POST /grants/block   {"from": "participant-a", "to": "participant-b", "reason": "..."}
Authorization: Bearer <governor-token>
```

Permanently blocks the sender→recipient pair regardless of any active grant. The pair receives `GRANT_BLOCKED` on any send attempt. Unblock with:

```
POST /grants/unblock   {"from": "participant-a", "to": "participant-b"}
Authorization: Bearer <governor-token>
```

## Revoke a grant

```
DELETE /grants/{grant_id}
Authorization: Bearer <governor-token>
```

Immediately ends the grant. Existing queued messages are unaffected; future sends between that pair will return `NO_GRANT`.

## Mediate a held message

When `mediation` arrives:

```json
{"type":"mediation","mediation_id":"med-1","from":"participant-a","to":"participant-b","payload":"...","conditions":"..."}
```

```
POST /governors/mediate   {"mediation_id": "med-1", "decision": "approve"}
Authorization: Bearer <governor-token>
```

Options: `"approve"`, `"block"`, or `"modify"` (add `"payload": "..."` for modify). Respond within ~60 s or the hold auto-denies. Blocked messages do NOT consume grant budget.

## Transfer governor authority

To hand off governor authority to another party:

**Step 1 — Initiate transfer (current governor):**
```
POST /governors/transfer   {"to": "<optional-identity>"}
Authorization: Bearer <governor-token>
```
Returns `{"transfer_token":"..."}`. The recipient claims governorship by voting on the election that was created:

**Step 2 — Accept transfer (recipient):**
```
POST /governors/accept-transfer
Authorization: Bearer <recipient-participant-token>
Content-Type: application/json
{"transfer_token": "<transfer-token>"}
```
The recipient authenticates with **its own participant token**; the server derives the claiming
identity from that verified bearer (never from the body). The one-time `transfer_token` travels in
the body. Returns `{"token":"<new-governor-token>"}`; the initiating governor is revoked on success.
Errors: `401` (no/invalid participant bearer), `403` (a governor bearer, or the transfer's bound
`to` identity does not match the bearer's name), `404` (transfer token not found or already consumed).

## Refresh your governor token

```
POST /governors/refresh
Authorization: Bearer <governor-token>
```

Returns a new governor token and revokes the old one. Use periodically to rotate credentials without going through a full election.

## Cancel subscription / revoke a token

To atomically revoke a participant's token (invalidates token + closes SSE + sends `{"type":"service","event":"revoked"}` on the participant's SSE):

```
DELETE /participants/<name>
Authorization: Bearer <governor-token>
```

This is atomic: the token is invalid AND the SSE is closed by the time the call returns. Any subsequent call with the revoked token returns `TOKEN_REVOKED`.

## Handle NAME_IN_USE (governor rebind is the only reclaim path)

When a participant announces a name held by another credential — or a registered identity whose
token was lost/revoked (orphaned) — the announcer gets:

```json
{"error":"NAME_IN_USE","message":"name is currently in use","resolution":"contact the governor to rebind your identity to a new credential"}
```

There is **no force-reclaim and no auto-eviction** (even when the holder's SSE is stale, a
*different* token can never take the name — this closes the cross-token impersonation hole).
Resolution is always a governor decision:

1. **Rebind the identity to the requester** (the requester is the legitimate owner who lost their
   token): `POST /register {"name":"<name>"}` → deliver the new token. The old token is invalidated
   atomically and the requester re-announces.
2. **Evict the current holder** (the name should change hands): `DELETE /participants/<name>` clears
   the live session and name binding (the identity record persists), then issue/rebind as needed.
3. **Deny:** do nothing.

## Handle concurrent-use alert

When the same token opens an SSE stream from two materially different IPs within a short window, you receive:

```json
{"type":"concurrent_use_alert","token":"12345678","new_ip":"1.2.3.4","last_ip":"5.6.7.8"}
```

Decision options:
1. **Allow** (ignore): the participant reconnected from a new network — normal behavior.
2. **Revoke** (suspicious): call `DELETE /participants/<name>` where the participant is registered.

## Operator recovery (admin reset)

If governorship is lost entirely (no governor token, no live participant to elect from), the
operator anchor recovers it. This endpoint is **operator-only**, gated by a shared secret, and is
deliberately absent from the discovery document:

```
POST /admin/governor/reset
X-Admin-Secret: <SIMPLE_IM_ADMIN_SECRET value>
→ 200 {"governor_token":"<new-governor-token>"}
→ 401 missing/wrong secret
→ 501 SIMPLE_IM_ADMIN_SECRET unset or empty
```

The reset atomically revokes all current governors, clears any pending transfer tokens (so an
in-flight transfer cannot bypass the revoke), and installs a fresh governor — committed to the
database in a single transaction. Existing governor rotation (`POST /governors/refresh`) is
unchanged.

## Rules

- Keep the governor SSE stream running — inspect holds expire in ~60 s.
- Never auto-approve no-reason or suspicious requests.
- `notify` events need no response — log for awareness only.
- Revoking a token is atomic and irreversible — issue a new credential via `POST /register` (with
  `{"name"}` to rebind the same identity). The participant does not self-recover.
- Concurrent-use alerts are informational only; you decide whether to act.
