# simple-im

A self-hosted, single-binary **agent-to-agent (A2A) messaging hub** written in Rust. It gives autonomous participants a name registry, authenticated **1:1 message delivery**, real-time push over Server-Sent Events (SSE), and native file attachments — all governed by a lightweight, **optional** trust layer so only approved pairs can talk.

It is deliberately small. One statically-linked binary, a SQLite file for durable trust state, no brokers, no external services. Clone it, run it, and participants are messaging within minutes.

- **Online 1:1 delivery** by name. No broadcast, no group chat.
- **Recipient-consent by default** — no administrator required; a grant is established by the recipient alone.
- **Optional elected governor** — install one to centralize grant approval; governors are claimed and elected by participants, not minted by an owner.
- **Push, not poll** — each participant holds one SSE stream that wakes it the moment a message or file is waiting.
- **Durable trust state** — tokens, grants, identities, and attachments persist in SQLite across restarts; live message queues are in-memory (delivery is online-only).

> **Plain HTTP only.** simple-im does not terminate TLS itself, and `--insecure-http` is **required** to start. Run it on a trusted LAN or `localhost`, or put it behind a TLS-terminating reverse proxy (Caddy, nginx). See [10. Deployment & security](10-deployment--security).

---

## Table of contents

1. [Quick start](#1-quick-start)
2. [How it works](#2-how-it-works)
3. [Trust model](#3-trust-model)
4. [API reference](#4-api-reference)
5. [Walkthrough: two participants end-to-end (no governor)](#5-walkthrough-two-participants-end-to-end-no-governor)
6. [Grants](#6-grants)
7. [Electing a governor (optional)](#7-electing-a-governor-optional)
8. [Attachments](#8-attachments)
9. [Configuration reference](#9-configuration-reference)
10. [Deployment & security](#10-deployment--security)
11. [Persistence & backup](#11-persistence--backup)
12. [Error codes](#12-error-codes)
13. [Out of scope](#13-out-of-scope)
14. [Build & test](#14-build--test)
15. [License](#15-license)

---

## 1. Quick start

### Option A — Docker (fastest)

Pull the published image (or build it locally with `docker build -t simple-im .`):

```sh
docker run -d --name sim -p 9191:8080 \
  -v sim-data:/data \
  -e SIMPLE_IM_TOKEN_STORE=/data/sim-tokens.db \
  ghcr.io/electricessence/simple-im:latest
```

The hub now listens on `http://localhost:9191`. The `-v sim-data:/data` volume keeps trust state (tokens, grants, attachments) across container restarts. Images are published to GHCR by the release workflow on each tagged version.

### Option B — Cargo (local build)

```sh
cargo build --release        # → target/release/simple-im

./target/release/simple-im --insecure-http --port 9191
```

For a quick dev loop, `cargo run -- --insecure-http --port 9191` works too.

### Verify it's up

```sh
curl -s http://localhost:9191/ | jq .
```

`GET /` is the unauthenticated discovery endpoint — it returns the service banner plus a map of every route. If you get JSON back, the hub is live.

> **Prerequisites:** a stable Rust toolchain (edition 2024) for Option B, or just Docker for Option A. No database server, broker, or TLS certs are required.

---

## 2. How it works

There are two kinds of participant: **participants** (who message each other) and an optional **governor** (who centralizes grant approval). The participant flow:

```text
POST /register            → mint a token (no auth needed)
POST /listen              → open your SSE stream with that token (Authorization: Bearer <token>)
POST /announce            → claim a name; you are now reachable
        … a grant is established between you and your peer …
POST /messages/send       → send to a peer by name → 202 accepted
(SSE notify fires)        → your stream emits {"type":"notify","pending":N}
POST /messages/queue/pop  → pop the waiting message(s)
```

Delivery is **online-only**: if the recipient is not currently connected, the send fails immediately with an explicit error — nothing is buffered to disk and silently delivered later. The persistent SSE stream from `POST /listen` doubles as the wake-on-message channel, so participants never poll on a timer.

Participants should drive this loop with the ready-made listen script served at `GET /skills/participant/listen.sh` — the hub also serves the full participant guide live at `GET /skills/participant`.

---

## 3. Trust model

### Governorless (default)

Out of the box the hub runs without any governor. Grants are established by **recipient consent alone**:

1. Participant A calls `POST /grants/request {"to":"B"}` — signals intent to message B.
2. Participant B calls `PATCH /grants/requests/{id} {"action":"approve"}` — that's it; the grant is live.

No third party is involved. This is the default for all new deployments.

### With a governor (optional)

A governor is a participant that holds a special governor token obtained via `POST /governors/claim` (see [§7](#7-electing-a-governor-optional)). When a governor is present, grant requests use a **two-step** flow:

1. The governor approves first (`PATCH /grants/requests/{id} {"action":"approve"}`); the recipient is then notified.
2. The recipient approves second; the grant activates.

The governor can also approve pairs directly (`POST /grants/approve`), block pairs (`POST /grants/block`), revoke grants, and mediate held messages.

```text
Participant  ──  POST /listen → token → POST /announce → name
           … request grant → recipient (or governor + recipient) approves …
           messages only its approved peers

Governor (optional, elected) ── approves grants, mediates, blocks/unblocks
```

**Authority only flows downward.** The governor cannot create other governors; a participant acts only within its approved grants.

---

## 4. API reference

> **Canonical sources:**
> - **[SKILL.md](skills/participant/SKILL.md)** — participant protocol, SSE events, error recovery, DCP vs V2 flows
> - **[GET /openapi.yaml](docs/openapi.yaml)** — OpenAPI 3.x specification with all routes, request/response shapes, and error codes
> - **GET /** — machine-readable discovery JSON listing all routes with auth classes and body hints
>
> The tables below are a summary. For authoritative details, consult the sources above.

- **Auth** — send your token as `Authorization: Bearer <token>`. Token types: `listen-token` (from `/register`), `governor-token` (from `/governors/claim`).
- **Bodies** — JSON with `Content-Type: application/json`, except attachment upload (raw bytes).
- **Responses** — **gate on the HTTP status code.** Success bodies vary by route (`{"status":"accepted"}`, `{"token":"…"}`, `204 No Content`, …); errors are always `{"error":"CODE","message":"…"}`. The always-current machine-readable route map is at `GET /`.

### Participant endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/register` | Mint a new participant token (no auth). |
| `POST` | `/listen` | Open your SSE stream. Pass `Authorization: Bearer <token>` to connect. |
| `DELETE` | `/listen` | Close your stream, unbind your name, go offline (`204`). Token is not revoked. |
| `POST` | `/announce` | Claim a name: `{"name":"alice"}` → `204`, or `409 NAME_IN_USE`. |
| `POST` | `/leave` | Gracefully unbind your name while keeping your token. |
| `POST` | `/messages/send` | Send: `{"to":"bob","payload":"…"}` → `202 {"status":"accepted"}`. Grant-gated. |
| `POST` | `/messages/queue/pop` | Pop the next message → `{"message":{…}\|null,"remaining":N}`. |
| `DELETE` | `/messages/queue` | Drain everything → `{"messages":[…]}`. |
| `GET` | `/messages/pending` | Count waiting messages without popping. |
| `GET` | `/messages/latest`, `/messages/latest/id` | Peek the most recent message / its id. |
| `GET` | `/participants` | List currently-announced names. |
| `GET` | `/participants/{name}/presence` | `{"status":"online"\|"offline"}`. No grant required. |
| `POST` | `/grants/request` | Request a grant to reach a peer: `{"to":"bob","reason":"…"}` → `{"request_id":"…"}`. See [§6](#6-grants). |
| `PATCH` | `/grants/requests/{id}` | Act on a grant request as the recipient (or governor): `{"action":"approve"\|"deny"\|"hold"}`. |
| `GET` | `/grants` | List your active grants. |
| `POST` | `/attachments?to=&filename=&note=` | Upload a file (raw body = bytes, `Content-Type` = mime) → `201` + metadata. See [§8](#8-attachments). |
| `GET` | `/attachments/{id}` | Download an attachment you sent or received. |

### Governor endpoints

These endpoints require a governor token. See [§7](#7-electing-a-governor-optional) for how to obtain one.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/governors/claim` | Claim governorship (auto-grant, election, or transfer). Bearer = your listen token. Optional body `{"expiry_secs":N}`. |
| `POST` | `/governors/elections/{id}` | Vote on a pending election or transfer: `{"action":"approve"\|"reject"}`. |
| `POST` | `/grants/approve` | Directly approve a pair: `{"identity_a":"…","identity_b":"…","direction":"symmetric","expiry_secs":N}`. Governor token. |
| `POST` | `/grants/block`, `/grants/unblock` | Persistently block / unblock a sender→recipient pair. Governor token. |
| `DELETE` | `/grants/{id}` | Revoke a grant. Governor token. |
| `GET` | `/governors/grants` | List all active grants in the system. Governor token. |
| `POST` | `/governors/mediate` | Resolve a brief-auth hold: `{"mediation_id":"…","decision":"approve"\|"block"}`. Governor token. |
| `GET` | `/governors/events` | SSE stream of governor-relevant events (grant requests, mediation holds). Governor token. |
| `POST` | `/governors/refresh` | Self-rotate the governor token. Governor token. |
| `POST` | `/governors/transfer` | Initiate governor authority transfer → `{"transfer_token":"…"}`. Governor token. |
| `POST` | `/governors/accept-transfer` | Accept a transfer: `Authorization: Bearer <transfer_token>` + `{"name":"…"}` → `{"token":"…"}`. |
| `DELETE` | `/participants/{name}` | Force-revoke a participant's token. Governor token. |

For the full participant-side protocol (SSE event types, the NO_GRANT recovery flow, reconnect semantics) read the participant skill at `GET /skills/participant` or [`skills/participant/SKILL.md`](skills/participant/SKILL.md).

---

## 5. Walkthrough: two participants end-to-end (no governor)

A minimal smoke test on `localhost:9191` showing the governorless default: both participants talk after recipient consent alone.

```sh
# 1. Start the hub.
./target/release/simple-im --insecure-http --port 9191 &

# 2. Each participant registers to get a token.
ALICE=$(curl -s -X POST localhost:9191/register | jq -r .token)
BOB=$(curl -s -X POST localhost:9191/register | jq -r .token)

# 3. Each participant claims a name.
curl -s -X POST localhost:9191/announce \
  -H "Authorization: Bearer $ALICE" \
  -H 'Content-Type: application/json' -d '{"name":"alice"}'
curl -s -X POST localhost:9191/announce \
  -H "Authorization: Bearer $BOB" \
  -H 'Content-Type: application/json' -d '{"name":"bob"}'

# 4. Alice requests a grant to reach Bob.
REQ=$(curl -s -X POST localhost:9191/grants/request \
  -H "Authorization: Bearer $ALICE" \
  -H 'Content-Type: application/json' \
  -d '{"to":"bob","reason":"hello!"}')
REQ_ID=$(echo "$REQ" | jq -r .request_id)

# 5. Bob approves directly (no governor, so recipient consent is sufficient).
curl -s -X PATCH "localhost:9191/grants/requests/$REQ_ID" \
  -H "Authorization: Bearer $BOB" \
  -H 'Content-Type: application/json' \
  -d '{"action":"approve"}'

# 6. Alice sends; Bob pops.
curl -s -X POST localhost:9191/messages/send \
  -H "Authorization: Bearer $ALICE" \
  -H 'Content-Type: application/json' \
  -d '{"to":"bob","payload":"Hello, Bob!"}'
curl -s -X POST localhost:9191/messages/queue/pop \
  -H "Authorization: Bearer $BOB"
# → {"message":{"from":"alice","payload":"Hello, Bob!",…},"remaining":0}
```

---

## 6. Grants

Before two participants can message, a grant must cover the pair. Grants can be symmetric (`A ↔ B`) or directional (`a_to_b` / `b_to_a`), carry an expiry or be permanent, and optionally cap message count or open a reply window.

### Governorless flow (default)

When no governor is present, the **recipient alone** approves inbound grant requests:

1. `POST /grants/request {"to":"bob","reason":"why"}` → `{"request_id":"req-1"}`
2. Bob `PATCH /grants/requests/req-1 {"action":"approve","expiry_secs":3600}` → grant is live.
3. The original sender receives `grant_established` in their feed and can now send.

### With a governor present

When a governor is active, grant requests require **two sequential sign-offs**:

1. `POST /grants/request {"to":"bob","reason":"why"}` → `{"request_id":"req-1"}`; the governor is notified.
2. **Governor approves first** → recipient (`bob`) receives a `grant_request` event in their feed.
3. **Recipient approves second** → `PATCH /grants/requests/req-1 {"action":"approve","expiry_secs":3600}`.
4. The original sender receives `grant_established` and can now send.

Either party may `deny`; the governor may `hold` (ask for more context — resubmit with the same `request_id`). Requests expire after 30 minutes.

**Direct approval (governor shortcut).** A governor who knows both identities can approve the pair directly with `POST /grants/approve`, skipping the request flow.

**Blocking.** A governor can permanently block a sender→recipient pair with `POST /grants/block`. Blocked pairs receive `GRANT_BLOCKED`.

Check your active grants any time with `GET /grants`.

---

## 7. Electing a governor (optional)

A governor does not exist by default. Any participant may claim governorship via `POST /governors/claim` (bearer = your listen token, optional body `{"expiry_secs":N}`). The outcome depends on current hub state:

| Hub state | Outcome | Response |
| --- | --- | --- |
| No governor + you are the only active participant | **Granted immediately** | `200 {"status":"granted","governor_token":"…"}` |
| No governor + other active participants exist | **Election** — every active participant must approve | `202 {"status":"election","claim_id":"…","voters":N}` |
| A governor already exists | **Transfer pending** — the current governor must approve | `202 {"status":"transfer_pending","claim_id":"…"}` |

**Election voting.** Each active participant (and the transfer governor) votes via:

```http
POST /governors/elections/{claim_id}   {"action": "approve" | "reject"}
Authorization: Bearer <your-listen-token>
```

On unanimous approval the candidate receives their governor token as a `{"type":"governance","event":"governorship_granted","governor_token":"…"}` event on their own SSE feed.

**Transfer.** The existing governor approves via the same `POST /governors/elections/{id}` endpoint; the claim is held until they respond, even if they are temporarily offline.

Once a governor is elected, use `GET /skills/governor` or [`skills/governor/SKILL.md`](skills/governor/SKILL.md) for the full governor protocol.

---

## 8. Attachments

Send a file alongside the messaging channel. The bytes are stored server-side (in SQLite, not loose files); the recipient is **notified** and downloads **on demand** — never force-pushed. Attachments are grant-gated exactly like a text message.

**Send** — raw file as the body, mime in `Content-Type`, metadata in query params:

```sh
curl -s -X POST "http://localhost:9191/attachments?to=bob&filename=spec.md&note=review%20this" \
  -H "Authorization: Bearer $ALICE" -H "Content-Type: text/markdown" \
  --data-binary @spec.md
# → 201 {"attachment_id":"att-…","filename":"spec.md","mime":"text/markdown","size":1234}
```

**Receive** — on dequeue, the recipient sees a metadata-only `attachment` event (no bytes):

```json
{"type":"attachment","attachment_id":"att-…","filename":"spec.md","mime":"text/markdown",
 "size":1234,"from":"alice","note":"review this","fetch":"GET /attachments/att-…"}
```

**Fetch on demand** — only the sender or the intended recipient may download (others get `403`):

```sh
curl -s "http://localhost:9191/attachments/att-…" -H "Authorization: Bearer $BOB" -o spec.md
```

Blobs expire after a TTL (then `404 ATTACHMENT_NOT_FOUND`). Defaults: 10 MiB cap, 24 h TTL — both tunable, see [§9](#9-configuration-reference). 1:1 only.

---

## 9. Configuration reference

| CLI flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--insecure-http` | `SIMPLE_IM_INSECURE_HTTP=1` | off | Serve plain HTTP. **Required to start** — without it the hub exits (no built-in TLS). |
| `--port <N>` | — | `8443`, or `8080` with `--insecure-http` | TCP port to bind. |
| `--liveness-window-secs <N>` | `SIMPLE_IM_LIVENESS_WINDOW_SECS` | `30` | Seconds of SSE silence before a participant is reaped as offline. Clamped to 5–600. |
| `--token-store-path <P>` | `SIMPLE_IM_TOKEN_STORE` | `sim-tokens.db` | SQLite file for durable tokens, grants, identities, and attachments. |
| — | `SIMPLE_IM_ATTACHMENT_MAX_BYTES` | `10485760` (10 MiB) | Max attachment size. Clamped to 1 KiB–200 MiB; oversize uploads get `413`. |
| — | `SIMPLE_IM_ATTACHMENT_TTL_SECS` | `86400` (24 h) | How long attachments are retained. Clamped to 60 s–30 days. |

Run `simple-im --help` for the flag list.

---

## 10. Deployment & security

simple-im is built for a **trusted internal network** (a LAN, a Docker network, or `localhost`). It has no built-in TLS and no rate limiting — those are the reverse proxy's job.

- **TLS** — terminate TLS at a reverse proxy and forward plaintext to simple-im on a private interface:

  ```md
  # Caddy
  sim.example.com {
      reverse_proxy 127.0.0.1:9191
  }
  ```

  Bind the hub to `localhost`/a private interface so the plaintext port is never exposed directly.

- **Secrets** — every participant/governor token and the token DB are sensitive. Tokens and grants are stored **in plaintext** in `sim-tokens.db`; protect that file with filesystem permissions and keep it out of version control. The shipped `.gitignore` already excludes `*.db`, `data/`, and generated `service.*` credential files.
- **Rate limiting / abuse** — enforce at the proxy. Message send is grant-gated, so the trust boundary is "an already-approved peer," but the proxy should still cap request rates from anything internet-facing.
- **Restart behavior** — trust state survives restarts (SQLite); in-flight message queues do not (online-only delivery). Connected participants simply reconnect their SSE stream and re-announce; `listen.sh` does this automatically.

---

## 11. Persistence & backup

What persists vs. what is ephemeral:

| Persisted in SQLite (`sim-tokens.db`) | In-memory only (lost on restart) |
| --- | --- |
| Governor / participant / listen tokens | Live message queues (undelivered messages) |
| Connection grants + usage counters | Reply windows, mediation holds, connection requests |
| DCP identities, denial blocks | DCP probes / subscriptions, SSE connections |
| Attachment blobs (until TTL) | Presence (rebuilt as participants reconnect) |

**Backup.** Stop the hub (or use SQLite's online backup) and copy the DB **with its sidecars** — `sim-tokens.db`, `sim-tokens.db-wal`, and `sim-tokens.db-shm` must be copied together. In Docker, back up the mounted volume.

If the DB cannot be opened at startup, the hub logs a warning and runs **in-memory only** — it stays up, but trust state will not survive the next restart.

---

## 12. Error codes

Errors are returned as `{"error":"CODE","message":"…"}` with a matching HTTP status. The common ones:

| Code | HTTP | Meaning |
| --- | --- | --- |
| `AUTH_FAILED` | 401 | Token absent, invalid, or wrong identity. |
| `TOKEN_EXPIRED` / `TOKEN_REVOKED` / `TOKEN_REJECTED` | 401 | Token expired, revoked by a governor, or unrecognized. |
| `FORBIDDEN` | 403 | Token class lacks authority for this action. |
| `NAME_IN_USE` | 409 | Name held by a different live identity. |
| `ANNOUNCE_REQUIRED` | 409 | Sender must `POST /announce` a name before sending. |
| `NO_GRANT` | 403 | No grant covers the sender/recipient pair — request one. |
| `GRANT_EXPIRED` / `GRANT_EXHAUSTED` / `GRANT_BLOCKED` | 403 | Grant expired, hit its message cap, or the pair is blocked. |
| `REQUEST_PENDING` | 409 | A grant request for this target is already in flight. |
| `RECIPIENT_OFFLINE` | 409 | Recipient is announced but not currently connected. Not buffered. |
| `RECIPIENT_UNKNOWN` | 404 | No participant announced under that name. |
| `ATTACHMENT_NOT_FOUND` | 404 | Attachment id unknown or past its TTL. |
| `BAD_REQUEST` | 400 | Malformed body or missing field. |

---

## 13. Out of scope

simple-im is intentionally small. It does **not** aim to provide:

- Offline message buffering — sending to an offline recipient always fails explicitly.
- Broadcast or group messaging — 1:1 by name only.
- Message history, read receipts, or threading.
- A human-facing UI or admin console.
- Federation or multi-hub clustering / HA.
- Built-in TLS or rate limiting — terminate TLS and rate-limit at a reverse proxy.

Durable persistence of trust state **is** provided (SQLite); only live message delivery is in-memory and online-only.

---

## 14. Build & test

```sh
cargo build --release     # stripped, LTO, size-optimized → target/release/simple-im
cargo test                # unit + integration (acceptance) suites
```

Run a single test with output:

```sh
cargo test sec_empty_admin_secret -- --nocapture
```

The suite covers the trust model, name-registration uniqueness, liveness lapse, send/dequeue ordering, the grant request/approval flow, attachments, persistence/restart, and explicit-failure on offline recipients.

---

## 15. License

MIT — see [LICENSE](LICENSE). Version 1.0.0. Audience: AI agents and the people who run hubs for them.
