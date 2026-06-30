//! HTTP layer for Simple IM — Axum router, request handlers, and skill download endpoints.

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::delivery::{
    AnnounceResult, ApproveStatus, ClaimOutcome, ClaimResolution, DeliveryHub, MediationDecision,
    MediationResult, ParticipantInfo,
};
use crate::error::Error;
use crate::rooms::RoomStore;
use crate::trust::{ApproveGrantRequest, GrantDirection, GrantMediation};
use crate::types::{GovernorToken, ParticipantToken, Payload};

// ── Bundled skill files ───────────────────────────────────────────────────────

const PARTICIPANT_SKILL_MD: &str = include_str!("../skills/participant/SKILL.md");
const PARTICIPANT_LISTEN_SH: &str = include_str!("../skills/participant/listen.sh");
const GOVERNOR_SKILL_MD: &str = include_str!("../skills/governor/SKILL.md");
const OPENAPI_YAML: &str = include_str!("../docs/openapi.yaml");

// ── State ─────────────────────────────────────────────────────────────────────

/// Shared Axum application state holding the delivery hub and attachment configuration.
pub struct AppState {
    pub hub: DeliveryHub,
    /// In-memory room store — shared with the hub for room-based presence visibility.
    /// Does not persist across server restarts.
    pub rooms: Arc<RoomStore>,
    pub attachment_ttl: Duration,
    pub attachment_max_bytes: usize,
    /// Bootstrap window close time (15-0029 / security-MINOR-2). When set and elapsed with no
    /// governor established, unauthenticated POST /register returns 503. None = window never
    /// closes by timeout. Sourced from `SIMPLE_IM_BOOTSTRAP_TIMEOUT_SECS`.
    pub bootstrap_deadline: Option<Instant>,
    /// Operator anchor secret for POST /admin/governor/reset (15-0029). `None` when
    /// `SIMPLE_IM_ADMIN_SECRET` is unset or empty — the endpoint then returns 501. A non-empty
    /// secret is required for operator-anchor recovery (security-MINOR-1).
    pub admin_secret: Option<String>,
}

/// Compute the bootstrap-window deadline from `SIMPLE_IM_BOOTSTRAP_TIMEOUT_SECS` (seconds).
/// Returns None when unset, unparsable, or zero (window never times out).
fn bootstrap_deadline_from_env() -> Option<Instant> {
    std::env::var("SIMPLE_IM_BOOTSTRAP_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .map(|n| Instant::now() + Duration::from_secs(n))
}

/// Read `SIMPLE_IM_ADMIN_SECRET`. An empty string is treated identically to unset (None).
fn admin_secret_from_env() -> Option<String> {
    std::env::var("SIMPLE_IM_ADMIN_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
}

impl AppState {
    /// Creates `AppState` with an in-memory hub; used in tests and default startup.
    pub fn new(liveness_window: Duration) -> Self {
        let (attachment_ttl, attachment_max_bytes) = attachment_config();
        let hub = DeliveryHub::new(liveness_window);
        // Share the room store between hub (for presence fanout) and HTTP handlers.
        let rooms = hub.room_store();
        Self {
            hub,
            rooms,
            attachment_ttl,
            attachment_max_bytes,
            bootstrap_deadline: bootstrap_deadline_from_env(),
            admin_secret: admin_secret_from_env(),
        }
    }

    /// Creates `AppState` wrapping an existing hub; used when restoring persisted state.
    pub fn new_with_hub(hub: DeliveryHub) -> Self {
        let (attachment_ttl, attachment_max_bytes) = attachment_config();
        let rooms = hub.room_store();
        Self {
            hub,
            rooms,
            attachment_ttl,
            attachment_max_bytes,
            bootstrap_deadline: bootstrap_deadline_from_env(),
            admin_secret: admin_secret_from_env(),
        }
    }
}

/// Attachment limits from env: `SIMPLE_IM_ATTACHMENT_TTL_SECS` (default 24h, clamp 60s–30d)
/// and `SIMPLE_IM_ATTACHMENT_MAX_BYTES` (default 10 MiB, clamp 1 KiB–200 MiB).
fn attachment_config() -> (Duration, usize) {
    let ttl = std::env::var("SIMPLE_IM_ATTACHMENT_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(86_400)
        .clamp(60, 30 * 24 * 3_600);
    let max = std::env::var("SIMPLE_IM_ATTACHMENT_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10 * 1024 * 1024)
        .clamp(1024, 200 * 1024 * 1024);
    (Duration::from_secs(ttl), max)
}

// ── SSE drop guard ────────────────────────────────────────────────────────────

/// Wraps a stream and calls `on_drop` when dropped (i.e., when the SSE connection closes).
struct SseDropGuard<S> {
    inner: S,
    on_drop: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl<S: futures_core::stream::Stream + Unpin> futures_core::stream::Stream for SseDropGuard<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl<S> Drop for SseDropGuard<S> {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
}

// ── Router ────────────────────────────────────────────────────────────────────

/// Builds and returns the full Axum router with all S-IM routes wired up.
pub fn router(state: Arc<AppState>) -> Router {
    let max_attach = state.attachment_max_bytes;
    Router::new()
        .route("/", get(handle_discovery))
        .route("/openapi.yaml", get(handle_openapi))
        .route("/register", post(handle_register))
        .route("/listen", post(handle_listen))
        .route("/listen", delete(handle_cancel_listen))
        .route("/announce", post(handle_announce))
        .route("/skills/participant", get(handle_skill_participant))
        .route(
            "/skills/participant/listen.sh",
            get(handle_skill_participant_listen),
        )
        .route("/skills/governor", get(handle_skill_governor))
        .route("/participants", get(handle_list_participants))
        .route("/participants/{name}", delete(handle_deregister))
        .route("/participants/{name}/presence", get(handle_presence))
        .route(
            "/participants/{name}/presence-scope",
            post(handle_set_presence_scope),
        )
        .route("/messages/send", post(handle_send))
        .route("/messages/queue/pop", post(handle_dequeue))
        .route("/messages/dequeue", post(handle_dequeue))
        .route("/messages/queue", delete(handle_dequeue_all))
        .route("/messages/pending", get(handle_pending))
        .route("/messages/latest/id", get(handle_latest_message_id))
        .route("/messages/latest", get(handle_latest_message))
        .route("/governors/claim", post(handle_claim_governorship))
        .route("/governors/elections/{id}", post(handle_election_vote))
        .route("/governors/refresh", post(handle_refresh_governor_token))
        .route("/governors/transfer", post(handle_transfer_governor))
        .route(
            "/governors/accept-transfer",
            post(handle_accept_governor_transfer),
        )
        .route("/governors/mediate", post(handle_mediate))
        .route("/governors/events", get(handle_gov_events))
        .route("/governors/grants", get(handle_governor_list_grants))
        // Operator-only escape hatch — intentionally absent from the discovery document.
        .route("/admin/governor/reset", post(handle_admin_governor_reset))
        .route("/grants", get(handle_list_grants))
        .route("/grants/approve", post(handle_approve_grant))
        .route("/grants/request", post(handle_grant_request))
        .route("/grants/requests/{id}", patch(handle_grant_request_action))
        .route("/grants/unblock", post(handle_grant_unblock))
        .route("/grants/block", post(handle_grant_block))
        .route("/grants/{id}", delete(handle_revoke_grant))
        .route(
            "/attachments",
            post(handle_attach_upload).layer(DefaultBodyLimit::max(max_attach)),
        )
        .route("/attachments/{id}", get(handle_attach_download))
        // ── Rooms discovery ────────────────────────────────────────────────────
        // Static route for POST /room/create (mint a new room).
        // An explicit GET on this path returns 400 so that "create" stays reserved
        // as a room_id: `GET /room/create` would otherwise be shadowed by the
        // static registration and never reach `GET /room/{room_id}`.
        .route(
            "/room/create",
            post(handle_room_create).get(handle_room_create_get_reserved),
        )
        .route("/room/{room_id}/join", post(handle_room_join))
        .route("/room/{room_id}", get(handle_room_get))
        .route("/room/{room_id}/leave", post(handle_room_leave))
        .with_state(state)
        .layer(axum::middleware::from_fn(log_requests))
}

/// Debug request logger (15-DEBUG). Logs every request on ENTRY (stderr is unbuffered,
/// so the line is flushed immediately) and again on completion with status + latency.
/// Because the release profile is `panic = "abort"`, a handler that panics aborts the
/// process before its completion line is written — so the LAST `[req] ->` line in
/// `docker logs` identifies the request that triggered the crash. Logs method + path
/// only; never the Authorization token or request/response bodies.
async fn log_requests(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    eprintln!("[req] -> {method} {path}");
    let start = Instant::now();
    let resp = next.run(req).await;
    eprintln!(
        "[req] <- {method} {path} {} ({}ms)",
        resp.status().as_u16(),
        start.elapsed().as_millis()
    );
    resp
}

// ── Token extraction helper ───────────────────────────────────────────────────

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

// ── Error mapping ─────────────────────────────────────────────────────────────

fn error_status(e: &Error) -> StatusCode {
    match e {
        Error::AuthFailed | Error::TokenExpired | Error::TokenRejected | Error::TokenRevoked => {
            StatusCode::UNAUTHORIZED
        }
        Error::Forbidden
        | Error::NoGrant
        | Error::GrantExpired
        | Error::GrantExhausted
        | Error::BriefRequired
        | Error::Blocked
        | Error::GrantBlocked(_) => StatusCode::FORBIDDEN,
        Error::NameInUse
        | Error::RecipientOffline
        | Error::MediationUnavailable
        | Error::RequestPending
        | Error::ActiveSubscription => StatusCode::CONFLICT,
        Error::RecipientUnknown | Error::AttachmentNotFound => StatusCode::NOT_FOUND,
        Error::BadRequest => StatusCode::BAD_REQUEST,
        Error::AnnounceRequired => StatusCode::FORBIDDEN,
        Error::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn err_response(e: Error) -> Response {
    let status = error_status(&e);
    let message = e.message();
    let hint = match &e {
        Error::BriefRequired => Some(
            "No authorization covers this message. \
             Resend with a 'reason' field to request authorization from the governor.",
        ),
        _ => None,
    };
    let block_reason = if let Error::GrantBlocked(r) = &e {
        Some(r.clone())
    } else {
        None
    };
    let mut body = json!({"error": e.code(), "message": message});
    if let Some(h) = hint {
        body["hint"] = json!(h);
    }
    if let Some(r) = block_reason {
        body["reason"] = json!(r);
    }
    (status, Json(body)).into_response()
}

fn auth_failed() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "AUTH_FAILED", "message": "authentication required"})),
    )
        .into_response()
}

// ── ISO 8601 timestamp helpers ────────────────────────────────────────────────

fn is_leap_year(year: u32) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

fn epoch_secs_to_iso8601(secs: u64) -> String {
    let time_of_day = secs % 86400;
    let mut days = secs / 86400;
    let h = time_of_day / 3600;
    let m = (time_of_day % 3600) / 60;
    let s = time_of_day % 60;

    let mut year = 1970u32;
    loop {
        let days_in_year = if is_leap_year(year) { 366u64 } else { 365u64 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }

    let month_lengths: [u64; 12] = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut month = 1u32;
    for &ml in &month_lengths {
        if days < ml {
            break;
        }
        days -= ml;
        month += 1;
    }
    let day = days + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, h, m, s
    )
}

fn instant_to_iso8601(instant: Instant) -> String {
    let now_i = Instant::now();
    let now_s = SystemTime::now();
    let system_time = if instant >= now_i {
        now_s + (instant - now_i)
    } else {
        now_s.checked_sub(now_i - instant).unwrap_or(now_s)
    };
    let secs = system_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    epoch_secs_to_iso8601(secs)
}

// ── Request body types ────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct PresenceScopeBody {
    presence_scope: String,
}

#[derive(Deserialize, Default)]
struct ApproveGrantBody {
    identity_a: String,
    identity_b: String,
    expiry_secs: Option<u64>,
    direction: Option<String>,
    max_messages: Option<u64>,
    mediation: Option<String>,
    opens_reply_window: Option<bool>,
    conditions: Option<String>,
}

#[derive(Deserialize, Default)]
struct DequeueBody {
    thread: Option<String>,
}

#[derive(Deserialize, Default)]
struct LatestIdParams {
    since: Option<u64>,
    wait: Option<u64>,
}

#[derive(Deserialize)]
struct SendBody {
    to: Option<String>,
    to_token: Option<String>,
    payload: String,
    reason: Option<String>,
    thread_id: Option<String>,
}

#[derive(Deserialize)]
struct MediateBody {
    mediation_id: String,
    decision: String,
    payload: Option<String>,
}

#[derive(Deserialize)]
struct GrantRequestBody {
    to: String,
    reason: Option<String>,
    /// Optional: re-use a held request (provide the existing request_id).
    request_id: Option<String>,
}

/// Body for PATCH /grants/requests/{id} — unified approve/deny/hold action
#[derive(Deserialize)]
struct GrantRequestActionBody {
    action: String,
    reason: Option<String>,
    expiry_secs: Option<u64>,
    expires_at: Option<u64>,
}

#[derive(Deserialize)]
struct GrantUnblockBody {
    from_identity: String,
    to_name: String,
}

#[derive(Deserialize)]
struct GrantBlockBody {
    from_identity: String,
    to_name: String,
    #[serde(default)]
    reason: String,
    expires_at: Option<u64>,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// GET /participants  — governor-only participant list
async fn handle_list_participants(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov = GovernorToken(tok_str);
    match state.hub.list_participants(&gov) {
        Ok(agents) => {
            let participants_json: Vec<_> = agents
                .iter()
                .map(|a: &ParticipantInfo| json!({"name": a.name, "identity": a.identity, "status": a.status}))
                .collect();
            (
                StatusCode::OK,
                Json(json!({"participants": participants_json})),
            )
                .into_response()
        }
        Err(e) => err_response(e),
    }
}

// POST /governors/refresh  — governor self-rotates their token
async fn handle_refresh_governor_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let old_token = GovernorToken(tok_str);
    match state.hub.refresh_governor_token(&old_token) {
        Ok(new_token) => (StatusCode::OK, Json(json!({"token": new_token.0}))).into_response(),
        Err(e) => err_response(e),
    }
}

// POST /governors/transfer  — current governor creates a one-time transfer token
async fn handle_transfer_governor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov = GovernorToken(tok_str);
    let to_identity = body.get("to").and_then(|v| v.as_str());
    match state.hub.transfer_governor(&gov, to_identity) {
        Ok(transfer_token) => (
            StatusCode::OK,
            Json(json!({"transfer_token": transfer_token})),
        )
            .into_response(),
        Err(e) => err_response(e),
    }
}

// POST /governors/accept-transfer  — a participant claims governorship (FG-5 / security-MAJOR-3).
//
//   Authorization: Bearer <participant-token>   (the claimer; its name is the verified identity)
//   Body: {"transfer_token": "<transfer-token>"}
//
//   200 → {token}                       new governor token
//   401 → no bearer or bearer fails participant validation
//   403 → bearer is a governor token (must be a participant)
//   403 → transfer's to_identity is set and does not match the bearer's name
//   404 → transfer token not found or already consumed
//
// The claiming identity is derived server-side from the verified participant bearer; it is NOT
// read from the request body. The body carries only the transfer token.
async fn handle_accept_governor_transfer(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let bearer = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    // A governor token cannot accept a transfer — must be a participant.
    if state
        .hub
        .validate_governor_token(&GovernorToken(bearer.clone()))
        .is_ok()
    {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "FORBIDDEN", "message": "a participant token is required"})),
        )
            .into_response();
    }
    let transfer_token = match body.get("transfer_token").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "MISSING_FIELD", "message": "missing transfer_token"})),
            )
                .into_response();
        }
    };
    match state.hub.accept_governor_transfer(&bearer, &transfer_token) {
        Ok(new_token) => (StatusCode::OK, Json(json!({"token": new_token.0}))).into_response(),
        // transfer token not found / consumed → 404.
        Err(Error::RecipientUnknown) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "NOT_FOUND", "message": "transfer token not found or consumed"})),
        )
            .into_response(),
        // to_identity mismatch → 403.
        Err(Error::Forbidden) => (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "FORBIDDEN", "message": "transfer is bound to a different identity"})),
        )
            .into_response(),
        // bearer is not a valid named participant → 401.
        Err(e) => err_response(e),
    }
}

// POST /admin/governor/reset  — operator-anchored governor recovery (15-0029). Not advertised in
// the discovery document. Requires the `X-Admin-Secret` header to equal `SIMPLE_IM_ADMIN_SECRET`.
//
//   200 → {governor_token}              new governor installed (old governors revoked, pending
//                                       transfers cleared, committed in one transaction)
//   401 → missing or wrong secret
//   501 → SIMPLE_IM_ADMIN_SECRET unset or empty
async fn handle_admin_governor_reset(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let secret = match &state.admin_secret {
        Some(s) => s,
        None => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                Json(json!({"error": "NOT_IMPLEMENTED", "message": "admin secret not configured"})),
            )
                .into_response();
        }
    };
    let provided = headers.get("X-Admin-Secret").and_then(|v| v.to_str().ok());
    // Constant-ish comparison is unnecessary here (loopback operator hatch); equality check.
    if provided != Some(secret.as_str()) {
        return auth_failed();
    }
    let new_token = state.hub.admin_reset_governor();
    (StatusCode::OK, Json(json!({"governor_token": new_token.0}))).into_response()
}

// DELETE /participants/{name}  — governor force-revokes any participant by name
async fn handle_deregister(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov_tok = GovernorToken(tok_str);
    match state.hub.revoke_by_name(&name, &gov_tok) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_response(e),
    }
}

// GET /participants/{name}/presence
async fn handle_presence(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state.hub.presence_for_token(&tok_str, &name) {
        Ok(is_online) => {
            let status = if is_online { "online" } else { "offline" };
            (StatusCode::OK, Json(json!({"status": status}))).into_response()
        }
        Err(e) => err_response(e),
    }
}

// POST /participants/{name}/presence-scope
async fn handle_set_presence_scope(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(_name): Path<String>,
    Json(body): Json<PresenceScopeBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let result = if body.presence_scope == "hidden" {
        state.hub.hide(&tok_str)
    } else {
        state.hub.show(&tok_str)
    };
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_response(e),
    }
}

// POST /governors/claim
#[derive(Deserialize)]
struct ClaimGovernorshipBody {
    expiry_secs: Option<u64>,
}

async fn handle_claim_governorship(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<ClaimGovernorshipBody>>,
) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let expiry = body.and_then(|b| b.expiry_secs).map(Duration::from_secs);
    match state.hub.claim_governorship(&token, expiry) {
        Ok(ClaimOutcome::Granted { governor_token }) => (
            StatusCode::OK,
            Json(json!({"status": "granted", "governor_token": governor_token})),
        )
            .into_response(),
        Ok(ClaimOutcome::Election { claim_id, voters }) => (
            StatusCode::ACCEPTED,
            Json(json!({"status": "election", "claim_id": claim_id, "voters": voters})),
        )
            .into_response(),
        Ok(ClaimOutcome::Transfer { claim_id }) => (
            StatusCode::ACCEPTED,
            Json(json!({"status": "transfer_pending", "claim_id": claim_id})),
        )
            .into_response(),
        Err(e) => err_response(e),
    }
}

// POST /governors/elections/{id}
#[derive(Deserialize)]
struct ElectionVoteBody {
    action: String,
}

async fn handle_election_vote(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(claim_id): Path<String>,
    Json(body): Json<ElectionVoteBody>,
) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let approve = match body.action.as_str() {
        "approve" => true,
        "reject" => false,
        _ => return err_response(Error::BadRequest),
    };
    match state.hub.respond_claim(&token, &claim_id, approve) {
        Ok(ClaimResolution::Waiting { approved, required }) => (
            StatusCode::OK,
            Json(json!({"status": "waiting", "approved": approved, "required": required})),
        )
            .into_response(),
        Ok(ClaimResolution::Established { .. }) => {
            (StatusCode::OK, Json(json!({"status": "established"}))).into_response()
        }
        Ok(ClaimResolution::Rejected { .. }) => {
            (StatusCode::OK, Json(json!({"status": "rejected"}))).into_response()
        }
        Err(e) => err_response(e),
    }
}

// POST /grants/approve
async fn handle_approve_grant(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<ApproveGrantBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov = GovernorToken(tok_str);
    let expiry = body.expiry_secs.map(Duration::from_secs);

    let direction_str = body.direction.as_deref().unwrap_or("symmetric");
    let direction = match direction_str {
        "symmetric" => GrantDirection::Symmetric,
        "a_to_b" => GrantDirection::AToB,
        "b_to_a" => GrantDirection::BToA,
        _ => return err_response(Error::BadRequest),
    };

    let mediation_str = body.mediation.as_deref().unwrap_or("bypass");
    let mediation = match mediation_str {
        "bypass" => GrantMediation::Bypass,
        "inspect" => GrantMediation::Inspect,
        "notify" => GrantMediation::Notify,
        _ => return err_response(Error::BadRequest),
    };

    let conditions = body.conditions.clone();
    let req = ApproveGrantRequest {
        direction: Some(direction),
        max_messages: body.max_messages,
        opens_reply_window: body.opens_reply_window,
        mediation: Some(mediation),
        conditions: body.conditions,
        // FP1: name_a/name_b may be supplied by the caller (HTTP body) in future;
        // for now the hub's approve_grant_req wrapper auto-resolves them from token_to_name.
        name_a: None,
        name_b: None,
    };

    match state
        .hub
        .approve_grant_req(&gov, &body.identity_a, &body.identity_b, expiry, req)
    {
        Ok(grant_id) => (
            StatusCode::OK,
            Json(json!({
                "grant_id": grant_id,
                "direction": direction_str,
                "max_messages": body.max_messages,
                "mediation": mediation_str,
                "conditions": conditions
            })),
        )
            .into_response(),
        Err(e) => err_response(e),
    }
}

// POST /messages/send  (token in header, to/to_token + payload + optional fields in body)
async fn handle_send(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SendBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let token = ParticipantToken(tok_str);
    let payload = Payload(body.payload.into_bytes());

    // Route: if `to` is present use name routing; if only `to_token` is present use token routing.
    let (ack_result, _recipient_name) = match (body.to.as_deref(), body.to_token.as_deref()) {
        (Some(to_name), _) => {
            let r = state
                .hub
                .send(&token, to_name, payload, body.reason, body.thread_id);
            (r, to_name.to_string())
        }
        (None, Some(to_tok)) => {
            let r = state
                .hub
                .send_to_token(&token, to_tok, payload, body.reason, body.thread_id);
            (r, String::new())
        }
        (None, None) => return err_response(Error::BadRequest),
    };

    match ack_result {
        Ok(crate::delivery::Ack::Accepted) => {
            (StatusCode::ACCEPTED, Json(json!({"status": "accepted"}))).into_response()
        }
        Ok(crate::delivery::Ack::PendingMediation { mediation_id }) => (
            StatusCode::OK,
            Json(json!({"status": "pending_mediation", "mediation_id": mediation_id})),
        )
            .into_response(),
        Err(e) => err_response(e),
    }
}

// POST /messages/queue/pop
async fn handle_dequeue(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<DequeueBody>>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let thread = body.and_then(|b| b.thread.clone());
    match state.hub.dequeue(&tok_str, thread.as_deref()) {
        Ok((msg_opt, remaining)) => match msg_opt {
            None => (
                StatusCode::OK,
                Json(json!({"message": null, "remaining": 0})),
            )
                .into_response(),
            Some(msg) => {
                let payload_str = String::from_utf8_lossy(&msg.payload.0).into_owned();
                let mut m = json!({"payload": payload_str, "from": msg.from_name});
                if let Some(r) = msg.reason {
                    m["reason"] = json!(r);
                }
                if let Some(et) = msg.event_type {
                    m["event_type"] = json!(et);
                }
                if let Some(tid) = msg.thread_id {
                    m["thread_id"] = json!(tid);
                }
                (
                    StatusCode::OK,
                    Json(json!({"message": m, "remaining": remaining})),
                )
                    .into_response()
            }
        },
        Err(e) => err_response(e),
    }
}

// GET /messages/pending
async fn handle_pending(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state.hub.pending_count(&tok_str) {
        Ok(n) => (StatusCode::OK, Json(json!({"pending": n}))).into_response(),
        Err(e) => err_response(e),
    }
}

// GET /messages/latest/id  — non-consuming peek at latest message ID
// Supports optional long-poll: ?since=N&wait=60
// Returns bare integer on 200, 204 on long-poll timeout, 404 if no messages received.
async fn handle_latest_message_id(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<LatestIdParams>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };

    // If long-poll parameters are present, wait for a new message ID.
    if let (Some(since), Some(wait_secs)) = (params.since, params.wait) {
        let max_wait = Duration::from_secs(wait_secs.min(300));
        match state
            .hub
            .wait_for_new_message_id(&tok_str, since, max_wait)
            .await
        {
            Ok(Some(id)) => return (StatusCode::OK, id.to_string()).into_response(),
            Ok(None) => return StatusCode::NO_CONTENT.into_response(),
            Err(e) => return err_response(e),
        }
    }

    // Non-blocking peek.
    match state.hub.latest_message_id(&tok_str) {
        Ok(Some(id)) => (StatusCode::OK, id.to_string()).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => err_response(e),
    }
}

// GET /messages/latest  — non-consuming peek at full latest queued message
async fn handle_latest_message(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state.hub.peek_latest_message(&tok_str) {
        Ok(Some(msg)) => {
            let payload_str = String::from_utf8_lossy(&msg.payload.0).into_owned();
            let mut m = json!({"payload": payload_str, "from": msg.from_name});
            if let Some(r) = msg.reason {
                m["reason"] = json!(r);
            }
            if let Some(et) = msg.event_type {
                m["event_type"] = json!(et);
            }
            if let Some(tid) = msg.thread_id {
                m["thread_id"] = json!(tid);
            }
            (StatusCode::OK, Json(json!({"message": m}))).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "NO_MESSAGES", "message": "no messages in queue"})),
        )
            .into_response(),
        Err(e) => err_response(e),
    }
}

// POST /grants/request  — agent requests a grant to talk to another agent
//
// Gate (AC8/AC9): the caller must either already hold a grant with the target
// OR be co-present in a room with the target.  This prevents cold-contact spam:
// room discovery is the bootstrap path for first-contact grant requests.
async fn handle_grant_request(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<GrantRequestBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };

    // Resolve caller name for room check.  If the caller has no announced name
    // they cannot possibly be in a room or hold a named grant → reject.
    let caller_name = match state.hub.name_for_bearer_token(&tok_str) {
        Some(n) => n,
        None => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "FORBIDDEN",
                    "message": "must announce a name before requesting a grant"
                })),
            )
                .into_response();
        }
    };

    // AC9: no existing grant AND no shared room → reject.
    if !state.hub.has_any_grant_with(&tok_str, &body.to)
        && !state.rooms.shares_room(&caller_name, &body.to)
    {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "FORBIDDEN",
                "message": "must share a room or hold an existing grant to submit a grant request"
            })),
        )
            .into_response();
    }

    match state
        .hub
        .request_grant(&tok_str, &body.to, body.reason, body.request_id.as_deref())
    {
        Ok(request_id) => (StatusCode::OK, Json(json!({"request_id": request_id}))).into_response(),
        Err(e) => err_response(e),
    }
}

// POST /grants/unblock  — governor removes a persistent denial block
async fn handle_grant_unblock(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<GrantUnblockBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state
        .hub
        .unblock_grant(&tok_str, &body.from_identity, &body.to_name)
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_response(e),
    }
}

// POST /grants/block  — governor directly creates a denial block
async fn handle_grant_block(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<GrantBlockBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov = GovernorToken(tok_str);
    match state.hub.block_direct(
        &gov,
        &body.from_identity,
        &body.to_name,
        &body.reason,
        body.expires_at,
    ) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_response(e),
    }
}

// PATCH /grants/requests/{id}  — approve, deny, or hold a pending grant request
async fn handle_grant_request_action(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<GrantRequestActionBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match body.action.as_str() {
        "approve" => {
            let expiry = body.expiry_secs.map(Duration::from_secs);
            match state.hub.approve_grant_request(&tok_str, &id, expiry) {
                Ok(ApproveStatus::PendingRecipient) => {
                    (StatusCode::OK, Json(json!({"status": "pending_recipient"}))).into_response()
                }
                Ok(ApproveStatus::Established) => {
                    (StatusCode::OK, Json(json!({"status": "established"}))).into_response()
                }
                Err(e) => err_response(e),
            }
        }
        "deny" => {
            let reason = body.reason.unwrap_or_default();
            match state
                .hub
                .deny_grant_request(&tok_str, &id, &reason, body.expires_at)
            {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(e) => err_response(e),
            }
        }
        "hold" => {
            let reason = match body.reason {
                Some(r) => r,
                None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "BAD_REQUEST", "message": "reason is required for hold action"}))).into_response(),
            };
            match state.hub.hold_grant_request(&tok_str, &id, &reason) {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(e) => err_response(e),
            }
        }
        _ => (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error": "BAD_REQUEST", "message": "action must be approve, deny, or hold"}),
            ),
        )
            .into_response(),
    }
}

// DELETE /grants/{id}  — governor revokes an established grant by ID
async fn handle_revoke_grant(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov = GovernorToken(tok_str);
    match state.hub.revoke_grant(&id, &gov) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_response(e),
    }
}

// POST /governors/mediate  — governor resolves a brief-auth hold
async fn handle_mediate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<MediateBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov = GovernorToken(tok_str);

    let decision = match body.decision.as_str() {
        "approve" => MediationDecision::Approve,
        "block" => MediationDecision::Block,
        "modify" => match body.payload {
            Some(p) => MediationDecision::Modify(Payload(p.into_bytes())),
            None => return err_response(Error::BadRequest),
        },
        _ => return err_response(Error::BadRequest),
    };

    match state
        .hub
        .resolve_mediation(&gov, &body.mediation_id, decision)
    {
        Ok(MediationResult::Delivered { .. }) => {
            (StatusCode::OK, Json(json!({"status": "delivered"}))).into_response()
        }
        Ok(MediationResult::Blocked) => {
            (StatusCode::OK, Json(json!({"status": "blocked"}))).into_response()
        }
        Ok(MediationResult::RecipientOffline) => err_response(Error::RecipientOffline),
        Err(e) => err_response(e),
    }
}

// GET /governors/events  — SSE stream of governor events (mediation holds + notify deliveries)
async fn handle_gov_events(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov = GovernorToken(tok_str);
    if let Err(e) = state.hub.validate_governor_token(&gov) {
        return err_response(e);
    }

    let rx = state.hub.subscribe_gov_events();
    let stream = BroadcastStream::new(rx)
        .filter_map(|r: Result<String, _>| r.ok())
        .map(|data| Ok::<Event, Infallible>(Event::default().data(data)));

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

// \u2500\u2500 request body types \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

// ── request body types ─────────────────────────────────────────────────────

// announce body — deserialized inline via serde_json::Value in handle_announce
#[allow(dead_code)]
#[derive(Deserialize)]
struct AnnounceBody {
    name: String,
}

#[derive(Deserialize, Default)]
struct DequeueAllBody {
    thread: Option<String>,
}

// ── GET / — discovery JSON ─────────────────────────────────────────────────────

/// Returns the machine-readable discovery document listing all API routes.
/// This is the canonical source of truth for the API surface; the drift-guard
/// test in this module ensures router ↔ discovery consistency.
async fn handle_discovery() -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "service": "simple-im",
            "version": "2",
            "openapi": "/openapi.yaml",
            "entry": "POST /register",
            "description": "Register with POST /register to receive a token, then POST /listen with that token to open your SSE stream. POST /announce to claim a name. See the participant skill for the full flow.",
            "skill": "/skills/participant",
            "auth": "Bearer <token> in the Authorization header. Token types: listen-token (participant), governor-token (governor). Gate on HTTP status code; errors are {\"error\":CODE,\"message\":...}.",
            "routes": {
                "discovery": {
                    "GET /": {"auth": "none", "body": null, "hint": "Returns this discovery document"}
                },
                "skill": {
                    "GET /skills/participant": {"auth": "none", "body": null, "hint": "Participant skill markdown"},
                    "GET /skills/participant/listen.sh": {"auth": "none", "body": null, "hint": "Participant listen script"},
                    "GET /skills/governor": {"auth": "none", "body": null, "hint": "Governor skill markdown"},
                    "GET /openapi.yaml": {"auth": "none", "body": null, "hint": "OpenAPI 3.x specification (YAML)"}
                },
                "participant": {
                    "POST /register": {"auth": "governor", "body": "{name?}", "hint": "Governor issues a participant token; with {name} atomically rebinds an existing identity. Open during bootstrap (no governor)."},
                    "POST /listen": {"auth": "participant", "body": "{name?}", "hint": "Open SSE stream; optional name to auto-announce"},
                    "DELETE /listen": {"auth": "participant", "body": null, "hint": "Close SSE stream, unbind name"},
                    "POST /announce": {"auth": "participant", "body": "{name}", "hint": "Claim a name for this token"},
                    "GET /participants": {"auth": "governor", "body": null, "hint": "List all announced participants"},
                    "DELETE /participants/{name}": {"auth": "governor", "body": null, "hint": "Force-revoke participant by name"},
                    "GET /participants/{name}/presence": {"auth": "participant", "body": null, "hint": "Check if participant is online"},
                    "POST /participants/{name}/presence-scope": {"auth": "participant", "body": "{presence_scope}", "hint": "Set presence visibility (hidden/visible)"}
                },
                "message": {
                    "POST /messages/send": {"auth": "participant", "body": "{to|to_token, payload, reason?, thread_id?}", "hint": "Send message to participant"},
                    "POST /messages/queue/pop": {"auth": "participant", "body": "{thread?}", "hint": "Dequeue one message"},
                    "POST /messages/dequeue": {"auth": "participant", "body": "{thread?}", "hint": "Alias for POST /messages/queue/pop"},
                    "DELETE /messages/queue": {"auth": "participant", "body": "{thread?}", "hint": "Drain all queued messages"},
                    "GET /messages/pending": {"auth": "participant", "body": null, "hint": "Count pending messages"},
                    "GET /messages/latest/id": {"auth": "participant", "body": null, "hint": "Peek latest message ID (supports long-poll: ?since=N&wait=60)"},
                    "GET /messages/latest": {"auth": "participant", "body": null, "hint": "Peek full latest message without consuming"}
                },
                "grant": {
                    "GET /grants": {"auth": "participant", "body": null, "hint": "List your active grants"},
                    "POST /grants/request": {"auth": "participant", "body": "{to, reason?, request_id?}", "hint": "Request a grant to reach a peer"},
                    "PATCH /grants/requests/{id}": {"auth": "participant|governor", "body": "{action, reason?, expiry_secs?}", "hint": "Approve/deny/hold a grant request"},
                    "POST /grants/approve": {"auth": "governor", "body": "{identity_a, identity_b, expiry_secs?, direction?, max_messages?, mediation?, conditions?}", "hint": "Directly approve a grant pair"},
                    "POST /grants/block": {"auth": "governor", "body": "{from_identity, to_name, reason, expires_at?}", "hint": "Persistently block a sender→recipient pair"},
                    "POST /grants/unblock": {"auth": "governor", "body": "{from_identity, to_name}", "hint": "Remove a persistent block"},
                    "DELETE /grants/{id}": {"auth": "governor", "body": null, "hint": "Revoke a grant by ID"}
                },
                "governor": {
                    "POST /governors/claim": {"auth": "participant", "body": "{expiry_secs?}", "hint": "Claim governorship (may trigger election)"},
                    "POST /governors/elections/{id}": {"auth": "participant", "body": "{action}", "hint": "Vote on a pending election/transfer"},
                    "POST /governors/refresh": {"auth": "governor", "body": null, "hint": "Rotate governor token"},
                    "POST /governors/transfer": {"auth": "governor", "body": "{to?}", "hint": "Initiate governor transfer"},
                    "POST /governors/accept-transfer": {"auth": "participant", "body": "{transfer_token}", "hint": "Accept governor transfer (claimer identity derived from the participant bearer)"},
                    "POST /governors/mediate": {"auth": "governor", "body": "{mediation_id, decision, payload?}", "hint": "Resolve a mediation hold"},
                    "GET /governors/events": {"auth": "governor", "body": null, "hint": "SSE stream of governor events"},
                    "GET /governors/grants": {"auth": "governor", "body": null, "hint": "List all grants in the system (supports ?participant=)"}
                },
                "attachment": {
                    "POST /attachments": {"auth": "participant", "body": "raw bytes (query: ?to=&filename=&note=)", "hint": "Upload file attachment"},
                    "GET /attachments/{id}": {"auth": "participant", "body": null, "hint": "Download attachment by ID"}
                },
                "room": {
                    "POST /room/create": {"auth": "participant", "body": null, "hint": "Create a new room (returns room_id)"},
                    "GET /room/create": {"auth": "none", "body": null, "hint": "Reserved path — returns 400"},
                    "POST /room/{room_id}/join": {"auth": "participant", "body": "{ttl?}", "hint": "Join a room (idempotent)"},
                    "GET /room/{room_id}": {"auth": "participant", "body": null, "hint": "Get room members (must be a member)"},
                    "POST /room/{room_id}/leave": {"auth": "participant", "body": null, "hint": "Leave a room (idempotent)"}
                }
            }
        })),
    )
        .into_response()
}

// ── Attachments ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct AttachUploadParams {
    to: String,
    filename: Option<String>,
    note: Option<String>,
}

/// `POST /attachments?to=<name>&filename=<f>&note=<text>` — body is the raw file bytes,
/// `Content-Type` is the mime. Grant-gated like send; stores the blob server-side and
/// notifies the recipient (metadata only). Body size is bounded by the per-route
/// `DefaultBodyLimit` (`SIMPLE_IM_ATTACHMENT_MAX_BYTES`); oversize → 413.
async fn handle_attach_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<AttachUploadParams>,
    body: Bytes,
) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    if body.is_empty() {
        return err_response(Error::BadRequest);
    }
    let mime = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(';').next().unwrap_or(s).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let filename = sanitize_filename(params.filename.as_deref().unwrap_or("file"));
    match state
        .hub
        .attach(
            &token,
            &params.to,
            &filename,
            &mime,
            body.to_vec(),
            params.note.as_deref(),
            state.attachment_ttl,
        )
        .await
    {
        Ok(meta) => (
            StatusCode::CREATED,
            Json(json!({
                "attachment_id": meta.id,
                "filename": meta.filename,
                "mime": meta.mime,
                "size": meta.size,
            })),
        )
            .into_response(),
        Err(e) => err_response(e),
    }
}

/// `GET /attachments/{id}` — returns the raw bytes on demand. Access-controlled to the
/// sender's identity or the intended recipient's bound name.
async fn handle_attach_download(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state.hub.fetch_attachment(&token, &id).await {
        Ok((bytes, filename, mime)) => {
            let mut resp = (StatusCode::OK, bytes).into_response();
            let h = resp.headers_mut();
            if let Ok(ct) = axum::http::HeaderValue::from_str(&mime) {
                h.insert(axum::http::header::CONTENT_TYPE, ct);
            }
            let disp = format!("attachment; filename=\"{}\"", sanitize_filename(&filename));
            if let Ok(cd) = axum::http::HeaderValue::from_str(&disp) {
                h.insert(axum::http::header::CONTENT_DISPOSITION, cd);
            }
            resp
        }
        Err(e) => err_response(e),
    }
}

/// Strip control chars, quotes, and backslashes from a client-supplied filename
/// (header-injection safe); bound to 255 chars; never empty.
fn sanitize_filename(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|c| !c.is_control() && *c != '"' && *c != '\\')
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        "file".to_string()
    } else {
        cleaned.chars().take(255).collect()
    }
}

// ── POST /register ─────────────────────────────────────────────────────────────
// Governor-gated participant token issuance (15-0029 / FG-2).
//
//   Authorization: Bearer <governor-token>        (required once a governor is active)
//   Body (optional): {"name": "<existing-identity>"}   → atomic governor rebind
//
//   200 → {token}            (no name → fresh unbound token)
//   200 → {token, name}      (with name → token bound to the identity)
//   401 → no/invalid governor bearer when a governor is active
//   403 → bearer is a valid participant token (not a governor)
//   404 → name given but not a registered identity
//   503 → bootstrap window closed by timeout with no governor established
//
// Bootstrap window: while no governor exists, /register is open (no auth). Each use logs WARN.
async fn handle_register(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    let name = body
        .as_ref()
        .and_then(|b| b.0.get("name"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if !state.hub.has_active_governor() {
        // Bootstrap escape hatch (chicken-and-egg): open registration until a governor exists.
        if let Some(deadline) = state.bootstrap_deadline
            && Instant::now() > deadline
        {
            eprintln!("BOOTSTRAP TIMEOUT: no governor established; closing registration window");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "bootstrap window closed; contact operator"})),
            )
                .into_response();
        }
        let token = state.hub.register_participant();
        eprintln!("WARN: POST /register served via open bootstrap window (no active governor)");
        return (StatusCode::OK, Json(json!({"token": token}))).into_response();
    }

    // A governor is active — a governor bearer is required.
    let bearer = match bearer_token(&headers) {
        Some(b) => b,
        None => return auth_failed(),
    };
    // 403 (not 401) when the presenter is a participant token (completeness-M3).
    if state.hub.is_participant_token(&bearer) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "FORBIDDEN",
                "message": "a participant token cannot register participants; present the governor token"
            })),
        )
            .into_response();
    }
    let gov = GovernorToken(bearer);
    match state.hub.issue_participant_token(&gov, name.as_deref()) {
        Ok((token, Some(name))) => {
            (StatusCode::OK, Json(json!({"token": token, "name": name}))).into_response()
        }
        Ok((token, None)) => (StatusCode::OK, Json(json!({"token": token}))).into_response(),
        Err(Error::RecipientUnknown) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "RECIPIENT_UNKNOWN", "message": "name not found in identities"})),
        )
            .into_response(),
        Err(Error::Forbidden) => (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "FORBIDDEN", "message": "governor token required"})),
        )
            .into_response(),
        // Invalid/expired governor bearer → 401.
        Err(e) => err_response(e),
    }
}

// ── POST /listen ──────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct ListenBody {
    name: Option<String>,
    /// Opt-in for push presence events (online/offline) from grant-peers.
    /// When absent or false (the default), no presence events are delivered.
    /// Must be set per subscription — not persisted across reconnects.
    presence_push: Option<bool>,
}

#[derive(Deserialize, Default)]
struct ListenQueryParams {
    force: Option<bool>,
}

async fn handle_listen(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<ListenQueryParams>,
    body: Option<Json<ListenBody>>,
) -> Response {
    // Token is required — no anonymous /listen.
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let force = params.force.unwrap_or(false);

    let peer_ip = headers
        .get("X-Forwarded-For")
        .or_else(|| headers.get("X-Real-IP"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string());

    let observed_host = headers
        .get("X-Forwarded-Host")
        .or_else(|| headers.get("Host"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let name_to_bind = body.as_ref().and_then(|b| b.0.name.as_deref());
    let presence_push = body
        .as_ref()
        .and_then(|b| b.0.presence_push)
        .unwrap_or(false);

    let (token, rx) = match state.hub.open_listen(
        Some(&token),
        peer_ip,
        name_to_bind,
        observed_host,
        force,
        presence_push,
    ) {
        Ok(pair) => pair,
        Err(e) => return err_response(e),
    };

    let token_for_drop = token.clone();
    let state_for_drop = Arc::clone(&state);

    let stream = SseDropGuard {
        inner: tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
            .map(|data| Ok::<Event, Infallible>(Event::default().data(data))),
        on_drop: Some(Box::new(move || {
            state_for_drop.hub.close_listen(&token_for_drop);
        })),
    };

    // SIM-2: keep-alive heartbeat every 20 s — safely below both the 60 s proxy
    // idle-timeout and the 30 s presence liveness window. The `: keepalive`
    // comment is ignored by SSE clients (per spec) and only emitted when no
    // real event has been sent during the interval.
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(20))
                .text("keepalive"),
        )
        .into_response()
}

// ── DELETE /listen ────────────────────────────────────────────────────────────

async fn handle_cancel_listen(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state.hub.cancel_listen(&token) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "NOT_FOUND", "message": "no active subscription"})),
        )
            .into_response(),
    }
}

// ── POST /announce ─────────────────────────────────────────────────────────────

async fn handle_announce(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };

    let name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return err_response(Error::BadRequest),
    };
    // FG-1: force-reclaim is removed. Any `force` field in the body is ignored.
    match state.hub.announce(&token, &name) {
        Ok(AnnounceResult::Bound) => StatusCode::NO_CONTENT.into_response(),
        Ok(AnnounceResult::NameInUse { resolution_stream }) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "NAME_IN_USE",
                "message": "name is currently in use",
                "resolution_stream": resolution_stream,
                "resolution": "contact the governor to rebind your identity to a new credential"
            })),
        )
            .into_response(),
        Err(e) => err_response(e),
    }
}

// ── POST /messages/dequeue/all ────────────────────────────────────────────────

async fn handle_dequeue_all(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<DequeueAllBody>>,
) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let thread = body.and_then(|b| b.thread.clone());
    match state.hub.drain_queue(&token, thread.as_deref()) {
        Ok(messages) => {
            let msgs_json: Vec<_> = messages
                .iter()
                .map(|m| {
                    let payload_str = String::from_utf8_lossy(&m.payload.0).into_owned();
                    let mut obj = json!({"payload": payload_str, "from": m.from_name});
                    if let Some(ref r) = m.reason {
                        obj["reason"] = json!(r);
                    }
                    if let Some(ref et) = m.event_type {
                        obj["event_type"] = json!(et);
                    }
                    if let Some(ref tid) = m.thread_id {
                        obj["thread_id"] = json!(tid);
                    }
                    obj
                })
                .collect();
            (StatusCode::OK, Json(json!({"messages": msgs_json}))).into_response()
        }
        Err(e) => err_response(e),
    }
}

// ── GET /governors/grants — governor views all active grants in the system ────

async fn handle_governor_list_grants(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let gov = GovernorToken(tok_str);
    let participant = params.get("participant").map(|s| s.as_str());
    match state.hub.list_all_grants_gov(&gov, participant) {
        Ok(items) => {
            let grants_json: Vec<_> = items
                .into_iter()
                .map(|item| {
                    let direction_str = match item.direction {
                        crate::trust::GrantDirection::Symmetric => "symmetric",
                        crate::trust::GrantDirection::AToB => "a_to_b",
                        crate::trust::GrantDirection::BToA => "b_to_a",
                    };
                    let expires_val = item
                        .expires
                        .map(|inst| serde_json::Value::String(instant_to_iso8601(inst)))
                        .unwrap_or(serde_json::Value::Null);
                    json!({
                        "id": item.id,
                        "identity_a": item.identity_a,
                        "identity_b": item.identity_b,
                        "name_a": item.name_a,
                        "name_b": item.name_b,
                        "direction": direction_str,
                        "expires": expires_val,
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!({"grants": grants_json}))).into_response()
        }
        Err(e) => err_response(e),
    }
}

// ── GET /grants — list active grants for the calling participant ──────────────

async fn handle_list_grants(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state.hub.list_grants_for_token(&tok_str) {
        Ok(items) => {
            let grants_json: Vec<_> = items
                .into_iter()
                .map(|item| {
                    let direction_str = match item.direction {
                        crate::trust::GrantDirection::Symmetric => "symmetric",
                        crate::trust::GrantDirection::AToB => "a_to_b",
                        crate::trust::GrantDirection::BToA => "b_to_a",
                    };
                    let expires_val = item
                        .expires
                        .map(|inst| serde_json::Value::String(instant_to_iso8601(inst)))
                        .unwrap_or(serde_json::Value::Null);
                    json!({
                        "id": item.id,
                        "counterparty": item.counterparty,
                        "direction": direction_str,
                        "expires": expires_val,
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!({"grants": grants_json}))).into_response()
        }
        Err(e) => err_response(e),
    }
}

// ── Skill download endpoints (unauthenticated) ────────────────────────────────

async fn handle_skill_participant() -> impl IntoResponse {
    (
        [("content-type", "text/plain; charset=utf-8")],
        PARTICIPANT_SKILL_MD,
    )
}

async fn handle_skill_participant_listen() -> impl IntoResponse {
    (
        [("content-type", "text/plain; charset=utf-8")],
        PARTICIPANT_LISTEN_SH,
    )
}

async fn handle_skill_governor() -> impl IntoResponse {
    (
        [("content-type", "text/plain; charset=utf-8")],
        GOVERNOR_SKILL_MD,
    )
}

/// GET /openapi.yaml — returns the OpenAPI 3.x specification.
async fn handle_openapi() -> impl IntoResponse {
    ([("content-type", "text/yaml; charset=utf-8")], OPENAPI_YAML)
}

// ── Rooms discovery ───────────────────────────────────────────────────────────
//
// All room routes require `Authorization: Bearer <token>`.  The token must
// resolve to an announced agent name; otherwise 401 is returned.
//
// Room IDs are server-generated UUIDs.  The string "create" is reserved and
// may not appear as a room_id in join / get / leave paths (→ 400).

/// Helper: extract bearer token AND resolve it to an announced name.
/// Returns `None` if the header is missing, the token is unknown/revoked, or
/// the agent has not yet announced a name.
fn room_auth(headers: &HeaderMap, state: &AppState) -> Option<String> {
    let tok = bearer_token(headers)?;
    state.hub.name_for_bearer_token(&tok)
}

/// Returns 401 AUTH_FAILED — used when the room bearer-token check fails.
fn room_auth_failed() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "AUTH_FAILED", "message": "valid bearer token with announced name required"})),
    )
        .into_response()
}

/// Returns 400 BAD_REQUEST — reserved name "create" used as room_id.
fn room_id_reserved() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "BAD_REQUEST", "message": "\"create\" is a reserved name and cannot be used as a room_id"})),
    )
        .into_response()
}

// ── POST /room/create ─────────────────────────────────────────────────────────

/// POST /room/create — mint a new room and return its UUID.
/// The caller is NOT automatically joined.
async fn handle_room_create(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if room_auth(&headers, &state).is_none() {
        return room_auth_failed();
    }
    let room_id = state.rooms.create();
    (StatusCode::OK, Json(json!({"room_id": room_id}))).into_response()
}

/// GET /room/create — "create" is reserved; return 400.
///
/// Without this explicit handler, Axum would return 405 (Method Not Allowed)
/// because the static `/room/create` path is registered only for POST, and it
/// shadows the dynamic `GET /room/{room_id}` route at that position.
async fn handle_room_create_get_reserved() -> Response {
    room_id_reserved()
}

// ── POST /room/{room_id}/join ─────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct RoomJoinBody {
    ttl: Option<u64>,
}

/// POST /room/{room_id}/join — add the caller to the room.
/// Idempotent (re-join resets TTL).  Returns the current member list.
async fn handle_room_join(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    body: Option<Json<RoomJoinBody>>,
) -> Response {
    if room_id == "create" {
        return room_id_reserved();
    }
    let caller = match room_auth(&headers, &state) {
        Some(n) => n,
        None => return room_auth_failed(),
    };
    let ttl = body.as_ref().and_then(|b| b.0.ttl);
    match state.rooms.join(&room_id, &caller, ttl) {
        Ok(names) => {
            let members: Vec<_> = names
                .iter()
                .map(|n| json!({"name": n, "online": state.hub.presence(n)}))
                .collect();
            (StatusCode::OK, Json(json!({"members": members}))).into_response()
        }
        Err(crate::rooms::RoomError::RoomNotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "NOT_FOUND", "message": "room not found"})),
        )
            .into_response(),
        Err(crate::rooms::RoomError::NotMember) => unreachable!("join never returns NotMember"),
    }
}

// ── GET /room/{room_id} ───────────────────────────────────────────────────────

/// GET /room/{room_id} — return the live member list.
/// 403 if the caller is not a member.
async fn handle_room_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    if room_id == "create" {
        return room_id_reserved();
    }
    let caller = match room_auth(&headers, &state) {
        Some(n) => n,
        None => return room_auth_failed(),
    };
    match state.rooms.members(&room_id, &caller) {
        Ok(names) => {
            let members: Vec<_> = names
                .iter()
                .map(|n| json!({"name": n, "online": state.hub.presence(n)}))
                .collect();
            (StatusCode::OK, Json(json!({"members": members}))).into_response()
        }
        Err(crate::rooms::RoomError::RoomNotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "NOT_FOUND", "message": "room not found"})),
        )
            .into_response(),
        Err(crate::rooms::RoomError::NotMember) => (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "FORBIDDEN", "message": "caller is not a member of this room"})),
        )
            .into_response(),
    }
}

// ── POST /room/{room_id}/leave ────────────────────────────────────────────────

/// POST /room/{room_id}/leave — remove the caller from the room.
/// Idempotent: 200 even if the caller was not a member or the room doesn't exist.
async fn handle_room_leave(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    if room_id == "create" {
        return room_id_reserved();
    }
    let caller = match room_auth(&headers, &state) {
        Some(n) => n,
        None => return room_auth_failed(),
    };
    state.rooms.leave(&room_id, &caller);
    (StatusCode::OK, Json(json!({"status": "ok"}))).into_response()
}

// ── Drift guard: router ↔ discovery consistency ───────────────────────────────

/// Returns all routes registered in the router.
/// Each entry is "METHOD /path" (e.g., "GET /", "POST /register").
#[cfg(test)]
fn router_routes() -> Vec<String> {
    // This is the canonical list of routes. If you add a route to the router,
    // add it here too. The drift guard test will catch mismatches.
    vec![
        "GET /".to_string(),
        "GET /openapi.yaml".to_string(),
        "POST /register".to_string(),
        "POST /listen".to_string(),
        "DELETE /listen".to_string(),
        "POST /announce".to_string(),
        "GET /skills/participant".to_string(),
        "GET /skills/participant/listen.sh".to_string(),
        "GET /skills/governor".to_string(),
        "GET /participants".to_string(),
        "DELETE /participants/{name}".to_string(),
        "GET /participants/{name}/presence".to_string(),
        "POST /participants/{name}/presence-scope".to_string(),
        "POST /messages/send".to_string(),
        "POST /messages/queue/pop".to_string(),
        "POST /messages/dequeue".to_string(),
        "DELETE /messages/queue".to_string(),
        "GET /messages/pending".to_string(),
        "GET /messages/latest/id".to_string(),
        "GET /messages/latest".to_string(),
        "POST /governors/claim".to_string(),
        "POST /governors/elections/{id}".to_string(),
        "POST /governors/refresh".to_string(),
        "POST /governors/transfer".to_string(),
        "POST /governors/accept-transfer".to_string(),
        "POST /governors/mediate".to_string(),
        "GET /governors/events".to_string(),
        "GET /governors/grants".to_string(),
        "GET /grants".to_string(),
        "POST /grants/approve".to_string(),
        "POST /grants/request".to_string(),
        "PATCH /grants/requests/{id}".to_string(),
        "POST /grants/unblock".to_string(),
        "POST /grants/block".to_string(),
        "DELETE /grants/{id}".to_string(),
        "POST /attachments".to_string(),
        "GET /attachments/{id}".to_string(),
        "POST /room/create".to_string(),
        "GET /room/create".to_string(),
        "POST /room/{room_id}/join".to_string(),
        "GET /room/{room_id}".to_string(),
        "POST /room/{room_id}/leave".to_string(),
    ]
}

/// Extracts routes from the LIVE discovery handler response.
/// This calls handle_discovery() and parses its JSON output to get the actual
/// routes being advertised, ensuring the test detects real drift.
#[cfg(test)]
fn discovery_routes() -> Vec<String> {
    use tokio::runtime::Runtime;

    // Create a runtime to call the async handler
    let rt = Runtime::new().expect("Failed to create runtime");
    let response = rt.block_on(handle_discovery());

    // Extract the JSON body from the response
    let body = rt.block_on(async {
        let (_, body) = response.into_parts();
        axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("Failed to read body")
    });

    let discovery: serde_json::Value =
        serde_json::from_slice(&body).expect("Discovery response is not valid JSON");

    // Extract all routes from the "routes" object
    let mut routes = Vec::new();
    if let Some(categories) = discovery["routes"].as_object() {
        for (_category, endpoints) in categories {
            if let Some(eps) = endpoints.as_object() {
                for (route, _) in eps {
                    routes.push(route.clone());
                }
            }
        }
    }
    routes.sort();
    routes
}

#[cfg(test)]
mod drift_guard_tests {
    use super::*;
    use std::collections::HashSet;

    /// Drift guard test: ensures router routes match discovery routes.
    /// If this test fails, either:
    /// 1. A route was added to the router but not to discovery (update handle_discovery)
    /// 2. A route was added to discovery but not to the router (remove from discovery or add to router)
    /// 3. A route was renamed in one place but not the other (update both)
    #[test]
    fn router_discovery_consistency() {
        let router_set: HashSet<String> = router_routes().into_iter().collect();
        let discovery_set: HashSet<String> = discovery_routes().into_iter().collect();

        let in_router_not_discovery: Vec<_> = router_set.difference(&discovery_set).collect();
        let in_discovery_not_router: Vec<_> = discovery_set.difference(&router_set).collect();

        let mut errors = Vec::new();

        if !in_router_not_discovery.is_empty() {
            errors.push(format!(
                "Routes in router but missing from discovery:\n  {}",
                in_router_not_discovery
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join("\n  ")
            ));
        }

        if !in_discovery_not_router.is_empty() {
            errors.push(format!(
                "Routes in discovery but missing from router:\n  {}",
                in_discovery_not_router
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join("\n  ")
            ));
        }

        if !errors.is_empty() {
            panic!(
                "Router ↔ Discovery drift detected!\n\n{}\n\nUpdate handle_discovery() or router() to fix.",
                errors.join("\n\n")
            );
        }
    }

    /// Validates that the OpenAPI spec can be parsed as YAML.
    #[test]
    fn openapi_spec_is_valid_yaml() {
        // Basic validation: the spec should start with "openapi:" and contain "paths:"
        assert!(
            OPENAPI_YAML.starts_with("openapi:"),
            "OpenAPI spec should start with 'openapi:'"
        );
        assert!(
            OPENAPI_YAML.contains("paths:"),
            "OpenAPI spec should contain 'paths:'"
        );
        assert!(
            OPENAPI_YAML.contains("/register:"),
            "OpenAPI spec should document /register"
        );
        assert!(
            OPENAPI_YAML.contains("/listen:"),
            "OpenAPI spec should document /listen"
        );
        assert!(
            OPENAPI_YAML.contains("/openapi.yaml:"),
            "OpenAPI spec should document /openapi.yaml"
        );
    }
}
