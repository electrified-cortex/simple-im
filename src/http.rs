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
    AgentInfo, AnnounceResult, ApproveStatus, ClaimOutcome, ClaimResolution, DeliveryHub,
    MediationDecision, MediationResult,
};
use crate::error::Error;
use crate::trust::{ApproveGrantRequest, GrantDirection, GrantMediation};
use crate::types::{AgentToken, GovernorToken, Payload};

// ── Bundled skill files ───────────────────────────────────────────────────────

const PARTICIPANT_SKILL_MD: &str = include_str!("../skills/participant/SKILL.md");
const PARTICIPANT_LISTEN_SH: &str = include_str!("../skills/participant/listen.sh");
const GOVERNOR_SKILL_MD: &str = include_str!("../skills/governor/SKILL.md");


// ── State ─────────────────────────────────────────────────────────────────────

/// Shared Axum application state holding the delivery hub and attachment configuration.
pub struct AppState {
    pub hub: DeliveryHub,
    pub attachment_ttl: Duration,
    pub attachment_max_bytes: usize,
}

impl AppState {
    /// Creates `AppState` with an in-memory hub; used in tests and default startup.
    pub fn new(liveness_window: Duration) -> Self {
        let (attachment_ttl, attachment_max_bytes) = attachment_config();
        Self {
            hub: DeliveryHub::new(liveness_window),
            attachment_ttl,
            attachment_max_bytes,
        }
    }

    /// Creates `AppState` wrapping an existing hub; used when restoring persisted state.
    pub fn new_with_hub(hub: DeliveryHub) -> Self {
        let (attachment_ttl, attachment_max_bytes) = attachment_config();
        Self {
            hub,
            attachment_ttl,
            attachment_max_bytes,
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
        .route("/agents/register", post(handle_agents_register))
        .route("/listen", post(handle_listen))
        .route("/listen", delete(handle_cancel_listen))
        .route("/announce", post(handle_announce))
        .route("/introduce", post(handle_introduce))
        .route("/connect-probe-ack", post(handle_probe_ack))
        .route("/leave", post(handle_leave))
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
        Error::RecipientUnknown | Error::IdentityNotFound | Error::AttachmentNotFound => {
            StatusCode::NOT_FOUND
        }
        Error::BadRequest => StatusCode::BAD_REQUEST,
        Error::AnnounceRequired => StatusCode::FORBIDDEN,
        Error::HandleExists => StatusCode::CONFLICT,
        Error::ProbeExpired | Error::ProbeInvalid | Error::NotConnected => StatusCode::UNAUTHORIZED,
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
    match state.hub.list_agents(&gov) {
        Ok(agents) => {
            let participants_json: Vec<_> = agents
                .iter()
                .map(|a: &AgentInfo| json!({"name": a.name, "identity": a.identity, "status": a.status}))
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

// POST /governors/accept-transfer  — recipient claims authority using the transfer token
// The transfer credential is passed as `Authorization: Bearer <transfer_token>` (not in body)
// to prevent credential exposure in request body logs.
async fn handle_accept_governor_transfer(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let transfer_token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    let claiming_identity = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "MISSING_FIELD", "message": "missing name"})),
            )
                .into_response();
        }
    };
    match state
        .hub
        .accept_governor_transfer(&transfer_token, &claiming_identity)
    {
        Ok(new_token) => (StatusCode::OK, Json(json!({"token": new_token.0}))).into_response(),
        Err(e) => err_response(e),
    }
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
    let token = AgentToken(tok_str);
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
async fn handle_grant_request(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<GrantRequestBody>,
) -> Response {
    let tok_str = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
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

// \u2500\u2500 V2 request body types \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

// ── DCP request body types ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct IntroduceBody {
    handle: String,
    sub_id: String,
}

#[derive(Deserialize)]
struct ProbeAckBody {
    nonce: String,
    sub_id: String,
}

#[derive(Deserialize)]
struct LeaveBody {
    sub_id: String,
}

// DCP announce body — deserialized inline via serde_json::Value in handle_announce
// These structs are kept as documentation of the wire format:
#[allow(dead_code)]
#[derive(Deserialize, Default)]
struct DcpAnnounceBody {
    handle: String,
    #[serde(default)]
    force: bool,
    sub_id: String,
}

// ── V2 request body types ─────────────────────────────────────────────────────

// V2 announce body — deserialized inline via serde_json::Value in handle_announce
#[allow(dead_code)]
#[derive(Deserialize)]
struct AnnounceBody {
    name: String,
    /// If true and the name is held by a live session, evict the holder and
    /// claim the name. Use when recovering from a stale/orphaned session that
    /// still holds your name. Any valid listen token may force-reclaim.
    #[serde(default)]
    force: bool,
}

#[derive(Deserialize, Default)]
struct DequeueAllBody {
    thread: Option<String>,
}

// ── GET / — discovery JSON ─────────────────────────────────────────────────────

async fn handle_discovery() -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "service": "simple-im",
            "version": "2",
            "entry": "POST /agents/register",
            "description": "Register with POST /agents/register to receive a token, then POST /listen with that token to open your SSE stream. POST /announce to claim a name. See the participant skill for the full flow.",
            "skill": "/skills/participant",
            "auth": "Bearer <listen-token> in the Authorization header. Gate on HTTP status code; errors are {\"error\":CODE,\"message\":...}.",
            "agents_register": "POST /agents/register",
            "participant": {
                "register": "POST /agents/register",
                "listen": "POST /listen",
                "cancel_listen": "DELETE /listen",
                "announce": "POST /announce",
                "leave": "POST /leave",
                "send": "POST /messages/send",
                "dequeue": "POST /messages/queue/pop",
                "dequeue_alias": "POST /messages/dequeue",
                "dequeue_all": "DELETE /messages/queue",
                "pending": "GET /messages/pending",
                "latest": "GET /messages/latest",
                "latest_id": "GET /messages/latest/id",
                "presence": "GET /participants/{name}/presence",
                "presence_scope": "POST /participants/{name}/presence-scope",
                "participants": "GET /participants",
                "deregister": "DELETE /participants/{name}",
                "grant_request": "POST /grants/request",
                "grants": "GET /grants",
                "attach": "POST /attachments?to=<name>&filename=<f>  (raw body = file bytes, Content-Type = mime)",
                "attachment_fetch": "GET /attachments/{id}"
            },
            "dcp": {
                "note": "Advanced connect-by-handle handshake. The V2 listen+announce flow above is the default; use DCP only for handle-addressed direct connect.",
                "introduce": "POST /introduce",
                "connect_probe_ack": "POST /connect-probe-ack"
            },
            "governor": {
                "skill": "/skills/governor",
                "claim": "POST /governors/claim",
                "election_vote": "POST /governors/elections/{id}  (body {\"action\":\"approve|reject\"})",
                "refresh": "POST /governors/refresh",
                "transfer": "POST /governors/transfer",
                "accept_transfer": "POST /governors/accept-transfer",
                "mediate": "POST /governors/mediate",
                "events": "GET /governors/events",
                "approve_grant": "POST /grants/approve",
                "grant_request_action": "PATCH /grants/requests/{id}  (body: {\"action\":\"approve|deny|hold\"})",
                "list_grants": "GET /grants",
                "list_all_grants": "GET /governors/grants",
                "block": "POST /grants/block",
                "unblock": "POST /grants/unblock",
                "revoke_grant": "DELETE /grants/{id}"
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

// ── POST /agents/register ─────────────────────────────────────────────────────
// Mint a new agent token for future /listen use.
// No authentication required (anyone can register, same as anonymous /listen before).

async fn handle_agents_register(State(state): State<Arc<AppState>>) -> Response {
    let token = state.hub.register_agent();
    (StatusCode::OK, Json(json!({"token": token}))).into_response()
}

// ── POST /listen ──────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct ListenBody {
    name: Option<String>,
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

    let (token, rx) = match state.hub.open_listen(
        Some(&token),
        peer_ip,
        name_to_bind,
        observed_host,
        force,
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
// DCP: sub_token in Authorization header → dcp_cancel_sub_by_token
// V2 fallback: listen token in Authorization header → cancel_listen

async fn handle_cancel_listen(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    // Try DCP sub_token cancel first
    match state.hub.dcp_cancel_sub_by_token(&token) {
        Ok(()) => return StatusCode::NO_CONTENT.into_response(),
        Err(Error::TokenRejected) => {
            // Not a DCP sub_token — fall through to V2
        }
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "NOT_FOUND", "message": "no active subscription"})),
            )
                .into_response();
        }
    }
    // V2 fallback
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
// DCP: takes Authorization: Bearer <auth-token> + {handle, force, sub_id}
// Falls back to V2 if no sub_id is provided (backward compat).

async fn handle_announce(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };

    // DCP path: if sub_id is present in body, use DCP announce
    if let Some(sub_id) = body.get("sub_id").and_then(|v| v.as_str()) {
        let handle = match body.get("handle").and_then(|v| v.as_str()) {
            Some(h) => h,
            None => return err_response(Error::BadRequest),
        };
        let force = body.get("force").and_then(|v| v.as_bool()).unwrap_or(false);
        return match state.hub.dcp_announce(&token, handle, force, sub_id) {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(Error::NameInUse) => (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "NAME_IN_USE",
                    "message": "name is currently in use",
                    "resolution": "re-announce with force:true to supersede the live holder"
                })),
            )
                .into_response(),
            Err(e) => err_response(e),
        };
    }

    // V2 fallback: {name, force}
    let name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return err_response(Error::BadRequest),
    };
    let force = body.get("force").and_then(|v| v.as_bool()).unwrap_or(false);
    match state.hub.announce(&token, &name, force) {
        Ok(AnnounceResult::Bound) => StatusCode::NO_CONTENT.into_response(),
        Ok(AnnounceResult::NameInUse { resolution_stream }) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "NAME_IN_USE",
                "message": "name is currently in use",
                "resolution_stream": resolution_stream,
                "resolution": "re-announce with force:true to reclaim your own name"
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

// ── DCP handlers ─────────────────────────────────────────────────────────────

// POST /introduce  — mint a new identity (TOFU for dogfood phase)
async fn handle_introduce(
    State(state): State<Arc<AppState>>,
    Json(body): Json<IntroduceBody>,
) -> Response {
    match state.hub.dcp_introduce(&body.handle, &body.sub_id) {
        Ok(auth_token) => (
            StatusCode::OK,
            Json(json!({
                "auth_token": auth_token,
                "hint": "Save this auth_token — it is your permanent identity credential. Present it on future /listen calls."
            })),
        ).into_response(),
        Err(Error::HandleExists) => (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "HANDLE_EXISTS",
                "message": "handle already exists",
                "hint": "This handle already exists. If this is your account, reclaim it via POST /announce with your auth-token. Do not re-introduce."
            })),
        ).into_response(),
        Err(e) => err_response(e),
    }
}

// POST /connect-probe-ack  — agent acknowledges the nonce probe
async fn handle_probe_ack(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<ProbeAckBody>,
) -> Response {
    let auth_token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state
        .hub
        .dcp_probe_ack(&auth_token, &body.nonce, &body.sub_id)
    {
        Ok(()) => (StatusCode::OK, Json(json!({"status": "connected"}))).into_response(),
        Err(e) => err_response(e),
    }
}

// POST /leave  — agent disconnects while preserving identity
async fn handle_leave(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<LeaveBody>,
) -> Response {
    let auth_token = match bearer_token(&headers) {
        Some(t) => t,
        None => return auth_failed(),
    };
    match state.hub.dcp_leave(&auth_token, &body.sub_id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
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

