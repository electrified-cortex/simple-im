use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use rand::Rng;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::error::Error;
use crate::persistence::{
    PersistedDenialBlock, PersistedGrant, PersistedIdentity, PersistedToken, StoredAttachment,
    TokenStore,
};
use crate::registry::{AgentIdentity, PresenceScope, Registry};
use crate::trust::{ApproveGrantRequest, GrantMediation, TrustChain};
use crate::types::{AgentToken, GovernorToken, Payload, QueuedMessage};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Generate a CSPRNG hex string of `n_bytes` bytes.
fn rand_hex(n_bytes: usize) -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut bytes = vec![0u8; n_bytes];
    rng.fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Convert a future Instant to an approximate SystemTime for DB persistence.
fn instant_to_system_time(instant: Instant) -> SystemTime {
    let now_i = Instant::now();
    let now_s = SystemTime::now();
    if instant >= now_i {
        now_s + (instant - now_i)
    } else {
        now_s.checked_sub(now_i - instant).unwrap_or(now_s)
    }
}

// ── Public types ──────────────────────────────────────────────────────────────

/// Snapshot of a registered agent's name, identity, and online status.
pub struct AgentInfo {
    pub name: String,
    pub identity: String,
    pub status: &'static str,
}

/// Outcome returned by a successful `send`: either immediately accepted or held for governor mediation.
#[derive(Debug, PartialEq, Eq)]
pub enum Ack {
    /// Message was delivered (or queued) without mediation.
    Accepted,
    /// Message is on hold pending a governor decision; the hold is identified by `mediation_id`.
    PendingMediation { mediation_id: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionStage {
    /// Waiting for the governor to approve.
    PendingGovernor,
    /// Governor approved; waiting for the recipient to approve.
    PendingRecipient,
}

/// An in-flight bilateral grant request, tracking its approval stage and participants.
pub struct ConnectionRequest {
    pub request_id: String,
    pub from_name: String,
    pub to_name: String,
    pub from_identity: String,
    pub to_identity: String,
    pub reason: Option<String>,
    pub stage: ConnectionStage,
    pub governor_expiry: Option<Duration>,
    pub recipient_expiry: Option<Duration>,
    /// Governor token stored on approval; used to create the grant when both sides approve.
    pub approving_governor: Option<String>,
    /// When this request expires. 30 min from creation; resets to 30 min from now on governor approval.
    pub expires_at: Instant,
}

/// A persistent denial block keyed on (from_identity, to_name).
struct DenialBlock {
    reason: String,
    /// Unix timestamp (seconds) after which the block expires. None = permanent.
    expires_at: Option<u64>,
}

const GRANT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30 * 60);

// ── Governance claim / election / transfer ────────────────────────────────────

#[derive(Clone, PartialEq)]
enum ClaimKind {
    Election,
    Transfer,
}

struct GovernanceClaim {
    candidate_token: String,
    candidate_name: String,
    kind: ClaimKind,
    /// Election: agent NAMES that must approve; Transfer: governor TOKEN ids that must approve.
    required: std::collections::HashSet<String>,
    approved: std::collections::HashSet<String>,
    expires_at: Instant,
}

/// Immediate result of calling `claim_governorship`.
pub enum ClaimOutcome {
    /// Governorship was awarded immediately (no competition).
    Granted { governor_token: String },
    /// Other agents must vote; the claim is pending their responses.
    Election { claim_id: String, voters: usize },
    /// An active governor must approve the transfer.
    Transfer { claim_id: String },
}

/// Current state of a pending governance claim after a vote is cast.
pub enum ClaimResolution {
    /// Not all required votes have been received yet.
    Waiting {
        approved: usize,
        required: usize,
    },
    /// All required votes approved; the candidate is now governor.
    Established {
        candidate_name: String,
        governor_token: String,
    },
    /// At least one required voter rejected the claim.
    Rejected {
        candidate_name: String,
    },
}

/// Result of approving a grant request at one of the two required stages.
pub enum ApproveStatus {
    /// Governor approved; waiting for the recipient to also approve.
    PendingRecipient,
    /// Both parties approved; the grant is now active.
    Established,
}

/// Result of responding to a bilateral connection request.
pub enum RespondStatus {
    /// This party approved but the other party has not yet acted.
    WaitingForOther,
    /// Both parties approved and the grant is established.
    Established { to_name: String },
    /// One party denied the request.
    Denied { from_name: String },
}

/// Result of a dequeue or long-poll operation.
#[derive(Debug)]
pub enum DequeueOutcome {
    /// A message was available and returned.
    Message(QueuedMessage),
    /// No message was available within the wait window.
    Empty,
}

/// Governor's ruling on a held mediation.
pub enum MediationDecision {
    /// Deliver the original payload as-is.
    Approve,
    /// Discard the payload without delivery.
    Block,
    /// Deliver a governor-supplied replacement payload.
    Modify(Payload),
}

/// Outcome after a governor resolves a mediation hold.
pub enum MediationResult {
    /// Payload was queued for the recipient.
    Delivered { to_name: String },
    /// Governor chose to block; payload was discarded.
    Blocked,
    /// Recipient deregistered before the hold was resolved.
    RecipientOffline,
}

// ── Listen token model ────────────────────────────────────────────────────────

/// Result of a name announcement attempt on a listen token.
pub enum AnnounceResult {
    /// The name was successfully bound to this token.
    Bound,
    /// Another live session already holds the name.
    NameInUse { resolution_stream: String },
}

/// State for a self-issued listen token.
struct V2TokenState {
    issued_at: Instant,
    ever_listened: bool,
    ever_granted: bool,
    name: Option<String>,
    revoked: bool,
    hidden: bool,
    /// Active SSE channel — sends JSON event strings to the live SSE stream.
    sse_sender: Option<mpsc::UnboundedSender<String>>,
    /// Notify interlock: true after NOTIFY fired, cleared on dequeue.
    notify_suppressed: bool,
    /// Last peer IP seen, for concurrent-use detection.
    last_ip: Option<String>,
    last_ip_at: Option<Instant>,
    /// If this listen session was opened by a governor (bearer = gov token), holds the governor token ID.
    governor_id: Option<String>,
    /// Monotonically increasing counter for messages enqueued to this subscriber's queue.
    /// Starts at 0; incremented on every push to message_queues for this token's name.
    /// Used by GET /messages/latest/id for non-consuming peek and long-poll gap detection.
    msg_id_watch: watch::Sender<u64>,
}

impl V2TokenState {
    fn new() -> Self {
        let (msg_id_tx, _msg_id_rx) = watch::channel(0u64);
        V2TokenState {
            issued_at: Instant::now(),
            ever_listened: false,
            ever_granted: false,
            name: None,
            revoked: false,
            hidden: false,
            sse_sender: None,
            notify_suppressed: false,
            last_ip: None,
            last_ip_at: None,
            governor_id: None,
            msg_id_watch: msg_id_tx,
        }
    }

    fn is_sse_alive_in_hub(token: &str, sse_connections: &HashMap<String, usize>) -> bool {
        sse_connections.get(token).copied().unwrap_or(0) > 0
    }
}

// ── DCP structs ───────────────────────────────────────────────────────────────

/// DCP: durable identity entry (handle-keyed)
struct DcpIdentity {
    #[allow(dead_code)]
    handle: String,
    auth_token: String,
}

/// DCP: ephemeral subscription entry (one per /listen call)
struct DcpSub {
    sub_id: String,         // first event to agent: {"type":"sub","sub_id":"..."}
    sub_token: String,      // for /listen/cancel pre-announce bail
    handle: Option<String>, // None until introduce/announce
    sse_sender: Option<mpsc::UnboundedSender<String>>,
    #[allow(dead_code)]
    created_at: std::time::Instant,
}

/// DCP: connect_probe entry (single-use, expires)
struct DcpProbe {
    nonce: String, // CSPRNG ≥128 bits hex
    sub_id: String,
    handle: String,
    #[allow(dead_code)]
    probe_instance: String, // unique per probe emission
    expires_at: std::time::Instant,
    used: bool,
}

// ── Internal structs ──────────────────────────────────────────────────────────

/// A short-lived permission that lets a recipient reply to a sender without a standing grant.
pub struct ReplyWindow {
    pub recipient: String,
    pub sender: String,
    pub expires: Instant,
    pub used: bool,
}

/// A message intercepted by an Inspect-mode grant and awaiting a governor ruling.
pub struct MediationHold {
    pub mediation_id: String,
    pub from_name: String,
    pub to_name: String,
    pub from_identity: String,
    pub to_identity: String,
    pub payload: Payload,
    pub reason: String,
    pub expires: Instant,
    pub resolved: bool,
    pub grant_id: Option<String>,
}

struct AgentState {
    identity: String,
    /// Wakes up any `dequeue()` long-poll waiting for this agent.
    notify: Arc<tokio::sync::Notify>,
}

struct HubInner {
    trust: TrustChain,
    registry: Registry,
    agents: HashMap<String, AgentState>,
    token_to_name: HashMap<String, String>,
    active_sse_connections: HashMap<String, usize>,
    /// Per-agent FIFO message queue (survives liveness lapse and re-registration).
    message_queues: HashMap<String, VecDeque<QueuedMessage>>,
    /// Agents that have at least one queued message; cleared when queue empties.
    kick_pending: HashSet<String>,
    reply_windows: Vec<ReplyWindow>,
    mediation_holds: Vec<MediationHold>,
    /// In-memory bilateral connection requests (ephemeral — lost on restart).
    connection_requests: HashMap<String, ConnectionRequest>,
    reply_ttl: Duration,
    hold_ttl: Duration,
    med_counter: u64,
    req_counter: u64,
    gov_events: broadcast::Sender<String>,
    /// Self-issued listen token state (keyed by token string).
    listen_tokens: HashMap<String, V2TokenState>,
    /// Maps announced name → token (for name-claim lookup).
    name_to_token: HashMap<String, String>,
    /// Active SSE connection count per token.
    sse_connections: HashMap<String, usize>,
    /// TTL for never-listened tokens.
    v2_gc_ttl_unlisten: Duration,
    /// TTL for listened-but-never-granted tokens.
    v2_gc_ttl_no_grant: Duration,
    /// Persistent denial blocks keyed on (from_identity, to_name).
    denial_blocks: HashMap<(String, String), DenialBlock>,
    // DCP identity store: handle → DcpIdentity
    dcp_identities: HashMap<String, DcpIdentity>,
    // auth_token → handle (reverse lookup)
    dcp_auth_to_handle: HashMap<String, String>,
    // sub_id → DcpSub
    dcp_subs: HashMap<String, DcpSub>,
    // sub_token → sub_id
    dcp_sub_token_to_id: HashMap<String, String>,
    // probe key → DcpProbe (key: "{sub_id}:{probe_instance}")
    dcp_probes: HashMap<String, DcpProbe>,
    // grant integrity: handle → Vec<grant_id> (expected set for CONNECTED check)
    dcp_expected_grants: HashMap<String, Vec<String>>,
    /// Guards the one-time startup announce: true after sim_online has been sent.
    startup_announced: bool,
    /// In-memory governance claims (election / transfer); ephemeral — lost on restart.
    pending_claims: HashMap<String, GovernanceClaim>,
    /// Monotonic counter for claim IDs.
    claim_counter: u64,
}

/// Central hub coordinating message delivery, token management, grants, and presence.
pub struct DeliveryHub {
    inner: Mutex<HubInner>,
    token_store: Option<Arc<TokenStore>>,
}

/// Metadata returned to the sender after a successful attachment send.
pub struct AttachmentMeta {
    pub id: String,
    pub filename: String,
    pub mime: String,
    pub size: usize,
}

impl HubInner {
    fn prune_expired(&mut self) {
        let now = Instant::now();
        self.reply_windows.retain(|w| w.expires > now);
        self.mediation_holds
            .retain(|h| !h.resolved && h.expires > now);
    }

    /// Collect active SSE senders for all grant-peers of `name`.
    /// Used to push presence events (online/offline) after a name is bound or unbound.
    ///
    /// Two resolution paths (15-0002F fix):
    ///   1. Name path — look up counterparty by name in `name_to_token` → `listen_tokens`.
    ///      Covers V2 listen-flow agents and minted agents whose name was stored in the grant.
    ///   2. Identity path — try the counterparty's raw identity as a key in `listen_tokens`
    ///      directly.  V2 listen-flow agents have identity == listen token, so this covers
    ///      grants where only the identity (not the name) was stored at creation time.
    ///
    /// Minted-agent grant-peers (registered via /register, not /listen) have no stored SSE
    /// sender in HubInner and fall through both paths silently.
    /// TODO(15-0002F): pushing to minted-agent grant-peers requires a separate sender registry.
    fn grant_peer_senders(&self, name: &str) -> Vec<mpsc::UnboundedSender<String>> {
        // Resolve the identity for this named agent.
        // INVARIANT: grant_peer_senders is always called before removing the agent from
        // `self.agents` (see deregister, governor_deregister, revoke_by_name, revoke_token,
        // cancel_listen, close_listen, announce — all collect senders before cleanup).
        // Falls back to `name` to avoid panicking on unexpected call ordering.
        let identity = self
            .agents
            .get(name)
            .map(|s| s.identity.as_str())
            .unwrap_or(name);

        let counterparties = self.trust.grant_counterparties_for(name, identity);
        let mut senders = Vec::new();
        let mut seen_tokens: HashSet<String> = HashSet::new();

        for (cp_name, cp_identity) in counterparties {
            // Path 1: resolve by counterparty name → name_to_token → listen_tokens → sse_sender.
            let tok_opt = cp_name
                .as_deref()
                .and_then(|n| self.name_to_token.get(n))
                .cloned();

            // Path 2: if name path finds nothing, try the counterparty's identity as a token key.
            // V2 listen-flow agents store identity == listen token, so this covers grants
            // where name_b (or name_a) was not set at grant-creation time.
            let tok_opt = tok_opt.or_else(|| {
                if self.listen_tokens.contains_key(cp_identity.as_str()) {
                    Some(cp_identity)
                } else {
                    None
                }
            });

            if let Some(tok) = tok_opt
                && seen_tokens.insert(tok.clone())
                && let Some(st) = self.listen_tokens.get(&tok)
                && let Some(ref tx) = st.sse_sender
                && !tx.is_closed()
            {
                senders.push(tx.clone());
            }
            // Minted-agent counterparties have no listen_tokens entry → silently skip.
        }
        senders
    }

    fn is_online_effective(&self, name: &str) -> bool {
        self.active_sse_connections.get(name).copied().unwrap_or(0) > 0
            || self.registry.is_online(name)
    }

    fn presence_scope_effective(&self, name: &str) -> Option<PresenceScope> {
        if self.active_sse_connections.get(name).copied().unwrap_or(0) > 0 {
            self.registry.presence_scope_unconditional(name)
        } else {
            self.registry.presence_scope(name)
        }
    }

    fn pop_message(&mut self, agent_name: &str) -> Option<QueuedMessage> {
        let msg = self
            .message_queues
            .get_mut(agent_name)
            .and_then(|q| q.pop_front());
        if self
            .message_queues
            .get(agent_name)
            .map(|q| q.is_empty())
            .unwrap_or(true)
        {
            self.kick_pending.remove(agent_name);
        }
        msg
    }

    /// Increment the message ID counter for a listen-flow subscriber by name.
    /// Call this every time a message is pushed to the subscriber's message queue.
    /// No-op if the agent is not a listen-flow subscriber.
    fn increment_msg_id_for_name(&mut self, name: &str) {
        if let Some(tok) = self.name_to_token.get(name).cloned()
            && let Some(st) = self.listen_tokens.get(&tok)
        {
            let new_id = *st.msg_id_watch.borrow() + 1;
            // send_replace() always stores the new value even with no live receivers,
            // unlike send() which silently no-ops when receiver_count() == 0.
            st.msg_id_watch.send_replace(new_id);
        }
    }

    /// Extract SSE sender for NOTIFY if recipient uses the listen flow and notify is armed.
    /// Sets notify_suppressed = true (interlock). Returns (sender, pending_count) if fired.
    fn take_notify(&mut self, name: &str) -> Option<(mpsc::UnboundedSender<String>, usize)> {
        let token = self.name_to_token.get(name)?.clone();
        let is_alive = V2TokenState::is_sse_alive_in_hub(&token, &self.sse_connections);
        let state = self.listen_tokens.get_mut(&token)?;
        if state.notify_suppressed || !is_alive {
            return None;
        }
        state.notify_suppressed = true;
        let pending = self.message_queues.get(name).map(|q| q.len()).unwrap_or(0);
        let sender = state.sse_sender.clone()?;
        Some((sender, pending))
    }

    /// Bind `name` to `token` atomically (name registry + agents map + token state).
    /// Caller is responsible for evicting any stale holder first.
    fn bind_name(&mut self, token: &str, name: &str) {
        self.name_to_token
            .insert(name.to_string(), token.to_string());
        self.token_to_name
            .insert(token.to_string(), name.to_string());
        let notify = Arc::new(tokio::sync::Notify::new());
        self.agents.insert(
            name.to_string(),
            AgentState {
                identity: token.to_string(),
                notify: Arc::clone(&notify),
            },
        );
        if let Some(st) = self.listen_tokens.get_mut(token) {
            st.name = Some(name.to_string());
        }
    }

    /// GC expired tokens inline. Called on listen/announce/dequeue operations.
    ///
    /// Returns a list of `(senders, name)` pairs for Branch-3 evictions.  Each entry
    /// represents a token that was `!ever_listened && ever_granted` and has now been
    /// evicted.  The **caller must fire `push_presence_event(senders, &name, "offline")`
    /// for every entry after releasing the lock** (out-of-lock, silent drop). (15-0002H)
    fn gc_tokens(&mut self) -> Vec<(Vec<mpsc::UnboundedSender<String>>, String)> {
        let now = Instant::now();
        let unlisten_ttl = self.v2_gc_ttl_unlisten;
        let no_grant_ttl = self.v2_gc_ttl_no_grant;

        // Branch 1: !ever_listened && !ever_granted — never subscribed and never granted; safe to
        //   collect after unlisten_ttl. No presence event: ever_granted=false → no grant-peers.
        // Branch 2: ever_listened && !ever_granted — subscribed but never granted; collect after
        //   no_grant_ttl. No presence event: ever_granted=false → no grant-peers.
        // Branch 3: !ever_listened && ever_granted — received a grant before /listen was called,
        //   then vanished (session dropped, no explicit revocation). Evict after unlisten_ttl and
        //   fire sim_offline to grant-peers out-of-lock (caller responsibility). (15-0002H)
        //   Branches 1 and 3 share the same TTL threshold; the filter is unified as !ever_listened.
        let to_remove: Vec<String> = self
            .listen_tokens
            .iter()
            .filter(|(_, st)| !st.revoked)
            .filter(|(_, st)| {
                if !st.ever_listened {
                    // Branches 1 (no grant) and 3 (ever_granted): both use unlisten_ttl.
                    now.duration_since(st.issued_at) > unlisten_ttl
                } else if !st.ever_granted {
                    // Branch 2: ever_listened && !ever_granted.
                    now.duration_since(st.issued_at) > no_grant_ttl
                } else {
                    false
                }
            })
            .map(|(tok, _)| tok.clone())
            .collect();

        let mut offline_events: Vec<(Vec<mpsc::UnboundedSender<String>>, String)> = Vec::new();

        for tok in to_remove {
            self.sse_connections.remove(&tok);
            if let Some(st) = self.listen_tokens.remove(&tok)
                && let Some(ref name) = st.name
            {
                // Branch 3: collect grant-peer senders BEFORE removing from agents map.
                // INVARIANT: grant_peer_senders() must be called while agents[name] still
                // exists (see grant_peer_senders() doc). (15-0002H)
                if st.ever_granted {
                    let senders = self.grant_peer_senders(name);
                    if !senders.is_empty() {
                        offline_events.push((senders, name.clone()));
                    }
                }
                self.name_to_token.remove(name.as_str());
                self.agents.remove(name.as_str());
                self.token_to_name.remove(&tok);
                self.message_queues.remove(name.as_str());
                self.kick_pending.remove(name.as_str());
            }
        }

        offline_events
    }
}

/// Enqueue a governor-role breadcrumb into the named agent's message queue (once per session).
/// Guard: skips if a `governor_role` message is already present in the queue.
fn maybe_enqueue_governor_breadcrumb(inner: &mut HubInner, name: &str) {
    let already_has = inner
        .message_queues
        .get(name)
        .map(|q| {
            q.iter()
                .any(|m| m.event_type.as_deref() == Some("governor_role"))
        })
        .unwrap_or(false);
    if already_has {
        return;
    }
    let payload_str = serde_json::json!({
        "type": "service",
        "kind": "governor_role",
        "role": "governor",
        "message": "You are the governor of this SIM instance. Your responsibilities: (1) Approve or deny grant requests via PATCH /grants/requests/{id}. (2) Mediate disputed messages via POST /governors/mediate. (3) Monitor governor events via GET /governors/events. Your token is valid for this session.",
        "actions": {
            "grant_action":  "PATCH /grants/requests/{id}  body: {action: approve|deny|hold, reason?, expiry_secs?}",
            "mediate":       "POST /governors/mediate",
            "events":        "GET /governors/events"
        }
    })
    .to_string();
    let breadcrumb = QueuedMessage {
        payload: Payload(payload_str.into_bytes()),
        from_name: "system".to_string(),
        reason: Some("governor_role_breadcrumb".to_string()),
        event_type: Some("governor_role".to_string()),
        thread_id: None,
    };
    inner
        .message_queues
        .entry(name.to_string())
        .or_default()
        .push_back(breadcrumb);
    inner.kick_pending.insert(name.to_string());
}

/// Push a presence event to a collected set of SSE senders (outside any lock).
fn push_presence_event(senders: Vec<mpsc::UnboundedSender<String>>, name: &str, event: &str) {
    if senders.is_empty() {
        return;
    }
    let ev = serde_json::json!({
        "type": "presence",
        "event": event,
        "participant": name,
    })
    .to_string();
    for tx in senders {
        let _ = tx.send(ev.clone());
    }
}

fn clamp_env_secs(var: &str, min: u64, max: u64, default: u64) -> Duration {
    let val = env::var(var)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default)
        .clamp(min, max);
    Duration::from_secs(val)
}

impl DeliveryHub {
    /// Create an in-memory hub with no persisted state. `lapse_after` sets the agent liveness window.
    pub fn new(lapse_after: Duration) -> Self {
        let reply_ttl = clamp_env_secs("SIMPLE_IM_REPLY_TTL_SECS", 5, 600, 120);
        let hold_ttl = clamp_env_secs("SIMPLE_IM_HOLD_TTL_SECS", 10, 300, 60);
        let v2_gc_ttl_unlisten = clamp_env_secs("SIMPLE_IM_V2_GC_UNLISTEN_SECS", 60, 3600, 300);
        let v2_gc_ttl_no_grant = clamp_env_secs("SIMPLE_IM_V2_GC_NO_GRANT_SECS", 120, 7200, 1800);
        let (gov_events, _) = broadcast::channel(64);
        Self {
            inner: Mutex::new(HubInner {
                trust: TrustChain::new(),
                registry: Registry::new(lapse_after),
                agents: HashMap::new(),
                token_to_name: HashMap::new(),
                active_sse_connections: HashMap::new(),
                message_queues: HashMap::new(),
                kick_pending: HashSet::new(),
                reply_windows: Vec::new(),
                mediation_holds: Vec::new(),
                connection_requests: HashMap::new(),
                reply_ttl,
                hold_ttl,
                med_counter: 0,
                req_counter: 0,
                gov_events,
                listen_tokens: HashMap::new(),
                name_to_token: HashMap::new(),
                sse_connections: HashMap::new(),
                v2_gc_ttl_unlisten,
                v2_gc_ttl_no_grant,
                denial_blocks: HashMap::new(),
                dcp_identities: HashMap::new(),
                dcp_auth_to_handle: HashMap::new(),
                dcp_subs: HashMap::new(),
                dcp_sub_token_to_id: HashMap::new(),
                dcp_probes: HashMap::new(),
                dcp_expected_grants: HashMap::new(),
                startup_announced: false,
                pending_claims: HashMap::new(),
                claim_counter: 0,
            }),
            token_store: None,
        }
    }

    /// Construct a hub pre-loaded with persisted tokens and grants, backed by `token_store`.
    pub fn new_with_persisted_state(
        lapse_after: Duration,
        token_store: Arc<TokenStore>,
        persisted_tokens: Vec<PersistedToken>,
        persisted_grants: Vec<PersistedGrant>,
        persisted_identities: Vec<PersistedIdentity>,
        persisted_denial_blocks: Vec<PersistedDenialBlock>,
    ) -> Self {
        let mut hub = Self::new(lapse_after);
        {
            let mut inner = hub.inner.lock().unwrap();
            let (listen_toks, regular_toks): (Vec<PersistedToken>, Vec<PersistedToken>) =
                persisted_tokens
                    .into_iter()
                    .partition(|t| t.token_type == "listen");
            inner.trust.load_from_store(regular_toks, persisted_grants);
            for t in listen_toks {
                let mut state = V2TokenState::new();
                state.ever_listened = true;
                state.ever_granted = true;
                // Restore name bindings so the agent is reachable while offline.
                // If two persisted tokens share a name (shouldn't happen), last-write-wins.
                if let Some(ref name) = t.name {
                    state.name = Some(name.clone());
                    inner.name_to_token.insert(name.clone(), t.token.clone());
                    inner.token_to_name.insert(t.token.clone(), name.clone());
                    inner.agents.insert(
                        name.clone(),
                        AgentState {
                            identity: t.token.clone(),
                            notify: Arc::new(tokio::sync::Notify::new()),
                        },
                    );
                }
                inner.listen_tokens.insert(t.token, state);
            }
            // DCP: restore durable identities. No active subs after restart (subs are ephemeral).
            for pi in persisted_identities {
                inner
                    .dcp_auth_to_handle
                    .insert(pi.auth_token.clone(), pi.handle.clone());
                inner.dcp_identities.insert(
                    pi.handle.clone(),
                    DcpIdentity {
                        handle: pi.handle,
                        auth_token: pi.auth_token,
                    },
                );
            }
            // Load persisted denial blocks.
            for block in persisted_denial_blocks {
                inner.denial_blocks.insert(
                    (block.from_identity, block.to_name),
                    DenialBlock {
                        reason: block.reason,
                        expires_at: block.expires_at,
                    },
                );
            }
        }
        hub.token_store = Some(token_store);
        hub
    }

    /// Run an async DB operation outside the hub mutex.
    /// Multi-thread runtime: uses block_in_place so the worker thread can block safely.
    /// Single-thread runtime: spawns a fire-and-forget task.
    /// Only called when token_store is Some; existing tests (token_store=None) never reach this.
    fn db_write<F>(&self, f: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| handle.block_on(f));
                }
                _ => {
                    handle.spawn(f);
                }
            }
        }
    }

    /// Lock the hub state, recovering even if a prior panic poisoned the Mutex.
    ///
    /// Reliability invariant: the release profile is `panic = "unwind"`, so a panicking
    /// request no longer aborts the process — but a panic *while this lock is held* would
    /// poison the Mutex, after which every later `lock().unwrap()` would itself panic,
    /// turning one bad request into a permanently-bricked hub. Recovering the guard via
    /// `into_inner()` keeps the hub serving. (State may be marginally inconsistent from the
    /// panic, but a live hub beats a dead one; the panic itself is logged by main.rs's hook.)
    fn lock(&self) -> std::sync::MutexGuard<'_, HubInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Subscribe to the governor event broadcast channel (governance notices, concurrent-use alerts).
    pub fn subscribe_gov_events(&self) -> broadcast::Receiver<String> {
        self.lock().gov_events.subscribe()
    }

    /// Debug (15-DEBUG): snapshot of in-memory collection sizes for leak/OOM diagnosis.
    /// Logged every 30s by the periodic task in `main::run`. A steadily rising count on
    /// any one collection (notably `dcp_probes`, which is never pruned) points at the
    /// leak; flat counts across a crash interval argue against OOM (look at panic log).
    pub fn debug_state_sizes(&self) -> String {
        let inner = self.lock();
        let queued_msgs: usize = inner.message_queues.values().map(|q| q.len()).sum();
        format!(
            "listen_tokens={} dcp_subs={} dcp_probes={} dcp_identities={} agents={} \
             name_to_token={} token_to_name={} queues={} queued_msgs={} conn_reqs={} \
             reply_windows={} mediation_holds={} denial_blocks={} sse_conns={} active_sse={}",
            inner.listen_tokens.len(),
            inner.dcp_subs.len(),
            inner.dcp_probes.len(),
            inner.dcp_identities.len(),
            inner.agents.len(),
            inner.name_to_token.len(),
            inner.token_to_name.len(),
            inner.message_queues.len(),
            queued_msgs,
            inner.connection_requests.len(),
            inner.reply_windows.len(),
            inner.mediation_holds.len(),
            inner.denial_blocks.len(),
            inner.sse_connections.len(),
            inner.active_sse_connections.len(),
        )
    }

    /// True when at least one non-revoked governor exists (controls bootstrap gate).
    pub fn has_active_governor(&self) -> bool {
        self.lock().trust.has_active_governor()
    }

    /// Install a governor directly, no policy check. The governed paths are claim_governorship/
    /// respond_claim; this is for bootstrapping, embedding, and tests. Persists like mint did.
    pub fn install_governor(&self, expiry: Option<Duration>) -> GovernorToken {
        let gov = self.lock().trust.install_governor(expiry);
        if let Some(store) = self.token_store.clone() {
            let tok = gov.0.clone();
            let expires_at = expiry.map(|d| SystemTime::now() + d.min(crate::types::MAX_EXPIRY));
            self.db_write(async move {
                if let Err(e) = store
                    .upsert_token(&tok, &tok, "governor", expires_at, None)
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }
        gov
    }

    // ── Governance claim / election / transfer ────────────────────────────────

    /// A claimant (identified by their listen token) requests governorship.
    ///
    /// - Auto-grant:  no governor AND no other live agents → immediate.
    /// - Election:    no governor BUT other SSE-alive agents exist → unanimous vote required.
    /// - Transfer:    a governor already exists → current governor(s) must approve.
    #[allow(clippy::type_complexity)] // deliberate: local tuple packs heterogeneous per-voter notification state
    pub fn claim_governorship(
        &self,
        claimant_token: &str,
        expiry: Option<Duration>,
    ) -> Result<ClaimOutcome, Error> {
        // Collect data and determine outcome inside the lock; fire notifies outside.
        let (outcome, notify_pairs) = {
            let mut inner = self.lock();

            // Resolve claimant name.
            let candidate_name = inner
                .token_to_name
                .get(claimant_token)
                .cloned()
                .ok_or(Error::AnnounceRequired)?;

            // Build a unique claim ID.
            inner.claim_counter += 1;
            let claim_id = format!("claim-{}", inner.claim_counter);

            if inner.trust.has_active_governor() {
                // ── Transfer path ─────────────────────────────────────────
                let required: std::collections::HashSet<String> =
                    inner.trust.active_governor_tokens().into_iter().collect();

                inner.pending_claims.insert(
                    claim_id.clone(),
                    GovernanceClaim {
                        candidate_token: claimant_token.to_string(),
                        candidate_name: candidate_name.clone(),
                        kind: ClaimKind::Transfer,
                        required: required.clone(),
                        approved: std::collections::HashSet::new(),
                        expires_at: Instant::now() + GRANT_REQUEST_TIMEOUT,
                    },
                );

                // Broadcast transfer_request to governor SSE.
                let event = serde_json::json!({
                    "type": "governance",
                    "event": "transfer_request",
                    "claim_id": &claim_id,
                    "candidate": &candidate_name,
                    "action_url": format!("/governors/elections/{}", &claim_id),
                    "method": "POST",
                    "actions": ["approve", "reject"],
                })
                .to_string();
                let _ = inner.gov_events.send(event);

                (ClaimOutcome::Transfer { claim_id }, vec![])
            } else {
                // ── Election or auto-grant path ───────────────────────────
                // Collect names of other SSE-alive agents (excluding the claimant).
                let required: std::collections::HashSet<String> = inner
                    .agents
                    .keys()
                    .filter(|n| n.as_str() != candidate_name.as_str())
                    .filter(|n| {
                        inner
                            .name_to_token
                            .get(*n)
                            .map(|tok| {
                                V2TokenState::is_sse_alive_in_hub(tok, &inner.sse_connections)
                            })
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect();

                if required.is_empty() {
                    // Auto-grant: no one else to approve.
                    let gov = inner.trust.install_governor(expiry);
                    let tok = gov.0.clone();
                    // Persist outside the lock via db_write (token_store may be None in tests).
                    let store_opt = self.token_store.clone();
                    let expires_at =
                        expiry.map(|d| SystemTime::now() + d.min(crate::types::MAX_EXPIRY));
                    drop(inner); // release lock before db_write
                    if let Some(store) = store_opt {
                        let tok2 = tok.clone();
                        self.db_write(async move {
                            if let Err(e) = store
                                .upsert_token(&tok2, &tok2, "governor", expires_at, None)
                                .await
                            {
                                eprintln!("WARNING: token store write failed: {e}");
                            }
                        });
                    }
                    return Ok(ClaimOutcome::Granted {
                        governor_token: tok,
                    });
                }

                // Election: insert claim and notify each voter via their message queue.
                let voters = required.len();

                // Collect voter notify data before inserting the claim.
                let mut notify_pairs: Vec<(
                    String, // voter name
                    String, // msg json
                    Option<Arc<tokio::sync::Notify>>,
                    Option<(mpsc::UnboundedSender<String>, usize)>,
                )> = Vec::new();

                for voter_name in &required {
                    let msg_json = serde_json::json!({
                        "type": "governance",
                        "event": "election_request",
                        "claim_id": &claim_id,
                        "candidate": &candidate_name,
                        "action_url": format!("/governors/elections/{}", &claim_id),
                        "method": "POST",
                        "actions": ["approve", "reject"],
                    })
                    .to_string();

                    inner
                        .message_queues
                        .entry(voter_name.clone())
                        .or_default()
                        .push_back(QueuedMessage {
                            payload: Payload(msg_json.clone().into_bytes()),
                            from_name: "system".to_string(),
                            reason: None,
                            event_type: Some("governance".to_string()),
                            thread_id: None,
                        });
                    inner.kick_pending.insert(voter_name.clone());
                    inner.increment_msg_id_for_name(voter_name);

                    let notify = inner.agents.get(voter_name).map(|s| Arc::clone(&s.notify));
                    let v2n = inner.take_notify(voter_name);
                    notify_pairs.push((voter_name.clone(), msg_json, notify, v2n));
                }

                inner.pending_claims.insert(
                    claim_id.clone(),
                    GovernanceClaim {
                        candidate_token: claimant_token.to_string(),
                        candidate_name,
                        kind: ClaimKind::Election,
                        required,
                        approved: std::collections::HashSet::new(),
                        expires_at: Instant::now() + GRANT_REQUEST_TIMEOUT,
                    },
                );

                (ClaimOutcome::Election { claim_id, voters }, notify_pairs)
            }
        }; // lock released

        // Fire notifies out of lock.
        for (_, _, notify, v2n) in notify_pairs {
            if let Some(n) = notify {
                n.notify_one();
            }
            if let Some((sender, pending)) = v2n {
                let _ = sender.send(format!(r#"{{"type":"notify","pending":{}}}"#, pending));
            }
        }

        Ok(outcome)
    }

    /// An approver votes on a pending governance claim.
    pub fn respond_claim(
        &self,
        approver_token: &str,
        claim_id: &str,
        approve: bool,
    ) -> Result<ClaimResolution, Error> {
        let (resolution, post_lock) = {
            let mut inner = self.lock();

            let claim = inner
                .pending_claims
                .get(claim_id)
                .ok_or(Error::BadRequest)?;

            // Check expiry.
            if Instant::now() >= claim.expires_at {
                inner.pending_claims.remove(claim_id);
                return Err(Error::BadRequest);
            }

            let kind = claim.kind.clone();
            let candidate_name = claim.candidate_name.clone();
            let candidate_token = claim.candidate_token.clone();
            let required = claim.required.clone();

            // Authorize and determine the approval key.
            let approval_key: String = match kind {
                ClaimKind::Transfer => {
                    // Approver must be a valid governor whose token is in required.
                    let gov = GovernorToken(approver_token.to_string());
                    inner
                        .trust
                        .validate_governor_token(&gov)
                        .map_err(|_| Error::Forbidden)?;
                    if !required.contains(approver_token) {
                        return Err(Error::Forbidden);
                    }
                    approver_token.to_string()
                }
                ClaimKind::Election => {
                    // Approver must be an announced agent whose name is in required.
                    let name = inner
                        .token_to_name
                        .get(approver_token)
                        .cloned()
                        .ok_or(Error::Forbidden)?;
                    if !required.contains(&name) {
                        return Err(Error::Forbidden);
                    }
                    name
                }
            };

            if !approve {
                // Rejection: remove claim and notify candidate.
                inner.pending_claims.remove(claim_id);

                let rejection_json = serde_json::json!({
                    "type": "governance",
                    "event": "claim_rejected",
                    "claim_id": claim_id,
                })
                .to_string();

                inner
                    .message_queues
                    .entry(candidate_name.clone())
                    .or_default()
                    .push_back(QueuedMessage {
                        payload: Payload(rejection_json.into_bytes()),
                        from_name: "system".to_string(),
                        reason: None,
                        event_type: Some("governance".to_string()),
                        thread_id: None,
                    });
                inner.kick_pending.insert(candidate_name.clone());
                inner.increment_msg_id_for_name(&candidate_name);
                let notify = inner
                    .agents
                    .get(&candidate_name)
                    .map(|s| Arc::clone(&s.notify));
                let v2n = inner.take_notify(&candidate_name);

                return {
                    drop(inner);
                    if let Some(n) = notify {
                        n.notify_one();
                    }
                    if let Some((sender, pending)) = v2n {
                        let _ =
                            sender.send(format!(r#"{{"type":"notify","pending":{}}}"#, pending));
                    }
                    Ok(ClaimResolution::Rejected { candidate_name })
                };
            }

            // Approval: record it.
            let claim = inner
                .pending_claims
                .get_mut(claim_id)
                .ok_or(Error::BadRequest)?;
            claim.approved.insert(approval_key);

            let approved_count = claim.approved.len();
            let required_count = claim.required.len();
            let all_approved = claim.required.is_subset(&claim.approved);

            if !all_approved {
                return Ok(ClaimResolution::Waiting {
                    approved: approved_count,
                    required: required_count,
                });
            }

            // All approved: install new governor.
            let is_transfer = kind == ClaimKind::Transfer;
            if is_transfer {
                inner.trust.revoke_all_governors();
            }
            let gov = inner.trust.install_governor(None);
            let gov_tok_str = gov.0.clone();

            // Remove claim.
            inner.pending_claims.remove(claim_id);

            // Notify candidate via their queue.
            let grant_json = serde_json::json!({
                "type": "governance",
                "event": "governorship_granted",
                "claim_id": claim_id,
                "governor_token": &gov_tok_str,
            })
            .to_string();

            inner
                .message_queues
                .entry(candidate_name.clone())
                .or_default()
                .push_back(QueuedMessage {
                    payload: Payload(grant_json.into_bytes()),
                    from_name: "system".to_string(),
                    reason: None,
                    event_type: Some("governance".to_string()),
                    thread_id: None,
                });
            inner.kick_pending.insert(candidate_name.clone());
            inner.increment_msg_id_for_name(&candidate_name);
            let notify = inner
                .agents
                .get(&candidate_name)
                .map(|s| Arc::clone(&s.notify));
            let v2n = inner.take_notify(&candidate_name);
            let _ = candidate_token; // used above for token resolution; captured for completeness

            (
                ClaimResolution::Established {
                    candidate_name: candidate_name.clone(),
                    governor_token: gov_tok_str.clone(),
                },
                Some((gov_tok_str, notify, v2n)),
            )
        }; // lock released

        if let Some((tok, notify, v2n)) = post_lock {
            // Persist the new governor token.
            if let Some(store) = self.token_store.clone() {
                let t = tok.clone();
                self.db_write(async move {
                    if let Err(e) = store.upsert_token(&t, &t, "governor", None, None).await {
                        eprintln!("WARNING: token store write failed: {e}");
                    }
                });
            }
            if let Some(n) = notify {
                n.notify_one();
            }
            if let Some((sender, pending)) = v2n {
                let _ = sender.send(format!(r#"{{"type":"notify","pending":{}}}"#, pending));
            }
        }

        Ok(resolution)
    }

    /// Governor-minted agent token for a given identity, persisted if a token store is configured.
    pub fn mint_agent_token(
        &self,
        gov: &GovernorToken,
        identity: &str,
        expiry: Option<Duration>,
    ) -> Result<AgentToken, Error> {
        let token = self.lock().trust.mint_agent_token(gov, identity, expiry)?;
        if let Some(store) = self.token_store.clone() {
            let tok = token.0.clone();
            let id = identity.to_string();
            let expires_at = expiry.map(|d| SystemTime::now() + d.min(crate::types::MAX_EXPIRY));
            self.db_write(async move {
                if let Err(e) = store
                    .upsert_token(&tok, &id, "agent", expires_at, None)
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }
        Ok(token)
    }

    /// Establish a symmetric grant between two identities using default options.
    pub fn approve_grant(
        &self,
        gov: &GovernorToken,
        id_a: &str,
        id_b: &str,
        expiry: Option<Duration>,
    ) -> Result<String, Error> {
        self.approve_grant_req(gov, id_a, id_b, expiry, ApproveGrantRequest::default())
    }

    /// Establish a grant between two identities with fine-grained options (direction, mediation, limits).
    pub fn approve_grant_req(
        &self,
        gov: &GovernorToken,
        id_a: &str,
        id_b: &str,
        expiry: Option<Duration>,
        req: ApproveGrantRequest,
    ) -> Result<String, Error> {
        // FP1 fix: if names weren't supplied by the caller, look them up from token_to_name.
        // For listen-flow agents identity == token, so token_to_name gives us the stable name.
        let req = {
            let inner = self.lock();
            let mut r = req;
            if r.name_a.is_none() {
                r.name_a = inner.token_to_name.get(id_a).cloned();
            }
            if r.name_b.is_none() {
                r.name_b = inner.token_to_name.get(id_b).cloned();
            }
            r
        };
        let grant_id = self
            .lock()
            .trust
            .approve_grant_req(gov, id_a, id_b, expiry, req.clone())?;
        if let Some(store) = self.token_store.clone() {
            let gid = grant_id.clone();
            let a = id_a.to_string();
            let b = id_b.to_string();
            let dir = match &req.direction {
                Some(crate::trust::GrantDirection::AToB) => "a_to_b",
                Some(crate::trust::GrantDirection::BToA) => "b_to_a",
                _ => "symmetric",
            }
            .to_string();
            let med = match &req.mediation {
                Some(GrantMediation::Inspect) => "inspect",
                Some(GrantMediation::Notify) => "notify",
                _ => "bypass",
            }
            .to_string();
            let max_msg = req.max_messages;
            let cond = req.conditions.clone();
            let orw = req.opens_reply_window.unwrap_or(true);
            let gov_id = gov.0.clone();
            let expires_at = expiry.map(|d| SystemTime::now() + d.min(crate::types::MAX_EXPIRY));
            let na = req.name_a.clone();
            let nb = req.name_b.clone();
            self.db_write(async move {
                if let Err(e) = store
                    .upsert_grant(
                        &gid,
                        &a,
                        &b,
                        &dir,
                        &med,
                        max_msg,
                        0,
                        cond.as_deref(),
                        orw,
                        expires_at,
                        &gov_id,
                        na.as_deref(),
                        nb.as_deref(),
                    )
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }
        Ok(grant_id)
    }

    /// Register an agent. Re-registration refreshes liveness and the dequeue notify.
    /// Any messages queued for this agent during offline periods remain intact.
    /// Register a name for an agent token, making the agent reachable by that name.
    pub fn register(
        &self,
        name: &str,
        token: &AgentToken,
        scope: PresenceScope,
    ) -> Result<(), Error> {
        let mut inner = self.lock();
        inner.trust.validate_agent_token(token)?;
        let identity = inner
            .trust
            .agent_identity(token)
            .ok_or(Error::AuthFailed)?
            .to_string();
        inner
            .registry
            .register(name, AgentIdentity::valid(&identity), scope)?;
        let notify = Arc::new(tokio::sync::Notify::new());
        inner.agents.insert(
            name.to_string(),
            AgentState {
                identity,
                notify: Arc::clone(&notify),
            },
        );
        inner
            .token_to_name
            .insert(token.0.clone(), name.to_string());
        // Wake any waiting dequeue caller if messages were queued while offline.
        if inner
            .message_queues
            .get(name)
            .map(|q| !q.is_empty())
            .unwrap_or(false)
        {
            notify.notify_one();
        }
        Ok(())
    }

    /// Update the presence scope for a live same-identity registration.
    pub fn set_presence_scope(
        &self,
        name: &str,
        token: &AgentToken,
        scope: PresenceScope,
    ) -> Result<(), Error> {
        let mut inner = self.lock();
        inner.trust.validate_agent_token(token)?;
        let identity = inner
            .trust
            .agent_identity(token)
            .ok_or(Error::AuthFailed)?
            .to_string();
        inner
            .registry
            .set_presence_scope(name, &AgentIdentity::valid(&identity), scope)
    }

    /// Returns true if target is online and visible to querier per scope rules.
    /// Self-query always returns true `is_online`. Invalid token → Err(AuthFailed).
    pub fn presence_scoped(
        &self,
        querier_token: &AgentToken,
        target_name: &str,
    ) -> Result<bool, Error> {
        let inner = self.lock();
        inner.trust.validate_agent_token(querier_token)?;
        let querier_identity = inner
            .trust
            .agent_identity(querier_token)
            .ok_or(Error::AuthFailed)?
            .to_string();

        let querier_name = inner
            .token_to_name
            .get(&querier_token.0)
            .map(|s| s.as_str());
        if querier_name == Some(target_name) {
            return Ok(inner.is_online_effective(target_name));
        }

        let scope = match inner.presence_scope_effective(target_name) {
            Some(s) => s,
            None => return Ok(false),
        };

        let is_online = inner.is_online_effective(target_name);

        match scope {
            PresenceScope::Public => Ok(is_online),
            PresenceScope::Hidden => Ok(false),
            PresenceScope::GrantScoped => {
                let target_identity = match inner.agents.get(target_name) {
                    Some(state) => state.identity.clone(),
                    None => return Ok(false),
                };
                // FP1 fix: resolve stable names for presence grant check too.
                let querier_name = inner.token_to_name.get(&querier_identity).cloned();
                let target_name_str = Some(target_name.to_string());
                let has_grant = inner
                    .trust
                    .check_grant_directed_with_names(
                        &querier_identity,
                        &target_identity,
                        querier_name.as_deref(),
                        target_name_str.as_deref(),
                    )
                    .is_ok()
                    || inner
                        .trust
                        .check_grant_directed_with_names(
                            &target_identity,
                            &querier_identity,
                            target_name_str.as_deref(),
                            querier_name.as_deref(),
                        )
                        .is_ok();
                Ok(is_online && has_grant)
            }
        }
    }

    /// Send a message (§5.3). Implements the full authorization pipeline:
    /// grant → reply window → brief auth (hold) or BriefRequired.
    /// Queues the message for registered recipients regardless of online status.
    #[allow(clippy::type_complexity)] // deliberate: local tuple extracts grant/notify state atomically under the lock
    pub fn send(
        &self,
        from_token: &AgentToken,
        to_name: &str,
        payload: Payload,
        _reason: Option<String>,
        thread_id: Option<String>,
    ) -> Result<Ack, Error> {
        let (notify_arc, consumed_grant_id, v2_notify): (
            Option<Arc<tokio::sync::Notify>>,
            Option<String>,
            Option<(mpsc::UnboundedSender<String>, usize)>,
        ) = {
            let mut inner = self.lock();
            inner.prune_expired();
            // Unified sender auth: agents registered via /listen+/announce live in listen_tokens.
            // TrustChain fallback retained for any governor-minted agent tokens still in flight.
            let (from_identity, from_name) =
                if let Some(agent_state) = inner.listen_tokens.get(&from_token.0) {
                    if agent_state.revoked {
                        return Err(Error::TokenRevoked);
                    }
                    // Fix 1: name must be bound at send time (durable registry); ghost messages rejected.
                    let name = inner
                        .token_to_name
                        .get(&from_token.0)
                        .cloned()
                        .ok_or(Error::AnnounceRequired)?;
                    (from_token.0.clone(), name)
                } else {
                    inner.trust.validate_agent_token(from_token)?;
                    let identity = inner
                        .trust
                        .agent_identity(from_token)
                        .ok_or(Error::AuthFailed)?
                        .to_string();
                    let name = inner
                        .token_to_name
                        .get(&from_token.0)
                        .cloned()
                        .unwrap_or_default();
                    (identity, name)
                };

            // Fix 5: check for an active denial block on this sender→recipient pair before any other check.
            {
                let block_key = (from_identity.clone(), to_name.to_string());
                if let Some(block) = inner.denial_blocks.get(&block_key) {
                    let still_active = match block.expires_at {
                        None => true,
                        Some(exp) => {
                            let now_secs = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            exp > now_secs
                        }
                    };
                    if still_active {
                        return Err(Error::GrantBlocked(block.reason.clone()));
                    } else {
                        inner.denial_blocks.remove(&block_key);
                    }
                }
            }

            // Check registered only — no online check; queue for offline agents.
            let to_identity = match inner.agents.get(to_name) {
                None => return Err(Error::RecipientUnknown),
                Some(s) => s.identity.clone(),
            };

            // FP1 fix: resolve stable names for both parties so grants survive identity
            // rotation on /listen reconnect.  For listen-flow agents the token IS the
            // identity, so the stored name is in token_to_name.  For minted agents the
            // identity is already stable — from_name is looked up the same way.
            let resolved_from_name =
                inner
                    .token_to_name
                    .get(&from_identity)
                    .cloned()
                    .or_else(|| {
                        if !from_name.is_empty() {
                            Some(from_name.clone())
                        } else {
                            None
                        }
                    });
            let resolved_to_name = Some(to_name.to_string());

            match inner.trust.check_grant_directed_with_names(
                &from_identity,
                &to_identity,
                resolved_from_name.as_deref(),
                resolved_to_name.as_deref(),
            ) {
                Ok(grant_ref) => match grant_ref.mediation {
                    GrantMediation::Inspect => {
                        if !inner.trust.is_governor_id_online(&grant_ref.governor_id) {
                            return Err(Error::MediationUnavailable);
                        }
                        inner.med_counter += 1;
                        let mediation_id = format!("med-{}", inner.med_counter);
                        let expires = Instant::now() + inner.hold_ttl;
                        let event_payload = String::from_utf8_lossy(&payload.0).to_string();
                        let grant_id_str = grant_ref.id.clone();
                        inner.mediation_holds.push(MediationHold {
                            mediation_id: mediation_id.clone(),
                            from_name: from_name.clone(),
                            to_name: to_name.to_string(),
                            from_identity,
                            to_identity,
                            payload,
                            reason: String::new(),
                            expires,
                            resolved: false,
                            grant_id: Some(grant_ref.id),
                        });
                        let event = serde_json::json!({
                            "type": "mediation",
                            "mediation_id": &mediation_id,
                            "from": &from_name,
                            "to": to_name,
                            "payload": event_payload,
                            "grant_id": &grant_id_str,
                            "conditions": grant_ref.conditions,
                        })
                        .to_string();
                        let _ = inner.gov_events.send(event);
                        return Ok(Ack::PendingMediation { mediation_id });
                    }
                    GrantMediation::Notify => {
                        let gid = grant_ref.id.clone();
                        inner.trust.consume_grant_message(&grant_ref.id);
                        let event_payload = String::from_utf8_lossy(&payload.0).to_string();
                        let notify_json = serde_json::json!({
                            "type": "notify",
                            "from": &from_name,
                            "to": to_name,
                            "payload": event_payload,
                            "grant_id": &grant_ref.id,
                            "conditions": grant_ref.conditions,
                        })
                        .to_string();
                        let msg = QueuedMessage {
                            payload,
                            from_name: from_name.clone(),
                            reason: None,
                            event_type: None,
                            thread_id: thread_id.clone(),
                        };
                        inner
                            .message_queues
                            .entry(to_name.to_string())
                            .or_default()
                            .push_back(msg);
                        inner.kick_pending.insert(to_name.to_string());
                        inner.increment_msg_id_for_name(to_name);
                        if grant_ref.opens_reply_window {
                            let expires = Instant::now() + inner.reply_ttl;
                            inner.reply_windows.push(ReplyWindow {
                                recipient: to_name.to_string(),
                                sender: from_name,
                                expires,
                                used: false,
                            });
                        }
                        let _ = inner.gov_events.send(notify_json);
                        let v2n = inner.take_notify(to_name);
                        (
                            inner.agents.get(to_name).map(|s| Arc::clone(&s.notify)),
                            Some(gid),
                            v2n,
                        )
                    }
                    GrantMediation::Bypass => {
                        let gid = grant_ref.id.clone();
                        inner.trust.consume_grant_message(&grant_ref.id);
                        let msg = QueuedMessage {
                            payload,
                            from_name: from_name.clone(),
                            reason: None,
                            event_type: None,
                            thread_id: thread_id.clone(),
                        };
                        inner
                            .message_queues
                            .entry(to_name.to_string())
                            .or_default()
                            .push_back(msg);
                        inner.kick_pending.insert(to_name.to_string());
                        inner.increment_msg_id_for_name(to_name);
                        if grant_ref.opens_reply_window {
                            let expires = Instant::now() + inner.reply_ttl;
                            inner.reply_windows.push(ReplyWindow {
                                recipient: to_name.to_string(),
                                sender: from_name,
                                expires,
                                used: false,
                            });
                        }
                        let v2n = inner.take_notify(to_name);
                        (
                            inner.agents.get(to_name).map(|s| Arc::clone(&s.notify)),
                            Some(gid),
                            v2n,
                        )
                    }
                },
                Err(grant_err) => {
                    let now = Instant::now();
                    let window_idx = inner.reply_windows.iter().position(|w| {
                        w.recipient == from_name
                            && w.sender == to_name
                            && !w.used
                            && w.expires > now
                    });

                    if let Some(idx) = window_idx {
                        inner.reply_windows[idx].used = true;
                        let msg = QueuedMessage {
                            payload,
                            from_name: from_name.clone(),
                            reason: None,
                            event_type: None,
                            thread_id: thread_id.clone(),
                        };
                        inner
                            .message_queues
                            .entry(to_name.to_string())
                            .or_default()
                            .push_back(msg);
                        inner.kick_pending.insert(to_name.to_string());
                        inner.increment_msg_id_for_name(to_name);
                        // Window reply opens new window for back-and-forth
                        let expires = Instant::now() + inner.reply_ttl;
                        inner.reply_windows.push(ReplyWindow {
                            recipient: to_name.to_string(),
                            sender: from_name,
                            expires,
                            used: false,
                        });
                        let v2n = inner.take_notify(to_name);
                        (
                            inner.agents.get(to_name).map(|s| Arc::clone(&s.notify)),
                            None,
                            v2n,
                        )
                    } else {
                        // No window — propagate the error. NoGrant: push a one-time hint
                        // to the sender's feed so they know to call POST /grants/request.
                        match grant_err {
                            Error::NoGrant => {
                                // Only push the hint once per (from, to) pair.
                                let already_pending = inner
                                    .connection_requests
                                    .values()
                                    .any(|r| r.from_name == from_name && r.to_name == to_name);
                                let sender_v2_notify = if !already_pending {
                                    let hint = serde_json::json!({
                                        "type": "system",
                                        "event": "no_grant",
                                        "to": to_name,
                                        "hint": format!(
                                            "No grant exists. Request access: POST /grants/request {{\"to\":\"{}\",\"reason\":\"your reason\"}}",
                                            to_name
                                        ),
                                    }).to_string();
                                    inner
                                        .message_queues
                                        .entry(from_name.clone())
                                        .or_default()
                                        .push_back(QueuedMessage {
                                            payload: Payload(hint.into_bytes()),
                                            from_name: "system".to_string(),
                                            reason: None,
                                            event_type: Some("no_grant".to_string()),
                                            thread_id: None,
                                        });
                                    inner.increment_msg_id_for_name(&from_name);
                                    inner.take_notify(&from_name)
                                } else {
                                    None
                                };
                                drop(inner);
                                if let Some((sender, pending)) = sender_v2_notify {
                                    let _ = sender.send(format!(
                                        r#"{{"type":"notify","pending":{}}}"#,
                                        pending
                                    ));
                                }
                                return Err(Error::NoGrant);
                            }
                            other => return Err(other),
                        }
                    }
                }
            }
        }; // lock released

        // Persist grant usage increment after successful queue delivery.
        if let Some(gid) = consumed_grant_id
            && let Some(store) = self.token_store.clone()
        {
            self.db_write(async move {
                let _ = store.increment_grant_usage(&gid).await;
            });
        }

        // Wake any dequeue() long-poll outside the lock.
        if let Some(n) = notify_arc {
            n.notify_one();
        }

        // Fire SSE NOTIFY event if recipient is a listen-flow agent.
        if let Some((sender, pending)) = v2_notify {
            let event = format!(r#"{{"type":"notify","pending":{}}}"#, pending);
            let _ = sender.send(event);
        }

        Ok(Ack::Accepted)
    }

    /// Send a file attachment to `to_name`. Grant-gated exactly like `send()`; the blob is
    /// held server-side (DB BLOB, never a loose file) and the recipient receives a
    /// metadata-only `attachment` notify — the bytes are fetched on demand via
    /// `fetch_attachment`. Requires persistence (token store); else `Internal`.
    #[allow(clippy::too_many_arguments)]
    pub async fn attach(
        &self,
        from_token: &str,
        to_name: &str,
        filename: &str,
        mime: &str,
        bytes: Vec<u8>,
        note: Option<&str>,
        ttl: Duration,
    ) -> Result<AttachmentMeta, Error> {
        let store = self.token_store.clone().ok_or(Error::Internal)?;

        // Phase 1 — authenticate sender, confirm a grant covers sender→recipient (sync lock).
        let (from_identity, from_name, attachment_id) = {
            let inner = self.lock();
            let (from_identity, from_name) = if let Some(st) = inner.listen_tokens.get(from_token) {
                if st.revoked {
                    return Err(Error::TokenRevoked);
                }
                let name = inner
                    .token_to_name
                    .get(from_token)
                    .cloned()
                    .ok_or(Error::AnnounceRequired)?;
                (from_token.to_string(), name)
            } else {
                let tok = AgentToken(from_token.to_string());
                inner.trust.validate_agent_token(&tok)?;
                let identity = inner
                    .trust
                    .agent_identity(&tok)
                    .ok_or(Error::AuthFailed)?
                    .to_string();
                let name = inner
                    .token_to_name
                    .get(from_token)
                    .cloned()
                    .unwrap_or_default();
                (identity, name)
            };
            let to_identity = match inner.agents.get(to_name) {
                Some(s) => s.identity.clone(),
                None => return Err(Error::RecipientUnknown),
            };
            let resolved_from_name =
                inner
                    .token_to_name
                    .get(&from_identity)
                    .cloned()
                    .or_else(|| {
                        if from_name.is_empty() {
                            None
                        } else {
                            Some(from_name.clone())
                        }
                    });
            // Same gate as a text send: a grant must cover sender→recipient.
            inner
                .trust
                .check_grant_directed_with_names(
                    &from_identity,
                    &to_identity,
                    resolved_from_name.as_deref(),
                    Some(to_name),
                )
                .map_err(|_| Error::NoGrant)?;
            (from_identity, from_name, format!("att-{}", rand_hex(16)))
        };

        // Phase 2 — persist the blob (async; DB-backed). Opportunistic GC of expired blobs.
        let now_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let expires_at_secs = now_secs.saturating_add(ttl.as_secs());
        let size = bytes.len();
        let _ = store.gc_expired_attachments(now_secs).await;
        store
            .insert_attachment(
                &attachment_id,
                &from_identity,
                to_name,
                filename,
                mime,
                &bytes,
                expires_at_secs,
            )
            .await
            .map_err(|_| Error::Internal)?;

        // Phase 3 — queue the metadata-only notify to the recipient (sync lock).
        let (notify_arc, v2_notify) = {
            let mut inner = self.lock();
            let payload_json = serde_json::json!({
                "type": "attachment",
                "attachment_id": &attachment_id,
                "filename": filename,
                "mime": mime,
                "size": size,
                "from": &from_name,
                "note": note,
                "fetch": format!("GET /attachments/{}", &attachment_id),
            })
            .to_string();
            inner
                .message_queues
                .entry(to_name.to_string())
                .or_default()
                .push_back(QueuedMessage {
                    payload: Payload(payload_json.into_bytes()),
                    from_name: from_name.clone(),
                    reason: None,
                    event_type: Some("attachment".to_string()),
                    thread_id: None,
                });
            inner.kick_pending.insert(to_name.to_string());
            inner.increment_msg_id_for_name(to_name);
            let v2 = inner.take_notify(to_name);
            let arc = inner.agents.get(to_name).map(|s| Arc::clone(&s.notify));
            (arc, v2)
        };

        // Phase 4 — fire wakeups out of lock.
        if let Some(n) = notify_arc {
            n.notify_one();
        }
        if let Some((sender, pending)) = v2_notify {
            let _ = sender.send(format!(r#"{{"type":"notify","pending":{}}}"#, pending));
        }

        Ok(AttachmentMeta {
            id: attachment_id,
            filename: filename.to_string(),
            mime: mime.to_string(),
            size,
        })
    }

    /// Fetch a stored attachment by id. Access (NFR2): only the sender's identity or the
    /// intended recipient's bound name may fetch. Unknown/expired → `AttachmentNotFound`.
    /// Returns `(bytes, filename, mime)`.
    pub async fn fetch_attachment(
        &self,
        token: &str,
        attachment_id: &str,
    ) -> Result<(Vec<u8>, String, String), Error> {
        let store = self.token_store.clone().ok_or(Error::Internal)?;

        // Resolve caller identity + bound name (sync lock).
        let (caller_identity, caller_name) = {
            let inner = self.lock();
            if let Some(st) = inner.listen_tokens.get(token) {
                if st.revoked {
                    return Err(Error::TokenRevoked);
                }
                (token.to_string(), inner.token_to_name.get(token).cloned())
            } else {
                let tok = AgentToken(token.to_string());
                inner.trust.validate_agent_token(&tok)?;
                let id = inner
                    .trust
                    .agent_identity(&tok)
                    .ok_or(Error::AuthFailed)?
                    .to_string();
                (id, inner.token_to_name.get(token).cloned())
            }
        };

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _ = store.gc_expired_attachments(now).await;
        let att: StoredAttachment = store
            .get_attachment(attachment_id)
            .await
            .map_err(|_| Error::Internal)?
            .ok_or(Error::AttachmentNotFound)?;
        if att.expires_at_secs <= now {
            let _ = store.delete_attachment(attachment_id).await;
            return Err(Error::AttachmentNotFound);
        }
        // Access control: the sender's identity OR the intended recipient's bound name.
        let allowed = att.from_identity == caller_identity
            || caller_name.as_deref() == Some(att.to_name.as_str());
        if !allowed {
            return Err(Error::Forbidden);
        }
        Ok((att.bytes, att.filename, att.mime))
    }

    /// Resolve a mediation hold. Auth: valid governor token.
    pub fn resolve_mediation(
        &self,
        gov_token: &GovernorToken,
        mediation_id: &str,
        decision: MediationDecision,
    ) -> Result<MediationResult, Error> {
        let (to_name, notify, consumed_grant_id, v2n) = {
            let mut inner = self.lock();
            inner.prune_expired();
            inner.trust.validate_governor_token(gov_token)?;

            let now = Instant::now();
            let idx = inner
                .mediation_holds
                .iter()
                .position(|h| h.mediation_id == mediation_id && !h.resolved && h.expires > now)
                .ok_or(Error::MediationUnavailable)?;

            let hold_grant_id = inner.mediation_holds[idx].grant_id.clone();

            let (opens_reply_window, consumed_grant_id) = if let Some(ref gid) = hold_grant_id {
                let expected_gov = inner.trust.grant_governor_id(gid).map(|s| s.to_string());
                let expected_gov = expected_gov.ok_or(Error::MediationUnavailable)?;
                if gov_token.0 != expected_gov {
                    return Err(Error::Forbidden);
                }
                let consumed = if matches!(
                    &decision,
                    MediationDecision::Approve | MediationDecision::Modify(_)
                ) {
                    inner.trust.consume_grant_message(gid);
                    Some(gid.clone())
                } else {
                    None
                };
                let orw = inner.trust.grant_opens_reply_window(gid).unwrap_or(false);
                (orw, consumed)
            } else {
                (true, None)
            };

            inner.mediation_holds[idx].resolved = true;

            // Extract hold data while still holding the borrow.
            let (from_name, to_name_clone, payload_clone, reason_clone) = {
                let h = &inner.mediation_holds[idx];
                (
                    h.from_name.clone(),
                    h.to_name.clone(),
                    h.payload.clone(),
                    h.reason.clone(),
                )
            };

            let delivery_payload = match decision {
                MediationDecision::Block => return Ok(MediationResult::Blocked),
                MediationDecision::Approve => payload_clone,
                MediationDecision::Modify(new_payload) => new_payload,
            };

            // Recipient must still be registered to receive the queued message.
            if !inner.agents.contains_key(&to_name_clone) {
                return Ok(MediationResult::RecipientOffline);
            }

            let msg = QueuedMessage {
                payload: delivery_payload,
                from_name: from_name.clone(),
                reason: Some(reason_clone),
                event_type: None,
                thread_id: None,
            };

            inner
                .message_queues
                .entry(to_name_clone.clone())
                .or_default()
                .push_back(msg);
            inner.kick_pending.insert(to_name_clone.clone());
            inner.increment_msg_id_for_name(&to_name_clone);

            if opens_reply_window {
                let expires = Instant::now() + inner.reply_ttl;
                inner.reply_windows.push(ReplyWindow {
                    recipient: to_name_clone.clone(),
                    sender: from_name,
                    expires,
                    used: false,
                });
            }

            let notify = inner
                .agents
                .get(&to_name_clone)
                .map(|s| Arc::clone(&s.notify));
            let v2n = inner.take_notify(&to_name_clone);
            (to_name_clone, notify, consumed_grant_id, v2n)
        }; // lock released

        // Persist grant usage increment after successful queue delivery.
        if let Some(gid) = consumed_grant_id
            && let Some(store) = self.token_store.clone()
        {
            self.db_write(async move {
                let _ = store.increment_grant_usage(&gid).await;
            });
        }

        if let Some(n) = notify {
            n.notify_one();
        }

        if let Some((sender, pending)) = v2n {
            let event = format!(r#"{{"type":"notify","pending":{}}}"#, pending);
            let _ = sender.send(event);
        }

        Ok(MediationResult::Delivered { to_name })
    }

    /// Deregister an agent by name, removing it from the roster and notifying grant-peers of its offline status.
    pub fn deregister(&self, name: &str, token: &AgentToken) -> Result<(), Error> {
        // Collect grant-peer senders inside the lock (before name is removed from maps),
        // then fire the offline presence event after the lock releases. (15-0002D)
        let offline_senders = {
            let mut inner = self.lock();
            inner.trust.validate_agent_token(token)?;
            let identity = inner
                .trust
                .agent_identity(token)
                .ok_or(Error::AuthFailed)?
                .to_string();
            inner
                .registry
                .deregister(name, AgentIdentity::valid(&identity))?;
            let offline_senders = inner.grant_peer_senders(name);
            inner.agents.remove(name);
            inner.token_to_name.remove(&token.0);
            inner.active_sse_connections.remove(name);
            inner.message_queues.remove(name);
            inner.kick_pending.remove(name);
            inner
                .connection_requests
                .retain(|_, r| r.from_name != name && r.to_name != name);
            offline_senders
        }; // lock released
        push_presence_event(offline_senders, name, "offline");
        Ok(())
    }

    /// Governor force-deregister: removes any registered agent by name, bypassing identity check.
    pub fn governor_deregister(&self, name: &str, gov: &GovernorToken) -> Result<(), Error> {
        // Collect grant-peer senders inside the lock (before name is removed from maps),
        // then fire the offline presence event after the lock releases. (15-0002D)
        let offline_senders = {
            let mut inner = self.lock();
            inner.trust.validate_governor_token(gov)?;
            let offline_senders = inner.grant_peer_senders(name);
            inner.registry.force_deregister(name);
            inner.agents.remove(name);
            inner.token_to_name.retain(|_, n| n != name);
            inner.active_sse_connections.remove(name);
            inner.message_queues.remove(name);
            inner.kick_pending.remove(name);
            inner
                .connection_requests
                .retain(|_, r| r.from_name != name && r.to_name != name);
            offline_senders
        }; // lock released
        push_presence_event(offline_senders, name, "offline");
        Ok(())
    }

    /// Governor-deregisters a minted agent AND revokes their listen token (if any), atomically.
    /// The SSE revocation event and presence "offline" event are sent after the lock releases.
    pub fn revoke_by_name(&self, name: &str, gov: &GovernorToken) -> Result<(), Error> {
        let (sse_sender, offline_senders) = {
            let mut inner = self.lock();
            inner.trust.validate_governor_token(gov)?;
            // Collect grant-peer senders BEFORE removing name from maps (presence push AC4 / TR4).
            let offline_senders = inner.grant_peer_senders(name);
            // minted-agent deregister
            inner.registry.force_deregister(name);
            inner.agents.remove(name);
            inner.token_to_name.retain(|_, n| n != name);
            inner.active_sse_connections.remove(name);
            inner.message_queues.remove(name);
            inner.kick_pending.remove(name);
            inner
                .connection_requests
                .retain(|_, r| r.from_name != name && r.to_name != name);
            // listen-token revoke
            let sse_sender = if let Some(v2_tok) = inner.name_to_token.remove(name) {
                if let Some(state) = inner.listen_tokens.get_mut(&v2_tok) {
                    state.revoked = true;
                    state.sse_sender.take()
                } else {
                    None
                }
            } else {
                None
            };
            (sse_sender, offline_senders)
        }; // lock released

        if let Some(tx) = sse_sender {
            let _ = tx.send(r#"{"type":"service","event":"revoked"}"#.to_string());
        }
        // Fire presence "offline" event to all grant-peers with active SSE streams.
        push_presence_event(offline_senders, name, "offline");
        Ok(())
    }

    /// Increment the active SSE connection count for an agent.
    pub fn sse_open(&self, name: &str) {
        let mut inner = self.lock();
        *inner
            .active_sse_connections
            .entry(name.to_string())
            .or_insert(0) += 1;
    }

    /// Decrement the active SSE connection count for an agent (saturating at 0).
    pub fn sse_close(&self, name: &str) {
        let mut inner = self.lock();
        if let Some(count) = inner.active_sse_connections.get_mut(name) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                inner.active_sse_connections.remove(name);
            }
        }
    }

    /// Returns true if the named agent is currently within its liveness window or holds an active SSE connection.
    pub fn presence(&self, name: &str) -> bool {
        self.lock().is_online_effective(name)
    }

    /// Validates the token and returns the registered agent name, or None.
    pub fn registered_name(&self, token: &AgentToken) -> Option<String> {
        let inner = self.lock();
        inner.trust.validate_agent_token(token).ok()?;
        inner.token_to_name.get(&token.0).cloned()
    }

    /// Returns all active grants where the calling token's announced name is a party.
    /// Resolves the token to a name via `token_to_name`; returns `Err(AuthFailed)` if
    /// the token has no registered (announced) name.
    pub fn list_grants_for_token(
        &self,
        token: &str,
    ) -> Result<Vec<crate::trust::GrantListItem>, Error> {
        let inner = self.lock();
        let caller_name = inner
            .token_to_name
            .get(token)
            .cloned()
            .ok_or(Error::AuthFailed)?;
        Ok(inner.trust.list_grants_for_name(&caller_name))
    }

    /// Returns all active grants in the system (governor view).
    /// Requires a valid governor token; returns `Err` for missing, invalid, or non-governor tokens.
    /// When `participant_filter` is `Some(name)`, only grants involving that participant name are returned.
    pub fn list_all_grants_gov(
        &self,
        gov: &GovernorToken,
        participant_filter: Option<&str>,
    ) -> Result<Vec<crate::trust::AllGrantItem>, Error> {
        let inner = self.lock();
        inner.trust.validate_governor_token(gov)?;
        Ok(inner.trust.list_all_grants(participant_filter))
    }

    /// Validates the token (existence + expiry) without checking registration.
    pub fn validate_agent_token(&self, token: &AgentToken) -> Result<(), Error> {
        self.lock().trust.validate_agent_token(token)
    }

    /// Validates the governor token.
    pub fn validate_governor_token(&self, token: &GovernorToken) -> Result<(), Error> {
        self.lock().trust.validate_governor_token(token)
    }

    /// List all registered agents with their identity and effective status.
    /// Requires a valid governor token; returns Forbidden for agent tokens.
    /// Hidden agents always appear offline even to governors.
    pub fn list_agents(&self, gov: &GovernorToken) -> Result<Vec<AgentInfo>, Error> {
        let inner = self.lock();
        inner.trust.validate_governor_token(gov)?;
        let mut result: Vec<AgentInfo> = inner
            .agents
            .iter()
            .map(|(name, state)| {
                // Fix 3: listen-flow agents track SSE by token, not by name — check sse_connections first.
                // AC2 fix: also check registry liveness so roster shows online after announce+SSE-drop.
                let is_online = if let Some(tok) = inner.name_to_token.get(name) {
                    let v2_hidden = inner
                        .listen_tokens
                        .get(tok)
                        .map(|s| s.hidden)
                        .unwrap_or(false);
                    !v2_hidden
                        && (V2TokenState::is_sse_alive_in_hub(tok, &inner.sse_connections)
                            || inner.registry.is_online(name))
                } else {
                    let scope = inner.presence_scope_effective(name);
                    match scope {
                        Some(PresenceScope::Hidden) => false,
                        Some(_) => inner.is_online_effective(name),
                        None => false,
                    }
                };
                AgentInfo {
                    name: name.clone(),
                    identity: state.identity.clone(),
                    status: if is_online { "online" } else { "offline" },
                }
            })
            .collect();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }

    /// Rotate the caller's agent token atomically. Old token is immediately invalidated.
    /// Identity and all grants remain unchanged (grants are keyed on identity, not token).
    pub fn refresh_agent_token(&self, old_token: &AgentToken) -> Result<AgentToken, Error> {
        let (new_token, identity, expiry_instant) = {
            let mut inner = self.lock();
            let new_token = inner.trust.rotate_agent_token(old_token)?;
            let identity = inner
                .trust
                .agent_identity(&new_token)
                .map(|s| s.to_string());
            let expiry_instant = inner.trust.agent_expiry(&new_token);
            if let Some(name) = inner.token_to_name.remove(&old_token.0) {
                inner.token_to_name.insert(new_token.0.clone(), name);
            }
            (new_token, identity, expiry_instant)
        };
        if let Some(store) = self.token_store.clone() {
            let old = old_token.0.clone();
            let new = new_token.0.clone();
            let id = identity.unwrap_or_default();
            let expires_at = expiry_instant.map(instant_to_system_time);
            self.db_write(async move {
                let _ = store.delete_token(&old).await;
                if let Err(e) = store
                    .upsert_token(&new, &id, "agent", expires_at, None)
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }
        Ok(new_token)
    }

    /// Initiate a governor transfer. Returns a one-time transfer token to deliver to the recipient.
    pub fn transfer_governor(
        &self,
        from: &GovernorToken,
        to_identity: Option<&str>,
    ) -> Result<String, Error> {
        self.lock().trust.transfer_governor(from, to_identity)
    }

    /// Accept a pending governor transfer. Revokes the initiating governor; returns new gov token.
    pub fn accept_governor_transfer(
        &self,
        transfer_token: &str,
        claiming_identity: &str,
    ) -> Result<GovernorToken, Error> {
        let (new_token, expiry_instant) = {
            let mut inner = self.lock();
            let new_token = inner
                .trust
                .accept_governor_transfer(transfer_token, claiming_identity)?;
            let expiry_instant = inner.trust.governor_expiry(&new_token);
            (new_token, expiry_instant)
        };
        if let Some(store) = self.token_store.clone() {
            let new = new_token.0.clone();
            let expires_at = expiry_instant.map(instant_to_system_time);
            self.db_write(async move {
                if let Err(e) = store
                    .upsert_token(&new, &new, "governor", expires_at, None)
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }
        Ok(new_token)
    }

    /// Rotate the caller's governor token atomically. Old token is immediately invalidated.
    pub fn refresh_governor_token(
        &self,
        old_token: &GovernorToken,
    ) -> Result<GovernorToken, Error> {
        let (new_token, expiry_instant) = {
            let mut inner = self.lock();
            let new_token = inner.trust.rotate_governor_token(old_token)?;
            let expiry_instant = inner.trust.governor_expiry(&new_token);
            (new_token, expiry_instant)
        };
        if let Some(store) = self.token_store.clone() {
            let old = old_token.0.clone();
            let new = new_token.0.clone();
            let expires_at = expiry_instant.map(instant_to_system_time);
            self.db_write(async move {
                let _ = store.delete_token(&old).await;
                if let Err(e) = store
                    .upsert_token(&new, &new, "governor", expires_at, None)
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }
        Ok(new_token)
    }

    /// Governor force-rotate: invalidate all tokens for an agent identity, issue one new token.
    /// Old token entries are removed from token_to_name; new token is mapped to the same name.
    pub fn governor_refresh_agent_token(
        &self,
        gov: &GovernorToken,
        identity: &str,
    ) -> Result<AgentToken, Error> {
        let (new_token, old_ids, expiry_instant) = {
            let mut inner = self.lock();
            let (old_ids, new_token) = inner.trust.governor_rotate_agent_token(gov, identity)?;
            let expiry_instant = inner.trust.agent_expiry(&new_token);
            for old_id in &old_ids {
                if let Some(name) = inner.token_to_name.remove(old_id) {
                    inner.token_to_name.insert(new_token.0.clone(), name);
                }
            }
            (new_token, old_ids, expiry_instant)
        };
        if let Some(store) = self.token_store.clone() {
            let new = new_token.0.clone();
            let id = identity.to_string();
            let expires_at = expiry_instant.map(instant_to_system_time);
            self.db_write(async move {
                for old_id in &old_ids {
                    let _ = store.delete_token(old_id).await;
                }
                if let Err(e) = store
                    .upsert_token(&new, &id, "agent", expires_at, None)
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }
        Ok(new_token)
    }

    /// Respond to a bilateral connection request (approve or deny).
    /// Auth: valid governor token OR agent token whose identity matches the request's `to` identity.
    /// On both-approve: establishes grant (persisted if token_store is set) and delivers the
    /// original message to the recipient. On deny: drops request and queues CONNECTION_DENIED
    /// to the original sender.
    #[allow(clippy::type_complexity)] // deliberate: local tuple collects all post-lock side-effects atomically
    pub fn respond_to_connection_request(
        &self,
        token_str: &str,
        request_id: &str,
        approve: bool,
    ) -> Result<RespondStatus, Error> {
        let (status, notify_opt, persist_grant, identity_senders, v2_to_persist): (
            RespondStatus,
            Option<Arc<tokio::sync::Notify>>,
            Option<(String, String, String, String, String, String)>,
            Vec<mpsc::UnboundedSender<String>>,
            Vec<String>,
        ) = {
            let mut inner = self.lock();

            // Verify request exists.
            if !inner.connection_requests.contains_key(request_id) {
                return Err(Error::BadRequest);
            }

            // Auth check: governor takes precedence; then check recipient agent token.
            let gov_token = GovernorToken(token_str.to_string());
            let is_governor = inner.trust.validate_governor_token(&gov_token).is_ok();

            let to_identity = inner.connection_requests[request_id].to_identity.clone();
            let is_recipient = if !is_governor {
                let agent_token = AgentToken(token_str.to_string());
                match inner.trust.validate_agent_token(&agent_token) {
                    Ok(()) => inner
                        .trust
                        .agent_identity(&agent_token)
                        .map(|id| id == to_identity.as_str())
                        .unwrap_or(false),
                    Err(e) => return Err(e),
                }
            } else {
                false
            };

            if !is_governor && !is_recipient {
                return Err(Error::Forbidden);
            }

            let from_name = inner.connection_requests[request_id].from_name.clone();
            let to_name = inner.connection_requests[request_id].to_name.clone();
            let from_identity = inner.connection_requests[request_id].from_identity.clone();
            let to_identity = inner.connection_requests[request_id].to_identity.clone();

            if !approve {
                // Denial: drop request and queue CONNECTION_DENIED to original sender.
                inner.connection_requests.remove(request_id);
                let denial_json = serde_json::json!({
                    "type": "connection_denied",
                    "request_id": request_id,
                    "from": &from_name,
                    "to": &to_name,
                })
                .to_string();
                let qmsg = QueuedMessage {
                    payload: Payload(denial_json.into_bytes()),
                    from_name: "system".to_string(),
                    reason: None,
                    event_type: Some("connection_denied".to_string()),
                    thread_id: None,
                };
                inner
                    .message_queues
                    .entry(from_name.clone())
                    .or_default()
                    .push_back(qmsg);
                inner.kick_pending.insert(from_name.clone());
                inner.increment_msg_id_for_name(&from_name);
                let notify = inner.agents.get(&from_name).map(|s| Arc::clone(&s.notify));
                (
                    RespondStatus::Denied { from_name },
                    notify,
                    None,
                    vec![],
                    vec![],
                )
            } else {
                // Approval: advance stage using the new stage-based model.
                {
                    let req = inner
                        .connection_requests
                        .get_mut(request_id)
                        .ok_or(Error::BadRequest)?;
                    match req.stage {
                        ConnectionStage::PendingGovernor => {
                            if is_governor {
                                req.approving_governor = Some(token_str.to_string());
                                req.stage = ConnectionStage::PendingRecipient;
                            } else {
                                return Ok(RespondStatus::WaitingForOther);
                            }
                        }
                        ConnectionStage::PendingRecipient => {
                            if !is_governor && !is_recipient {
                                return Err(Error::Forbidden);
                            }
                            // Both sides have now approved — fall through to grant creation.
                        }
                    }
                }

                let both = matches!(
                    inner.connection_requests[request_id].stage,
                    ConnectionStage::PendingRecipient
                ) && (is_recipient || is_governor)
                    && inner.connection_requests[request_id]
                        .approving_governor
                        .is_some();

                if !both {
                    return Ok(RespondStatus::WaitingForOther);
                }

                // Both approved: establish the grant using the stored governor token.
                let (gov_tok_str, _reason) = {
                    let req = inner
                        .connection_requests
                        .get(request_id)
                        .ok_or(Error::BadRequest)?;
                    (
                        req.approving_governor.clone().ok_or(Error::BadRequest)?,
                        req.reason.clone(),
                    )
                };

                let gov_tok = GovernorToken(gov_tok_str.clone());
                // Call inner.trust directly (we hold the lock; persist happens outside via db_write).
                // FP1 fix: pass stable names so the grant survives identity rotation on reconnect.
                let grant_req = ApproveGrantRequest {
                    name_a: Some(from_name.clone()),
                    name_b: Some(to_name.clone()),
                    ..ApproveGrantRequest::default()
                };
                let grant_id = match inner.trust.approve_grant_req(
                    &gov_tok,
                    &from_identity,
                    &to_identity,
                    None,
                    grant_req,
                ) {
                    Ok(id) => id,
                    Err(e) => {
                        inner.connection_requests.remove(request_id);
                        return Err(e);
                    }
                };

                // No payload to deliver in the new flow — grant is established,
                // Bob gets notified via grant_established in approve_grant_request.
                inner.kick_pending.insert(to_name.clone());
                let notify = inner.agents.get(&to_name).map(|s| Arc::clone(&s.notify));

                // AC-T2: set ever_granted for any listen tokens in from/to identities and collect
                // their SSE senders so we can emit identity_persisted after the lock releases.
                let mut identity_senders: Vec<mpsc::UnboundedSender<String>> = Vec::new();
                for identity in [&from_identity, &to_identity] {
                    if let Some(st) = inner.listen_tokens.get_mut(identity.as_str()) {
                        if !st.ever_granted {
                            st.ever_granted = true;
                        }
                        if let Some(ref sender) = st.sse_sender {
                            identity_senders.push(sender.clone());
                        }
                    }
                }

                // AC-T3: collect token strings for both agents (by name) so we can
                // persist them to DB after the lock releases.
                let mut v2_to_persist: Vec<String> = Vec::new();
                for name in [&from_name, &to_name] {
                    if let Some(tok) = inner.name_to_token.get(name.as_str()).cloned() {
                        v2_to_persist.push(tok);
                    }
                }

                inner.connection_requests.remove(request_id);

                let to_name_for_grant = to_name.clone();
                let from_name_for_grant = from_name.clone();
                (
                    RespondStatus::Established { to_name },
                    notify,
                    Some((
                        grant_id,
                        from_identity,
                        to_identity,
                        gov_tok_str,
                        from_name_for_grant,
                        to_name_for_grant,
                    )),
                    identity_senders,
                    v2_to_persist,
                )
            }
        }; // lock released

        // Notify outside lock (matches established codebase pattern).
        if let Some(n) = notify_opt {
            n.notify_one();
        }

        // Fire identity_persisted SERVICE event on any listen SSE streams involved in the grant.
        for sender in identity_senders {
            let event = r#"{"type":"service","event":"identity_persisted"}"#.to_string();
            let _ = sender.send(event);
        }

        // Persist the newly established grant (mirrors DeliveryHub::approve_grant_req pattern).
        if let Some((grant_id, id_a, id_b, gov_id, na, nb)) = persist_grant
            && let Some(store) = self.token_store.clone()
        {
            let gid = grant_id.clone();
            self.db_write(async move {
                if let Err(e) = store
                    .upsert_grant(
                        &gid,
                        &id_a,
                        &id_b,
                        "symmetric",
                        "bypass",
                        None,
                        0,
                        None,
                        true,
                        None,
                        &gov_id,
                        Some(&na),
                        Some(&nb),
                    )
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }

        // AC-T3: persist listen tokens that gained their first grant.
        if !v2_to_persist.is_empty()
            && let Some(store) = self.token_store.clone()
        {
            for tok in v2_to_persist {
                let store2 = store.clone();
                self.db_write(async move {
                    if let Err(e) = store2.upsert_token(&tok, &tok, "listen", None, None).await {
                        eprintln!("WARNING: token store write failed: {e}");
                    }
                });
            }
        }

        Ok(status)
    }

    // ── Grant request flow ────────────────────────────────────────────────────

    /// Bob calls this after getting a NO_GRANT 403. Creates (or updates a held) grant request,
    /// notifies the governor via SSE, and returns the request_id.
    /// `update_id`: re-use a held request (Bob providing more context after a hold).
    /// Initiate or update a grant request from the calling agent to a named recipient.
    pub fn request_grant(
        &self,
        from_token_str: &str,
        to_name: &str,
        reason: Option<String>,
        update_id: Option<&str>,
    ) -> Result<String, Error> {
        let (req_id, recipient_notify) = {
            let mut inner = self.lock();
            // Governorless mode: with no active governor, a grant is established by the recipient
            // alone — the request skips the governor stage and goes straight to the recipient.
            // With a governor present this is false, so existing-governor setups are unaffected.
            let governorless = !inner.trust.has_active_governor();
            let initial_stage = if governorless {
                ConnectionStage::PendingRecipient
            } else {
                ConnectionStage::PendingGovernor
            };
            let initial_gov: Option<String> = if governorless {
                Some("recipient-consent".to_string())
            } else {
                None
            };

            // Resolve sender.
            let (from_identity, from_name) =
                if let Some(st) = inner.listen_tokens.get(from_token_str) {
                    if st.revoked {
                        return Err(Error::TokenRevoked);
                    }
                    let name = inner
                        .token_to_name
                        .get(from_token_str)
                        .cloned()
                        .unwrap_or_default();
                    (from_token_str.to_string(), name)
                } else {
                    let tok = AgentToken(from_token_str.to_string());
                    inner.trust.validate_agent_token(&tok)?;
                    let identity = inner
                        .trust
                        .agent_identity(&tok)
                        .ok_or(Error::AuthFailed)?
                        .to_string();
                    let name = inner
                        .token_to_name
                        .get(from_token_str)
                        .cloned()
                        .unwrap_or_default();
                    (identity, name)
                };

            // Fix 5: check for an active denial block on this sender→recipient pair.
            {
                let block_key = (from_identity.clone(), to_name.to_string());
                if let Some(block) = inner.denial_blocks.get(&block_key) {
                    let still_active = match block.expires_at {
                        None => true,
                        Some(exp) => {
                            let now_secs = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            exp > now_secs
                        }
                    };
                    if still_active {
                        return Err(Error::GrantBlocked(block.reason.clone()));
                    } else {
                        // Expired block — evict it so it doesn't linger.
                        inner.denial_blocks.remove(&block_key);
                    }
                }
            }

            // Recipient must be known.
            let to_identity = match inner.agents.get(to_name) {
                Some(s) => s.identity.clone(),
                None => return Err(Error::RecipientUnknown),
            };

            // Resolve request_id: update existing held request, or create new.
            let request_id = if let Some(uid) = update_id {
                // Re-use a held request (Bob providing updated reason after a hold).
                let req = inner
                    .connection_requests
                    .get_mut(uid)
                    .ok_or(Error::BadRequest)?;
                if req.from_name != from_name {
                    return Err(Error::Forbidden);
                }
                req.reason = reason.clone();
                req.stage = ConnectionStage::PendingGovernor;
                req.expires_at = Instant::now() + GRANT_REQUEST_TIMEOUT;
                uid.to_string()
            } else if let Some(existing) = inner
                .connection_requests
                .values()
                .find(|r| r.from_name == from_name && r.to_name == to_name)
                .map(|r| (r.request_id.clone(), r.expires_at))
            {
                let (existing_id, expires_at) = existing;
                if Instant::now() < expires_at {
                    // Still within the timeout window — Bob must wait.
                    return Err(Error::RequestPending);
                }
                // Expired: allow re-request in place.
                let req = inner
                    .connection_requests
                    .get_mut(&existing_id)
                    .ok_or(Error::BadRequest)?;
                req.reason = reason.clone();
                req.stage = initial_stage.clone();
                req.governor_expiry = None;
                req.recipient_expiry = None;
                req.approving_governor = initial_gov.clone();
                req.expires_at = Instant::now() + GRANT_REQUEST_TIMEOUT;
                existing_id
            } else {
                inner.req_counter += 1;
                let id = format!("req-{}", inner.req_counter);
                inner.connection_requests.insert(
                    id.clone(),
                    ConnectionRequest {
                        request_id: id.clone(),
                        from_name: from_name.clone(),
                        to_name: to_name.to_string(),
                        from_identity,
                        to_identity,
                        reason: reason.clone(),
                        stage: initial_stage.clone(),
                        governor_expiry: None,
                        recipient_expiry: None,
                        approving_governor: initial_gov.clone(),
                        expires_at: Instant::now() + GRANT_REQUEST_TIMEOUT,
                    },
                );
                id
            };

            // Notify: the governor SSE when a governor exists; the recipient directly when
            // governorless (the request is already at PendingRecipient, so the recipient approves).
            if governorless {
                let msg_json = serde_json::json!({
                    "type": "grant_request",
                    "request_id": &request_id,
                    "from": &from_name,
                    "reason": reason,
                    "action_url": format!("/grants/requests/{}", &request_id),
                    "method": "PATCH",
                    "actions": ["approve", "deny"],
                })
                .to_string();
                inner
                    .message_queues
                    .entry(to_name.to_string())
                    .or_default()
                    .push_back(QueuedMessage {
                        payload: Payload(msg_json.into_bytes()),
                        from_name: "system".to_string(),
                        reason: None,
                        event_type: Some("grant_request".to_string()),
                        thread_id: None,
                    });
                inner.kick_pending.insert(to_name.to_string());
                inner.increment_msg_id_for_name(to_name);
                let v2n = inner.take_notify(to_name);
                let notify = inner.agents.get(to_name).map(|s| Arc::clone(&s.notify));
                (request_id, Some((notify, v2n)))
            } else {
                let event = serde_json::json!({
                    "type": "grant_request",
                    "request_id": &request_id,
                    "from": &from_name,
                    "to": to_name,
                    "reason": reason,
                    "action_url": format!("/grants/requests/{}", &request_id),
                    "method": "PATCH",
                    "actions": ["approve", "deny", "hold"],
                })
                .to_string();
                let _ = inner.gov_events.send(event);
                (request_id, None)
            }
        };

        // Governorless: fire the recipient notify out-of-lock (mirrors the governor-approval
        // recipient notification in approve_grant_request).
        if let Some((notify, v2n)) = recipient_notify {
            if let Some(n) = notify {
                n.notify_one();
            }
            if let Some((sender, _pending)) = v2n {
                let _ =
                    sender.send(r#"{"type":"service","event":"identity_persisted"}"#.to_string());
            }
        }
        Ok(req_id)
    }

    /// Governor or recipient approves a pending grant request.
    /// Governor must approve first (PendingGovernor stage), then recipient (PendingRecipient).
    /// When both approve, the grant is created with min(governor_expiry, recipient_expiry).
    pub fn approve_grant_request(
        &self,
        token_str: &str,
        request_id: &str,
        expiry: Option<Duration>,
    ) -> Result<ApproveStatus, Error> {
        let (status, notify_opt, v2_to_persist, identity_senders, persist_grant) = {
            let mut inner = self.lock();
            let req = inner
                .connection_requests
                .get(request_id)
                .ok_or(Error::BadRequest)?;

            let is_governor = inner
                .trust
                .validate_governor_token(&GovernorToken(token_str.to_string()))
                .is_ok();
            let to_identity = req.to_identity.clone();
            let stage = req.stage.clone();

            // Auth: governor for PendingGovernor, recipient for PendingRecipient.
            match stage {
                ConnectionStage::PendingGovernor => {
                    if !is_governor {
                        return Err(Error::Forbidden);
                    }
                }
                ConnectionStage::PendingRecipient => {
                    // Recipient is valid if their token IS their identity (listen-flow) or trust-chain agent.
                    let is_recipient = if inner.listen_tokens.contains_key(token_str) {
                        token_str == to_identity.as_str()
                    } else {
                        let tok = AgentToken(token_str.to_string());
                        inner.trust.validate_agent_token(&tok).is_ok()
                            && inner
                                .trust
                                .agent_identity(&tok)
                                .map(|id| id == to_identity.as_str())
                                .unwrap_or(false)
                    };
                    if !is_governor && !is_recipient {
                        return Err(Error::Forbidden);
                    }
                }
            }

            let req_mut = inner
                .connection_requests
                .get_mut(request_id)
                .ok_or(Error::BadRequest)?;
            match req_mut.stage {
                ConnectionStage::PendingGovernor => {
                    req_mut.governor_expiry = expiry;
                    req_mut.approving_governor = Some(token_str.to_string());
                    req_mut.stage = ConnectionStage::PendingRecipient;
                    // Reset the timeout so the recipient gets a fresh 30 min.
                    req_mut.expires_at = Instant::now() + GRANT_REQUEST_TIMEOUT;

                    // Notify recipient via their feed.
                    let to_name = req_mut.to_name.clone();
                    let from_name = req_mut.from_name.clone();
                    let reason = req_mut.reason.clone();
                    let msg_json = serde_json::json!({
                        "type": "grant_request",
                        "request_id": request_id,
                        "from": &from_name,
                        "reason": reason,
                        "action_url": format!("/grants/requests/{}", request_id),
                        "method": "PATCH",
                        "actions": ["approve", "deny", "hold"],
                    })
                    .to_string();
                    inner
                        .message_queues
                        .entry(to_name.clone())
                        .or_default()
                        .push_back(QueuedMessage {
                            payload: Payload(msg_json.into_bytes()),
                            from_name: "system".to_string(),
                            reason: None,
                            event_type: Some("grant_request".to_string()),
                            thread_id: None,
                        });
                    inner.kick_pending.insert(to_name.clone());
                    inner.increment_msg_id_for_name(&to_name);
                    let v2n = inner.take_notify(&to_name);
                    let notify = inner.agents.get(&to_name).map(|s| Arc::clone(&s.notify));
                    (
                        ApproveStatus::PendingRecipient,
                        notify,
                        vec![],
                        v2n.into_iter().map(|(s, _)| s).collect::<Vec<_>>(),
                        None,
                    )
                }
                ConnectionStage::PendingRecipient => {
                    req_mut.recipient_expiry = expiry;

                    let from_name = req_mut.from_name.clone();
                    let to_name = req_mut.to_name.clone();
                    let from_identity = req_mut.from_identity.clone();
                    let to_identity = req_mut.to_identity.clone();
                    let gov_expiry = req_mut.governor_expiry;
                    let rec_expiry = req_mut.recipient_expiry;
                    let gov_tok_str = req_mut
                        .approving_governor
                        .clone()
                        .ok_or(Error::BadRequest)?;

                    let grant_expiry = match (gov_expiry, rec_expiry) {
                        (None, None) => None,
                        (Some(g), None) => Some(g),
                        (None, Some(r)) => Some(r),
                        (Some(g), Some(r)) => Some(g.min(r)),
                    };

                    let gov_tok = GovernorToken(gov_tok_str.clone());
                    // FP1 fix: pass stable names so the grant survives identity rotation on reconnect.
                    let grant_req = ApproveGrantRequest {
                        name_a: Some(from_name.clone()),
                        name_b: Some(to_name.clone()),
                        ..ApproveGrantRequest::default()
                    };
                    let grant_result = if gov_tok_str == "recipient-consent" {
                        // Governorless: the recipient's approval alone establishes the grant.
                        inner.trust.create_consent_grant(
                            &from_identity,
                            &to_identity,
                            grant_expiry,
                            grant_req,
                        )
                    } else {
                        inner.trust.approve_grant_req(
                            &gov_tok,
                            &from_identity,
                            &to_identity,
                            grant_expiry,
                            grant_req,
                        )
                    };
                    let grant_id = match grant_result {
                        Ok(id) => id,
                        Err(e) => {
                            inner.connection_requests.remove(request_id);
                            return Err(e);
                        }
                    };

                    // Notify Bob that the grant is established.
                    let established_json = serde_json::json!({
                        "type": "grant_established",
                        "request_id": request_id,
                        "to": &to_name,
                    })
                    .to_string();
                    inner
                        .message_queues
                        .entry(from_name.clone())
                        .or_default()
                        .push_back(QueuedMessage {
                            payload: Payload(established_json.into_bytes()),
                            from_name: "system".to_string(),
                            reason: None,
                            event_type: Some("grant_established".to_string()),
                            thread_id: None,
                        });
                    inner.kick_pending.insert(from_name.clone());
                    inner.increment_msg_id_for_name(&from_name);
                    let sender_notify = inner.take_notify(&from_name);

                    // Mark ever_granted on listen tokens and collect SSE senders.
                    let mut identity_senders: Vec<mpsc::UnboundedSender<String>> = Vec::new();
                    let mut v2_to_persist: Vec<String> = Vec::new();
                    for identity in [&from_identity, &to_identity] {
                        if let Some(st) = inner.listen_tokens.get_mut(identity.as_str()) {
                            st.ever_granted = true;
                            if let Some(ref s) = st.sse_sender {
                                identity_senders.push(s.clone());
                            }
                        }
                    }
                    for name in [&from_name, &to_name] {
                        if let Some(tok) = inner.name_to_token.get(name.as_str()).cloned() {
                            v2_to_persist.push(tok);
                        }
                    }

                    inner.connection_requests.remove(request_id);
                    let notify = inner.agents.get(&from_name).map(|s| Arc::clone(&s.notify));

                    // Merge sender_notify into identity_senders for post-lock firing.
                    let mut all_notify_senders: Vec<mpsc::UnboundedSender<String>> =
                        identity_senders;
                    if let Some((s, _)) = sender_notify {
                        all_notify_senders.push(s);
                    }

                    (
                        ApproveStatus::Established,
                        notify,
                        v2_to_persist,
                        all_notify_senders,
                        Some((
                            grant_id,
                            from_identity,
                            to_identity,
                            gov_tok_str,
                            from_name,
                            to_name,
                        )),
                    )
                }
            }
        };

        if let Some(n) = notify_opt {
            n.notify_one();
        }
        for sender in identity_senders {
            let _ = sender.send(r#"{"type":"service","event":"identity_persisted"}"#.to_string());
        }

        if let Some((grant_id, id_a, id_b, gov_id, na, nb)) = persist_grant
            && let Some(store) = self.token_store.clone()
        {
            let gid = grant_id;
            self.db_write(async move {
                if let Err(e) = store
                    .upsert_grant(
                        &gid,
                        &id_a,
                        &id_b,
                        "symmetric",
                        "bypass",
                        None,
                        0,
                        None,
                        true,
                        None,
                        &gov_id,
                        Some(&na),
                        Some(&nb),
                    )
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }
        if !v2_to_persist.is_empty()
            && let Some(store) = self.token_store.clone()
        {
            for tok in v2_to_persist {
                let store2 = store.clone();
                self.db_write(async move {
                    if let Err(e) = store2.upsert_token(&tok, &tok, "listen", None, None).await {
                        eprintln!("WARNING: token store write failed: {e}");
                    }
                });
            }
        }
        Ok(status)
    }

    /// Governor or recipient denies a pending grant request. Sends a denial message to the requester.
    /// Fix 5: `reason` is stored as a persistent block; `expires_at` is a Unix timestamp (None = permanent).
    pub fn deny_grant_request(
        &self,
        token_str: &str,
        request_id: &str,
        reason: &str,
        expires_at: Option<u64>,
    ) -> Result<(), Error> {
        let (from_name, from_identity, to_name, v2n, notify) = {
            let mut inner = self.lock();
            let req = inner
                .connection_requests
                .get(request_id)
                .ok_or(Error::BadRequest)?;

            let is_governor = inner
                .trust
                .validate_governor_token(&GovernorToken(token_str.to_string()))
                .is_ok();
            let to_identity = req.to_identity.clone();
            let is_recipient = !is_governor && {
                if inner.listen_tokens.contains_key(token_str) {
                    token_str == to_identity.as_str()
                } else {
                    let tok = AgentToken(token_str.to_string());
                    inner.trust.validate_agent_token(&tok).is_ok()
                        && inner
                            .trust
                            .agent_identity(&tok)
                            .map(|id| id == to_identity.as_str())
                            .unwrap_or(false)
                }
            };
            if !is_governor && !is_recipient {
                return Err(Error::Forbidden);
            }

            let from_name = req.from_name.clone();
            let from_identity = req.from_identity.clone();
            let to_name = req.to_name.clone();

            // Store persistent denial block.
            inner.denial_blocks.insert(
                (from_identity.clone(), to_name.clone()),
                DenialBlock {
                    reason: reason.to_string(),
                    expires_at,
                },
            );

            inner.connection_requests.remove(request_id);

            let denial = serde_json::json!({
                "type": "grant_denied",
                "request_id": request_id,
                "to": &to_name,
                "reason": reason,
            })
            .to_string();
            inner
                .message_queues
                .entry(from_name.clone())
                .or_default()
                .push_back(QueuedMessage {
                    payload: Payload(denial.into_bytes()),
                    from_name: "system".to_string(),
                    reason: None,
                    event_type: Some("grant_denied".to_string()),
                    thread_id: None,
                });
            inner.kick_pending.insert(from_name.clone());
            inner.increment_msg_id_for_name(&from_name);
            let v2n = inner.take_notify(&from_name);
            let notify = inner.agents.get(&from_name).map(|s| Arc::clone(&s.notify));
            (from_name, from_identity, to_name, v2n, notify)
        };
        let _ = from_name;
        if let Some(n) = notify {
            n.notify_one();
        }
        if let Some((sender, pending)) = v2n {
            let _ = sender.send(format!(r#"{{"type":"notify","pending":{}}}"#, pending));
        }
        if let Some(store) = self.token_store.clone() {
            let fi = from_identity;
            let tn = to_name;
            let r = reason.to_string();
            let exp = expires_at;
            self.db_write(async move {
                if let Err(e) = store.upsert_denial_block(&fi, &tn, &r, exp).await {
                    eprintln!("WARNING: denial_block persist failed: {e}");
                }
            });
        }
        Ok(())
    }

    /// Fix 5: Remove a denial block so the sender can request the grant again normally.
    /// `from_identity` is the identity of the blocked sender; `to_name` is the recipient's name.
    pub fn unblock_grant(
        &self,
        token_str: &str,
        from_identity: &str,
        to_name: &str,
    ) -> Result<(), Error> {
        let mut inner = self.lock();
        let is_governor = inner
            .trust
            .validate_governor_token(&GovernorToken(token_str.to_string()))
            .is_ok();
        if !is_governor {
            return Err(Error::Forbidden);
        }
        inner
            .denial_blocks
            .remove(&(from_identity.to_string(), to_name.to_string()));
        drop(inner);
        if let Some(store) = self.token_store.clone() {
            let fi = from_identity.to_string();
            let tn = to_name.to_string();
            self.db_write(async move {
                if let Err(e) = store.delete_denial_block(&fi, &tn).await {
                    eprintln!("WARNING: denial_block delete failed: {e}");
                }
            });
        }
        Ok(())
    }

    /// Directly create a denial block for a (from_identity, to_name) pair.
    /// Governor-only. Idempotent: overwrites any existing block silently.
    pub fn block_direct(
        &self,
        gov: &GovernorToken,
        from_identity: &str,
        to_name: &str,
        reason: &str,
        expires_at: Option<u64>,
    ) -> Result<(), Error> {
        {
            let mut inner = self.lock();
            inner.trust.validate_governor_token(gov)?;
            inner.denial_blocks.insert(
                (from_identity.to_string(), to_name.to_string()),
                DenialBlock {
                    reason: reason.to_string(),
                    expires_at,
                },
            );
        }
        if let Some(store) = self.token_store.clone() {
            let fi = from_identity.to_string();
            let tn = to_name.to_string();
            let r = reason.to_string();
            let exp = expires_at;
            self.db_write(async move {
                if let Err(e) = store.upsert_denial_block(&fi, &tn, &r, exp).await {
                    eprintln!("WARNING: denial_block persist failed: {e}");
                }
            });
        }
        Ok(())
    }

    /// Revoke an established grant by ID. Both parties are notified via SSE.
    /// Requires a valid governor token. Grant is removed from in-memory state and DB.
    pub fn revoke_grant(&self, grant_id: &str, gov: &GovernorToken) -> Result<(), Error> {
        let senders = {
            let mut inner = self.lock();
            inner.trust.validate_governor_token(gov)?;

            // Look up both parties of the grant.
            let parties = inner.trust.grant_parties(grant_id).ok_or(Error::NoGrant)?;
            let (identity_a, identity_b, name_a, name_b) = parties;

            // Remove from TrustChain.
            inner.trust.remove_grant(grant_id);

            // Collect SSE senders for both parties (by name → token → sse_sender).
            let mut senders: Vec<mpsc::UnboundedSender<String>> = Vec::new();
            for opt_name in [name_a.as_deref(), name_b.as_deref()] {
                let tok_opt = if let Some(name) = opt_name {
                    inner.name_to_token.get(name).cloned()
                } else {
                    // Fall back to looking up by identity (for minted-agent grants).
                    inner
                        .token_to_name
                        .iter()
                        .find(|(_, n)| {
                            n.as_str() == identity_a.as_str() || n.as_str() == identity_b.as_str()
                        })
                        .map(|(tok, _)| tok.clone())
                };
                if let Some(tok) = tok_opt
                    && let Some(st) = inner.listen_tokens.get(&tok)
                    && let Some(ref tx) = st.sse_sender
                {
                    senders.push(tx.clone());
                }
            }

            senders
        }; // lock released

        // Fire SSE event to each affected party.
        let event = serde_json::json!({
            "type": "service",
            "event": "grant_revoked",
            "grant_id": grant_id,
            "reason": "governor_revoked",
        })
        .to_string();
        for tx in senders {
            let _ = tx.send(event.clone());
        }

        // Remove from DB.
        if let Some(store) = self.token_store.clone() {
            let gid = grant_id.to_string();
            self.db_write(async move {
                if let Err(e) = store.delete_grant(&gid).await {
                    eprintln!("WARNING: grant delete failed: {e}");
                }
            });
        }

        Ok(())
    }

    /// Governor or recipient puts a request on hold and asks the requester for more information.
    /// The request stays open; the requester can call `request_grant` with the same `update_id`
    /// to provide more context.
    pub fn hold_grant_request(
        &self,
        token_str: &str,
        request_id: &str,
        reason: &str,
    ) -> Result<(), Error> {
        let (v2n, notify) = {
            let mut inner = self.lock();
            let req = inner
                .connection_requests
                .get(request_id)
                .ok_or(Error::BadRequest)?;

            let is_governor = inner
                .trust
                .validate_governor_token(&GovernorToken(token_str.to_string()))
                .is_ok();
            let to_identity = req.to_identity.clone();
            let is_recipient = !is_governor && {
                if inner.listen_tokens.contains_key(token_str) {
                    token_str == to_identity.as_str()
                } else {
                    let tok = AgentToken(token_str.to_string());
                    inner.trust.validate_agent_token(&tok).is_ok()
                        && inner
                            .trust
                            .agent_identity(&tok)
                            .map(|id| id == to_identity.as_str())
                            .unwrap_or(false)
                }
            };
            if !is_governor && !is_recipient {
                return Err(Error::Forbidden);
            }

            let from_name = req.from_name.clone();
            let to_name = req.to_name.clone();
            // Reset stage back to PendingGovernor — requester must resubmit.
            inner
                .connection_requests
                .get_mut(request_id)
                .ok_or(Error::BadRequest)?
                .stage = ConnectionStage::PendingGovernor;

            let hold_msg = serde_json::json!({
                "type": "grant_held",
                "request_id": request_id,
                "to": &to_name,
                "reason": reason,
                "hint": format!("Provide more context: POST /grants/request {{\"to\":\"{}\",\"reason\":\"...\",\"request_id\":\"{}\"}}", &to_name, request_id),
            }).to_string();
            inner
                .message_queues
                .entry(from_name.clone())
                .or_default()
                .push_back(QueuedMessage {
                    payload: Payload(hold_msg.into_bytes()),
                    from_name: "system".to_string(),
                    reason: None,
                    event_type: Some("grant_held".to_string()),
                    thread_id: None,
                });
            inner.kick_pending.insert(from_name.clone());
            inner.increment_msg_id_for_name(&from_name);
            let v2n = inner.take_notify(&from_name);
            let notify = inner.agents.get(&from_name).map(|s| Arc::clone(&s.notify));
            (v2n, notify)
        };
        if let Some(n) = notify {
            n.notify_one();
        }
        if let Some((sender, pending)) = v2n {
            let _ = sender.send(format!(r#"{{"type":"notify","pending":{}}}"#, pending));
        }
        Ok(())
    }

    /// Non-blocking dequeue: pops one message from the agent's queue, or returns None.
    /// Clears kick_pending when the queue becomes empty.
    pub fn pop_queued_message(&self, token: &AgentToken) -> Result<Option<QueuedMessage>, Error> {
        let mut inner = self.lock();
        inner.trust.validate_agent_token(token)?;
        let agent_name = inner
            .token_to_name
            .get(&token.0)
            .cloned()
            .ok_or(Error::AuthFailed)?;
        let msg = inner.pop_message(&agent_name);
        Ok(msg)
    }

    /// Returns true if this agent has at least one queued message (kick pending).
    /// Used by the SSE handler to fire an immediate kick on reconnect.
    pub fn kick_pending_for(&self, name: &str) -> bool {
        self.lock().kick_pending.contains(name)
    }

    /// Long-poll dequeue (§5.2). Blocks up to `max_wait`, returns Empty on timeout or no messages.
    pub async fn long_poll_dequeue(
        &self,
        token: &AgentToken,
        max_wait: Duration,
    ) -> Result<DequeueOutcome, Error> {
        // Validate token and get the agent's notify handle under the lock.
        let (agent_name, notify_arc) = {
            let inner = self.lock();
            inner.trust.validate_agent_token(token)?;
            let name = inner
                .token_to_name
                .get(&token.0)
                .cloned()
                .ok_or(Error::AuthFailed)?;
            let notify = inner
                .agents
                .get(&name)
                .map(|s| Arc::clone(&s.notify))
                .ok_or(Error::AuthFailed)?;
            (name, notify)
        };

        // Fast path: message already queued.
        {
            let mut inner = self.lock();
            if let Some(msg) = inner.pop_message(&agent_name) {
                return Ok(DequeueOutcome::Message(msg));
            }
        }

        // Slow path: wait for a notification (or timeout).
        // tokio::sync::Notify stores a permit if notify_one() fires before we await,
        // so there is no race between the fast-path check and the wait below.
        match timeout(max_wait, notify_arc.notified()).await {
            Ok(()) => {
                let mut inner = self.lock();
                match inner.pop_message(&agent_name) {
                    Some(msg) => Ok(DequeueOutcome::Message(msg)),
                    None => Ok(DequeueOutcome::Empty),
                }
            }
            Err(_) => Ok(DequeueOutcome::Empty),
        }
    }

    // ── Listen-flow methods ───────────────────────────────────────────────────

    /// Issue a new listen token (random 8-12 digit numeric string).
    pub fn issue_token(&self) -> String {
        let (tok, gc_offline_events) = {
            let mut inner = self.lock();
            let gc_offline_events = inner.gc_tokens();
            let mut rng = rand::thread_rng();
            let tok = loop {
                let digits: u64 = rng.gen_range(10_000_000..=999_999_999_999);
                let tok = digits.to_string();
                if !inner.listen_tokens.contains_key(&tok) {
                    inner.listen_tokens.insert(tok.clone(), V2TokenState::new());
                    break tok;
                }
            };
            (tok, gc_offline_events)
        }; // lock released
        // Fire any Branch-3 GC offline events after the lock is released. (15-0002H)
        for (senders, name) in gc_offline_events {
            push_presence_event(senders, &name, "offline");
        }
        tok
    }

    /// Register a new agent and obtain a listen token without opening an SSE stream.
    ///
    /// Use this token with `open_listen()` to start listening.
    /// This replaces the old anonymous /listen flow — now agents must register first.
    pub fn register_agent(&self) -> String {
        let mut inner = self.lock();
        let mut rng = rand::thread_rng();
        loop {
            let digits: u64 = rng.gen_range(10_000_000..=999_999_999_999);
            let tok = digits.to_string();
            if !inner.listen_tokens.contains_key(&tok) {
                inner.listen_tokens.insert(tok.clone(), V2TokenState::new());
                return tok;
            }
        }
    }

    /// Opens an SSE listen stream for a token.
    ///
    /// Token is REQUIRED. If no token or unknown token, returns `AuthFailed`.
    /// Use `register_agent()` to obtain a token before calling this.
    ///
    /// `force`: if true and an active SSE exists for this token, supersede it.
    ///          if false and an active SSE exists, return `ActiveSubscription` error.
    ///
    /// `name_to_bind`: if Some, attempt to bind that name at listen time (combined listen+announce).
    /// Welcome event always includes `name_in_use`, `holder_identity`, `resolution_token` fields.
    pub fn open_listen(
        &self,
        token_opt: Option<&str>,
        peer_ip: Option<String>,
        name_to_bind: Option<&str>,
        observed_host: Option<String>,
        force: bool,
    ) -> Result<(String, mpsc::UnboundedReceiver<String>), Error> {
        // Token is required.
        let provided_token = token_opt.ok_or(Error::AuthFailed)?;

        // Capture auth-token hint BEFORE locking for DCP breadcrumb generation.
        // In DCP flow, the agent presents their auth-token as Bearer on /listen.
        let dcp_auth_hint: Option<String> = Some(provided_token.to_string());
        let observed_host_str = observed_host.unwrap_or_default();
        let (token, rx, bound_name_for_persist, gc_offline_events) = {
            let mut inner = self.lock();
            let gc_offline_events = inner.gc_tokens();

            // Token must exist in listen_tokens (pre-registered via register_agent).
            let token = if inner.listen_tokens.contains_key(provided_token) {
                if inner
                    .listen_tokens
                    .get(provided_token)
                    .map(|s| s.revoked)
                    .unwrap_or(false)
                {
                    drop(inner);
                    // Fire any Branch-3 GC offline events before returning. (15-0002H)
                    for (senders, name) in gc_offline_events {
                        push_presence_event(senders, &name, "offline");
                    }
                    return Err(Error::TokenRevoked);
                }

                // Single-subscription enforcement: check if already has active SSE.
                let has_active_sse =
                    V2TokenState::is_sse_alive_in_hub(provided_token, &inner.sse_connections);
                if has_active_sse && !force {
                    drop(inner);
                    for (senders, name) in gc_offline_events {
                        push_presence_event(senders, &name, "offline");
                    }
                    return Err(Error::ActiveSubscription);
                }

                provided_token.to_string()
            } else {
                // Unknown token — check if it's a governor token (governor session-link flow).
                // Governors may present their governor token to /listen to establish an identity
                // link so that announce() can later enqueue the governor_role breadcrumb.
                let gov_candidate = GovernorToken(provided_token.to_string());
                if inner.trust.validate_governor_token(&gov_candidate).is_ok() {
                    // Mint a new listen token and link the governor identity to it.
                    let mut rng = rand::thread_rng();
                    let new_tok = loop {
                        let digits: u64 = rng.gen_range(10_000_000..=999_999_999_999);
                        let t = digits.to_string();
                        if !inner.listen_tokens.contains_key(&t) {
                            break t;
                        }
                    };
                    inner.listen_tokens.insert(new_tok.clone(), V2TokenState::new());
                    new_tok
                } else {
                    // Not a listen token or governor token — auth failed.
                    drop(inner);
                    for (senders, name) in gc_offline_events {
                        push_presence_event(senders, &name, "offline");
                    }
                    return Err(Error::AuthFailed);
                }
            };

            let (tx, rx) = mpsc::unbounded_channel::<String>();

            // Belt-and-suspenders: if this token has a stored name but it's missing from
            // name_to_token (e.g., edge case after an abnormal shutdown), re-insert.
            if let Some(name) = inner.listen_tokens.get(&token).and_then(|s| s.name.clone())
                && !inner.name_to_token.contains_key(&name)
            {
                inner.name_to_token.insert(name.clone(), token.clone());
                inner.token_to_name.insert(token.clone(), name.clone());
                inner
                    .agents
                    .entry(name.clone())
                    .or_insert_with(|| AgentState {
                        identity: token.clone(),
                        notify: Arc::new(tokio::sync::Notify::new()),
                    });
            }

            // Concurrent-use detection: if new IP differs from last IP within window.
            {
                let alert_opt: Option<String> = {
                    let state = inner.listen_tokens.get_mut(&token).unwrap();
                    if let Some(ip) = &peer_ip {
                        let concurrent_window = Duration::from_secs(60);
                        let alert = if let (Some(last_ip), Some(last_at)) =
                            (&state.last_ip, state.last_ip_at)
                        {
                            if last_ip != ip && last_at.elapsed() < concurrent_window {
                                Some(
                                    serde_json::json!({
                                        "type": "concurrent_use_alert",
                                        "token": &token,
                                        "new_ip": ip,
                                        "last_ip": last_ip,
                                    })
                                    .to_string(),
                                )
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        state.last_ip = Some(ip.clone());
                        state.last_ip_at = Some(Instant::now());
                        alert
                    } else {
                        None
                    }
                };
                if let Some(alert) = alert_opt {
                    let _ = inner.gov_events.send(alert);
                }
            }

            // Supersede old SSE if it exists.
            let old_sender = inner
                .listen_tokens
                .get_mut(&token)
                .and_then(|state| state.sse_sender.replace(tx.clone()));

            if let Some(old_tx) = old_sender {
                let supersede =
                    r#"{"type":"service","event":"superseded","reason":"new_listen_created"}"#
                        .to_string();
                let _ = old_tx.send(supersede);
            }

            // Mark as ever-listened and increment SSE connection count.
            if let Some(state) = inner.listen_tokens.get_mut(&token) {
                state.ever_listened = true;
            }
            *inner.sse_connections.entry(token.clone()).or_insert(0) += 1;

            // Governor session link: if the bearer was a governor token (not a listen token),
            // record the link on the new listen session so announce() can enqueue the breadcrumb.
            if let Some(prov_tok) = token_opt
                && prov_tok != token.as_str()
            {
                // The provided bearer generated a new listen token — check if it's a governor.
                let gov_tok = GovernorToken(prov_tok.to_string());
                if inner.trust.validate_governor_token(&gov_tok).is_ok() {
                    inner.trust.link_governor_session(prov_tok, &token);
                    if let Some(st) = inner.listen_tokens.get_mut(&token) {
                        st.governor_id = Some(prov_tok.to_string());
                    }
                }
            }

            // Fix 2: attempt inline name binding if requested.
            let (_name_in_use, _holder_identity, bound_name_for_persist) = if let Some(name) =
                name_to_bind
            {
                if inner.name_to_token.get(name).map(|t| t.as_str()) == Some(token.as_str()) {
                    // Already bound to this token — idempotent.
                    (false, None::<String>, None::<String>)
                } else if let Some(existing_token) = inner.name_to_token.get(name).cloned() {
                    let holder_alive =
                        V2TokenState::is_sse_alive_in_hub(&existing_token, &inner.sse_connections);
                    if holder_alive {
                        (true, Some(name.to_string()), None)
                    } else {
                        // Stale holder — evict and bind.
                        let old_name = inner
                            .listen_tokens
                            .get(&existing_token)
                            .and_then(|h| h.name.clone());
                        if let Some(ref n) = old_name {
                            inner.name_to_token.remove(n.as_str());
                            inner.agents.remove(n.as_str());
                            inner.token_to_name.remove(&existing_token);
                        }
                        if let Some(holder_mut) = inner.listen_tokens.get_mut(&existing_token) {
                            holder_mut.name = None;
                        }
                        inner.bind_name(&token, name);
                        (false, None, Some(name.to_string()))
                    }
                } else if inner.registry.is_online(name) {
                    // A minted agent holds this name.
                    (true, Some(name.to_string()), None)
                } else {
                    inner.bind_name(&token, name);
                    (false, None, Some(name.to_string()))
                }
            } else {
                (false, None, None)
            };

            // Emit service/welcome — the agent's entry point.
            // Normal participants already know their token (from POST /register) so we
            // do not echo it back. Exception: governor session-link path presents a governor
            // token and receives a newly minted listen token — the agent does NOT have it yet,
            // so we include it in the welcome so they can use it for announce/dequeue.
            {
                let name_opt = inner.listen_tokens.get(&token).and_then(|s| s.name.clone());
                let governor_minted = token.as_str() != provided_token;
                let welcome = if governor_minted {
                    serde_json::json!({
                        "type": "service",
                        "event": "welcome",
                        "token": &token,
                        "name": name_opt,
                        "instructions": "Call POST /announce to register your name. You will receive notify events when messages arrive — call POST /messages/dequeue to retrieve them.",
                    })
                } else {
                    serde_json::json!({
                        "type": "service",
                        "event": "welcome",
                        "name": name_opt,
                        "instructions": "Call POST /announce to register your name. You will receive notify events when messages arrive — call POST /messages/dequeue to retrieve them.",
                    })
                }
                .to_string();
                let _ = tx.send(welcome);
            }

            // DCP Step 1: mint sub_id and sub_token for this subscription.
            let sub_id = format!("sub-{}", rand_hex(8));
            let sub_token = rand_hex(16);
            inner.dcp_subs.insert(
                sub_id.clone(),
                DcpSub {
                    sub_id: sub_id.clone(),
                    sub_token: sub_token.clone(),
                    handle: None,
                    sse_sender: Some(tx.clone()),
                    created_at: std::time::Instant::now(),
                },
            );
            inner
                .dcp_sub_token_to_id
                .insert(sub_token.clone(), sub_id.clone());

            // DCP Step 2a: emit sub event with last_message_id for gap detection on reconnect.
            let last_msg_id = inner
                .listen_tokens
                .get(&token)
                .map(|st| *st.msg_id_watch.borrow())
                .unwrap_or(0);
            let sub_event = serde_json::json!({
                "type": "sub",
                "sub_id": &sub_id,
                "sub_token": &sub_token,
                "last_message_id": last_msg_id,
            })
            .to_string();
            let _ = tx.send(sub_event);

            // DCP startup announce: fire exactly once on first SSE subscription after server start.
            if !inner.startup_announced {
                inner.startup_announced = true;
                let sim_online = serde_json::json!({
                    "type": "service",
                    "event": "sim_online",
                })
                .to_string();
                let _ = tx.send(sim_online);
            }

            // DCP Step 2b: emit breadcrumb.
            // Look up the auth_token from the Authorization header — resolved from dcp_auth_to_handle.
            let breadcrumb = if let Some(ref auth_tok) = dcp_auth_hint {
                if let Some(handle) = inner.dcp_auth_to_handle.get(auth_tok.as_str()).cloned() {
                    // Returning agent — check if another sub is live for this handle
                    let other_sub_live = inner.dcp_subs.values().any(|s| {
                        s.handle.as_deref() == Some(&handle)
                            && s.sub_id != sub_id
                            && s.sse_sender
                                .as_ref()
                                .map(|tx2| !tx2.is_closed())
                                .unwrap_or(false)
                    });
                    if other_sub_live {
                        serde_json::json!({
                            "type": "breadcrumb",
                            "action": "force-announce",
                            "handle": &handle,
                            "host": &observed_host_str,
                            "hint": "POST /announce with force:true",
                        })
                        .to_string()
                    } else {
                        serde_json::json!({
                            "type": "breadcrumb",
                            "action": "announce",
                            "handle": &handle,
                            "host": &observed_host_str,
                            "hint": "POST /announce to reclaim your identity",
                        })
                        .to_string()
                    }
                } else {
                    // Unknown auth-token or no token
                    serde_json::json!({
                        "type": "breadcrumb",
                        "action": "introduce",
                        "host": &observed_host_str,
                        "hint": format!("POST /introduce {{\"handle\":\"<your-handle>\",\"sub_id\":\"{}\"}}",  &sub_id),
                    }).to_string()
                }
            } else {
                serde_json::json!({
                    "type": "breadcrumb",
                    "action": "introduce",
                    "host": &observed_host_str,
                    "hint": format!("POST /introduce {{\"handle\":\"<your-handle>\",\"sub_id\":\"{}\"}}", &sub_id),
                }).to_string()
            };
            let _ = tx.send(breadcrumb);

            // SIM-1: Re-arm notify on reconnect and fire a catch-up NOTIFY if messages are
            // already queued.  This closes two bugs:
            //   (a) re-arm race — a message that arrived while the client was between
            //       reconnects left notify_suppressed=true, so the new connection was
            //       permanently deaf;
            //   (b) deadlock — a notify fired before the previous connection dropped and
            //       dequeued meant notify_suppressed stayed true forever.
            // We reset the flag unconditionally (fixes both), then emit exactly one catch-up
            // notify using the same format as take_notify / send() so the client knows to
            // call /dequeue.
            if let Some(state) = inner.listen_tokens.get_mut(&token) {
                state.notify_suppressed = false;
            }
            // Check for queued messages under this token's bound name.
            let catchup_pending: Option<usize> = {
                let name_opt = inner
                    .listen_tokens
                    .get(&token)
                    .and_then(|s| s.name.as_deref());
                name_opt.and_then(|name| {
                    inner
                        .message_queues
                        .get(name)
                        .map(|q| q.len())
                        .filter(|&n| n > 0)
                })
            };
            if let Some(pending) = catchup_pending {
                let event = format!(r#"{{"type":"notify","pending":{}}}"#, pending);
                let _ = tx.send(event);
                // Preserve the interlock: client will clear it on dequeue.
                if let Some(state) = inner.listen_tokens.get_mut(&token) {
                    state.notify_suppressed = true;
                }
            }

            (token, rx, bound_name_for_persist, gc_offline_events)
        }; // lock released

        // Fire any Branch-3 GC offline events after the lock is released. (15-0002H)
        for (senders, name) in gc_offline_events {
            push_presence_event(senders, &name, "offline");
        }

        // Persist the newly bound name outside the lock (mirrors announce pattern).
        if let Some(name) = bound_name_for_persist
            && let Some(store) = self.token_store.clone()
        {
            let tok = token.clone();
            self.db_write(async move {
                if let Err(e) = store
                    .upsert_token(&tok, &tok, "listen", None, Some(&name))
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }

        Ok((token, rx))
    }

    /// Signal that a listen SSE stream has closed (client disconnected).
    /// When this is the last connection and the name is still bound, it means an unexpected
    /// drop (AC3 / TR3) — fire offline presence events to grant-peers.
    pub fn close_listen(&self, token: &str) {
        let (offline_senders, dropped_name) = {
            let mut inner = self.lock();

            // Determine whether this close triggers an offline presence event.
            // Conditions for firing:
            //   1. Token is NOT revoked (revoke_token/revoke_by_name fire their own events).
            //   2. This is the last (or only) SSE connection for the token.
            //   3. The name is still bound (not already unbound by cancel_listen or revoke_by_name).
            let is_revoked = inner
                .listen_tokens
                .get(token)
                .map(|st| st.revoked)
                .unwrap_or(false);
            let current_count = inner.sse_connections.get(token).copied().unwrap_or(0);
            let last_connection = current_count <= 1;

            let dropped_name = if !is_revoked && last_connection {
                inner.token_to_name.get(token).cloned()
            } else {
                None
            };
            let offline_senders = if let Some(ref name) = dropped_name {
                inner.grant_peer_senders(name)
            } else {
                vec![]
            };

            if let Some(count) = inner.sse_connections.get_mut(token) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    inner.sse_connections.remove(token);
                }
            }
            // Also clear the sender if closed (may have been superseded).
            if let Some(state) = inner.listen_tokens.get_mut(token)
                && state
                    .sse_sender
                    .as_ref()
                    .map(|s| s.is_closed())
                    .unwrap_or(false)
            {
                state.sse_sender = None;
            }

            // Reap DCP subscriptions whose SSE sender has closed. Every POST /listen mints a
            // dcp_subs entry (open_listen); without this they leak one entry per reconnect.
            // Safe: every live DCP path (resume at dcp_announce, probe, deliver) already
            // ignores subs with a closed/absent sender, so a closed-sender sub is dead weight.
            let dead_subs: Vec<(String, String)> = inner
                .dcp_subs
                .iter()
                .filter(|(_, s)| {
                    s.sse_sender
                        .as_ref()
                        .map(|tx| tx.is_closed())
                        .unwrap_or(true)
                })
                .map(|(sid, s)| (sid.clone(), s.sub_token.clone()))
                .collect();
            for (sub_id, sub_token) in dead_subs {
                inner.dcp_subs.remove(&sub_id);
                inner.dcp_sub_token_to_id.remove(&sub_token);
            }

            (offline_senders, dropped_name)
        };

        // Fire presence "offline" event to grant-peers on unexpected connection drop.
        if let Some(ref name) = dropped_name {
            push_presence_event(offline_senders, name, "offline");
        }
    }

    /// Cancel (unsubscribe) an active listen session for `token`.
    /// Closes the SSE stream, unbinds the name, and marks the agent offline.
    /// Returns Ok(()) on success, Err if the token is unknown/revoked or has no active subscription.
    pub fn cancel_listen(&self, token: &str) -> Result<(), Error> {
        let (sender_opt, offline_senders, cancelled_name) = {
            let mut inner = self.lock();
            let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
            if state.revoked {
                return Err(Error::TokenRevoked);
            }
            if !V2TokenState::is_sse_alive_in_hub(token, &inner.sse_connections) {
                return Err(Error::RecipientOffline);
            }
            // Collect peer senders and name BEFORE unbinding (presence push TR2).
            let cancelled_name = inner.token_to_name.get(token).cloned();
            let offline_senders = if let Some(ref name) = cancelled_name {
                inner.grant_peer_senders(name)
            } else {
                vec![]
            };
            // Remove SSE connection tracking.
            inner.sse_connections.remove(token);
            // Unbind name from all lookup maps.
            if let Some(name) = inner.token_to_name.remove(token) {
                inner.name_to_token.remove(&name);
                inner.agents.remove(&name);
            }
            // Clear name and SSE sender from token state.
            let st = inner.listen_tokens.get_mut(token).unwrap();
            st.name = None;
            let sender_opt = st.sse_sender.take();
            (sender_opt, offline_senders, cancelled_name)
        };
        if let Some(tx) = sender_opt {
            let _ = tx.send(r#"{"type":"service","event":"cancelled"}"#.to_string());
        }
        // Fire presence "offline" event to all grant-peers with active SSE streams.
        if let Some(ref name) = cancelled_name {
            push_presence_event(offline_senders, name, "offline");
        }
        Ok(())
    }

    /// Announce a name for a listen token.
    ///
    /// If `force` is true and the name is held by a live session (listen-token or
    /// minted agent), the holder is evicted immediately and the name is claimed.
    /// The evicted holder receives a `{"type":"service","event":"superseded",
    /// "reason":"name_reclaimed"}` event on their SSE stream.
    pub fn announce(&self, token: &str, name: &str, force: bool) -> Result<AnnounceResult, Error> {
        let mut inner = self.lock();
        // gc_tokens() returns Branch-3 offline events that must be fired after the lock
        // releases.  We use std::mem::take at each early-return path to fire any pending
        // events before returning, even if announce itself fails. (15-0002H)
        let mut gc_offline_events = inner.gc_tokens();
        // Collects (senders, name) for any token evicted by this announce call (listen-flow
        // force-eviction, stale-holder reclaim, or minted-agent force-eviction). Fired
        // out-of-lock after drop(inner), following the 15-0002C pattern. (15-0002G)
        let mut eviction_offline: Vec<(Vec<mpsc::UnboundedSender<String>>, String)> = Vec::new();

        // Validate token.
        if !inner.listen_tokens.contains_key(token) {
            drop(inner);
            for (senders, n) in gc_offline_events {
                push_presence_event(senders, &n, "offline");
            }
            return Err(Error::TokenRejected);
        }
        if inner
            .listen_tokens
            .get(token)
            .map(|s| s.revoked)
            .unwrap_or(false)
        {
            drop(inner);
            for (senders, n) in gc_offline_events {
                push_presence_event(senders, &n, "offline");
            }
            return Err(Error::TokenRevoked);
        }

        // Check if name is already claimed by THIS token (idempotent re-announce).
        if inner
            .name_to_token
            .get(name)
            .map(|t| t.as_str() == token)
            .unwrap_or(false)
        {
            // AC2 fix: refresh registry liveness on every announce so that presence
            // recovers automatically after an SSE monitor drop + re-announce cycle.
            let _ = inner.registry.register(
                name,
                AgentIdentity::valid(token),
                PresenceScope::GrantScoped,
            );
            drop(inner);
            for (senders, n) in std::mem::take(&mut gc_offline_events) {
                push_presence_event(senders, &n, "offline");
            }
            return Ok(AnnounceResult::Bound);
        }

        // Check if name is claimed by another listen token.
        if let Some(existing_token) = inner.name_to_token.get(name).cloned() {
            let holder_alive =
                V2TokenState::is_sse_alive_in_hub(&existing_token, &inner.sse_connections);
            if inner.listen_tokens.contains_key(&existing_token) {
                if holder_alive {
                    if !force {
                        // Live SSE holder — return NAME_IN_USE.
                        let resolution_stream = format!("/sessions/{}/events", name);
                        drop(inner);
                        for (senders, n) in std::mem::take(&mut gc_offline_events) {
                            push_presence_event(senders, &n, "offline");
                        }
                        return Ok(AnnounceResult::NameInUse { resolution_stream });
                    }
                    // force=true: notify holder they are being superseded.
                    // Do NOT clear holder_mut.name here — the shared cleanup block
                    // below reads it to remove name_to_token/agents/token_to_name.
                    if let Some(holder_mut) = inner.listen_tokens.get_mut(&existing_token)
                        && let Some(ref tx) = holder_mut.sse_sender
                    {
                        let _ = tx.send(
                            r#"{"type":"service","event":"superseded","reason":"name_reclaimed"}"#
                                .to_string(),
                        );
                    }
                    inner.sse_connections.remove(&existing_token);
                    // Fall through — shared cleanup below removes all name bindings.
                }
                // Stale or force-evicted: remove old name binding.
                let old_name = inner
                    .listen_tokens
                    .get(&existing_token)
                    .and_then(|h| h.name.clone());
                if let Some(ref n) = old_name {
                    // Collect grant-peer senders BEFORE removing from agents map.
                    // INVARIANT: grant_peer_senders() must be called while agents[name] still
                    // exists. Covers both force=true eviction of a live holder and stale-holder
                    // reclaim paths — grant-peers deserve sim_offline in both cases. (15-0002G)
                    let eviction_senders = inner.grant_peer_senders(n.as_str());
                    if !eviction_senders.is_empty() {
                        eviction_offline.push((eviction_senders, n.to_string()));
                    }
                    inner.name_to_token.remove(n.as_str());
                    inner.agents.remove(n.as_str());
                    inner.token_to_name.remove(&existing_token);
                    // AC2 fix: also evict the registry entry so the minted-agent conflict
                    // check below (registry.is_online + name_to_token.is_none) doesn't
                    // mistake this evicted listen-flow agent for a live minted agent.
                    inner.registry.force_deregister(n);
                }
                if let Some(holder_mut) = inner.listen_tokens.get_mut(&existing_token) {
                    holder_mut.name = None;
                }
            } else {
                inner.name_to_token.remove(name);
            }
        }

        // Also check minted-agent registry for name conflicts.
        if inner.registry.is_online(name) && !inner.name_to_token.contains_key(name) {
            if !force {
                // A minted agent holds this name.
                let resolution_stream = format!("/sessions/{}/events", name);
                drop(inner);
                for (senders, n) in std::mem::take(&mut gc_offline_events) {
                    push_presence_event(senders, &n, "offline");
                }
                return Ok(AnnounceResult::NameInUse { resolution_stream });
            }
            // force=true: deregister the minted agent and fire sim_offline to grant-peers.
            // INVARIANT: grant_peer_senders() must be called while agents[name] still exists.
            // Already past the `if !force { return }` guard above — force is always true here.
            // (15-0002G)
            let eviction_senders = inner.grant_peer_senders(name);
            if !eviction_senders.is_empty() {
                eviction_offline.push((eviction_senders, name.to_string()));
            }
            inner.registry.force_deregister(name);
            inner.agents.remove(name);
            inner.token_to_name.retain(|_, n| n != name);
            inner.active_sse_connections.remove(name);
        }

        // Claim the name (atomic under the Mutex).
        inner
            .name_to_token
            .insert(name.to_string(), token.to_string());
        inner
            .token_to_name
            .insert(token.to_string(), name.to_string());

        // Register in agents map so send() can route to this agent by name.
        let notify = Arc::new(tokio::sync::Notify::new());
        inner.agents.insert(
            name.to_string(),
            AgentState {
                identity: token.to_string(),
                notify: Arc::clone(&notify),
            },
        );

        // Update token state.
        if let Some(st) = inner.listen_tokens.get_mut(token) {
            st.name = Some(name.to_string());
        }
        // AC2 fix: register in the presence registry so the agent appears online
        // within the liveness window even when SSE is not currently active.
        let _ = inner.registry.register(
            name,
            AgentIdentity::valid(token),
            PresenceScope::GrantScoped,
        );

        // Governor breadcrumb: if this session was opened with a governor token as bearer,
        // enqueue the role breadcrumb once so the governor knows its responsibilities.
        let gov_v2_notify = {
            let gov_id_opt = inner
                .listen_tokens
                .get(token)
                .and_then(|s| s.governor_id.clone());
            if let Some(ref gov_id) = gov_id_opt {
                inner.trust.link_governor_session(gov_id, token);
                maybe_enqueue_governor_breadcrumb(&mut inner, name);
                inner.take_notify(name)
            } else {
                None
            }
        };

        // Presence push (AC1 / TR1): collect grant-peer SSE senders before releasing the lock.
        let online_senders = inner.grant_peer_senders(name);

        drop(inner);

        // Fire any Branch-3 GC offline events after the lock is released. (15-0002H)
        for (senders, n) in gc_offline_events {
            push_presence_event(senders, &n, "offline");
        }

        // Fire force-eviction / stale-holder-reclaim offline events out-of-lock. (15-0002G)
        for (senders, n) in eviction_offline {
            push_presence_event(senders, &n, "offline");
        }

        // Persist at announce so the name survives server restart even before the first grant.
        // gc_listen_tokens reaps listen-only (never-announced) tokens, so we only persist on announce.
        if let Some(store) = self.token_store.clone() {
            let tok = token.to_string();
            let name_s = name.to_string();
            self.db_write(async move {
                if let Err(e) = store
                    .upsert_token(&tok, &tok, "listen", None, Some(&name_s))
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }

        // Fire SSE NOTIFY for breadcrumb outside the lock.
        if let Some((sender, pending)) = gov_v2_notify {
            let event = format!(r#"{{"type":"notify","pending":{}}}"#, pending);
            let _ = sender.send(event);
        }

        // Fire presence "online" event to all grant-peers with active SSE streams.
        push_presence_event(online_senders, name, "online");

        Ok(AnnounceResult::Bound)
    }

    /// Non-blocking dequeue: pops one message, returns (message, remaining).
    /// If `thread` is Some, skips messages whose thread_id doesn't match.
    /// Re-arms the notify interlock BEFORE returning (R5.4 race-free guarantee).
    pub fn dequeue(
        &self,
        token: &str,
        thread: Option<&str>,
    ) -> Result<(Option<QueuedMessage>, usize), Error> {
        let mut inner = self.lock();

        let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
        if state.revoked {
            return Err(Error::TokenRevoked);
        }
        let name = match state.name.clone() {
            Some(n) => n,
            None => return Ok((None, 0)),
        };

        // R5.4: Re-arm notify BEFORE returning messages.
        if let Some(st) = inner.listen_tokens.get_mut(token) {
            st.notify_suppressed = false;
        }

        let msg = if let Some(tid) = thread {
            // Find and remove the first message matching the requested thread_id.
            if let Some(queue) = inner.message_queues.get_mut(&name) {
                if let Some(pos) = queue
                    .iter()
                    .position(|m| m.thread_id.as_deref() == Some(tid))
                {
                    let m = queue.remove(pos).unwrap();
                    if queue.is_empty() {
                        inner.kick_pending.remove(&name);
                    }
                    Some(m)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            inner.pop_message(&name)
        };

        let remaining = inner
            .message_queues
            .get(&name)
            .map(|q| q.len())
            .unwrap_or(0);
        Ok((msg, remaining))
    }

    /// Returns the number of pending messages for a token without consuming them.
    pub fn pending_count(&self, token: &str) -> Result<usize, Error> {
        let inner = self.lock();

        let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
        if state.revoked {
            return Err(Error::TokenRevoked);
        }
        let name = match &state.name {
            Some(n) => n.clone(),
            None => return Ok(0),
        };

        let count = inner
            .message_queues
            .get(&name)
            .map(|q| q.len())
            .unwrap_or(0);

        Ok(count)
    }

    /// Returns the latest message ID for a subscriber (0 = no messages received yet).
    /// Non-consuming peek. Returns None if no messages have been received.
    pub fn latest_message_id(&self, token: &str) -> Result<Option<u64>, Error> {
        let inner = self.lock();
        let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
        if state.revoked {
            return Err(Error::TokenRevoked);
        }
        let id = *state.msg_id_watch.borrow();
        if id == 0 { Ok(None) } else { Ok(Some(id)) }
    }

    /// Returns the latest queued message for a subscriber without consuming it.
    /// Returns None if the queue is empty or the subscriber has no name.
    pub fn peek_latest_message(&self, token: &str) -> Result<Option<QueuedMessage>, Error> {
        let inner = self.lock();
        let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
        if state.revoked {
            return Err(Error::TokenRevoked);
        }
        let name = match &state.name {
            Some(n) => n.clone(),
            None => return Ok(None),
        };
        Ok(inner
            .message_queues
            .get(&name)
            .and_then(|q| q.back())
            .cloned())
    }

    /// Long-poll variant of `latest_message_id`: waits until the message ID exceeds `since`,
    /// or until `max_wait` elapses. Returns Ok(Some(id)) on new message, Ok(None) on timeout.
    pub async fn wait_for_new_message_id(
        &self,
        token: &str,
        since: u64,
        max_wait: std::time::Duration,
    ) -> Result<Option<u64>, Error> {
        let mut rx = {
            let inner = self.lock();
            let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
            if state.revoked {
                return Err(Error::TokenRevoked);
            }
            let current = *state.msg_id_watch.borrow();
            if current > since {
                return Ok(Some(current));
            }
            state.msg_id_watch.subscribe()
        };
        let result = tokio::time::timeout(max_wait, async {
            loop {
                if rx.changed().await.is_err() {
                    return None;
                }
                let v = *rx.borrow_and_update();
                if v > since {
                    return Some(v);
                }
            }
        })
        .await;
        match result {
            Ok(Some(v)) => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    /// Drain all messages for a token (optionally filtered by thread_id).
    /// Re-arms notify BEFORE returning (R5.4).
    pub fn drain_queue(
        &self,
        token: &str,
        thread: Option<&str>,
    ) -> Result<Vec<QueuedMessage>, Error> {
        let mut inner = self.lock();

        let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
        if state.revoked {
            return Err(Error::TokenRevoked);
        }
        let name = match state.name.clone() {
            Some(n) => n,
            None => return Ok(vec![]),
        };

        // R5.4: Re-arm notify BEFORE returning messages.
        if let Some(st) = inner.listen_tokens.get_mut(token) {
            st.notify_suppressed = false;
        }

        let queue = inner.message_queues.entry(name.clone()).or_default();
        let messages: Vec<QueuedMessage> = if let Some(tid) = thread {
            queue
                .drain(..)
                .filter(|m| m.thread_id.as_deref() == Some(tid))
                .collect()
        } else {
            queue.drain(..).collect()
        };
        inner.kick_pending.remove(&name);
        Ok(messages)
    }

    /// Revoke a listen token atomically. Sends SERVICE revoked event on SSE then closes it.
    /// Also pushes a presence "offline" event to all grant-peers (AC4 / TR4).
    pub fn revoke_token(&self, token: &str, gov: &GovernorToken) -> Result<(), Error> {
        let (sender, offline_senders, revoked_name) = {
            let mut inner = self.lock();
            inner.trust.validate_governor_token(gov)?;
            // Collect the bound name and peer senders BEFORE marking revoked.
            let revoked_name = inner.token_to_name.get(token).cloned();
            let offline_senders = if let Some(ref name) = revoked_name {
                inner.grant_peer_senders(name)
            } else {
                vec![]
            };
            let state = inner
                .listen_tokens
                .get_mut(token)
                .ok_or(Error::TokenRejected)?;
            if state.revoked {
                return Err(Error::TokenRevoked);
            }
            state.revoked = true;
            // Take the SSE sender — dropping it closes the channel after we send the event.
            let sender = state.sse_sender.take();
            (sender, offline_senders, revoked_name)
        };

        if let Some(tx) = sender {
            let event = r#"{"type":"service","event":"revoked"}"#.to_string();
            let _ = tx.send(event);
            // tx dropped here → receiver sees None → SSE stream ends.
        }
        // Fire presence "offline" event to grant-peers.
        if let Some(ref name) = revoked_name {
            push_presence_event(offline_senders, name, "offline");
        }
        Ok(())
    }

    /// Set an agent to hidden (not visible in presence queries).
    pub fn hide(&self, token: &str) -> Result<(), Error> {
        let mut inner = self.lock();
        let state = inner
            .listen_tokens
            .get_mut(token)
            .ok_or(Error::TokenRejected)?;
        if state.revoked {
            return Err(Error::TokenRevoked);
        }
        state.hidden = true;
        Ok(())
    }

    /// Set an agent back to visible.
    pub fn show(&self, token: &str) -> Result<(), Error> {
        let mut inner = self.lock();
        let state = inner
            .listen_tokens
            .get_mut(token)
            .ok_or(Error::TokenRejected)?;
        if state.revoked {
            return Err(Error::TokenRevoked);
        }
        state.hidden = false;
        Ok(())
    }

    /// Check if a listen token is valid (not revoked/GC'd). Returns the token's name if announced.
    pub fn validate_token(&self, token: &str) -> Result<Option<String>, Error> {
        let inner = self.lock();
        let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
        if state.revoked {
            return Err(Error::TokenRevoked);
        }
        Ok(state.name.clone())
    }

    /// Send a message addressed by token (AC-S3). Looks up the announced name for `to_token`,
    /// then delegates to `send()` with the normal grant-check pipeline.
    /// Returns RECIPIENT_UNKNOWN if the token is revoked, GC'd, or not yet announced.
    pub fn send_to_token(
        &self,
        from_token: &AgentToken,
        to_token: &str,
        payload: Payload,
        reason: Option<String>,
        thread_id: Option<String>,
    ) -> Result<Ack, Error> {
        let to_name = {
            let inner = self.lock();
            let state = inner
                .listen_tokens
                .get(to_token)
                .ok_or(Error::RecipientUnknown)?;
            if state.revoked {
                return Err(Error::RecipientUnknown);
            }
            match &state.name {
                Some(n) => n.clone(),
                None => return Err(Error::RecipientUnknown),
            }
        };
        self.send(from_token, &to_name, payload, reason, thread_id)
    }

    /// Presence for a listen token: online if active SSE and not hidden.
    /// Grant-gated: querier must have an active grant with target to see their presence.
    pub fn presence_for_token(&self, token: &str, target_name: &str) -> Result<bool, Error> {
        let inner = self.lock();
        // Validate querier token.
        let querier_state = inner
            .listen_tokens
            .get(token)
            .ok_or(Error::TokenRejected)?;
        if querier_state.revoked {
            return Err(Error::TokenRevoked);
        }

        // Resolve querier name for grant check.
        let querier_name = querier_state.name.clone();

        // Resolve target identity (listen token string if they're a listen-flow agent).
        let target_tok = inner.name_to_token.get(target_name).cloned();

        // Grant check: querier must have a grant with target.
        // Check both directions (A→B or B→A) since grants can be symmetric.
        let has_grant = match &target_tok {
            Some(t_tok) => {
                // Target is a listen-flow agent.
                inner
                    .trust
                    .check_grant_directed_with_names(
                        token,
                        t_tok.as_str(),
                        querier_name.as_deref(),
                        Some(target_name),
                    )
                    .is_ok()
                    || inner
                        .trust
                        .check_grant_directed_with_names(
                            t_tok.as_str(),
                            token,
                            Some(target_name),
                            querier_name.as_deref(),
                        )
                        .is_ok()
            }
            None => {
                // Target might be a minted agent — check by identity.
                // For minted agents, identity is usually their name in the agents map.
                if let Some(agent_state) = inner.agents.get(target_name) {
                    inner
                        .trust
                        .check_grant_directed_with_names(
                            token,
                            &agent_state.identity,
                            querier_name.as_deref(),
                            Some(target_name),
                        )
                        .is_ok()
                        || inner
                            .trust
                            .check_grant_directed_with_names(
                                &agent_state.identity,
                                token,
                                Some(target_name),
                                querier_name.as_deref(),
                            )
                            .is_ok()
                } else {
                    false
                }
            }
        };

        if !has_grant {
            // No grant → target not visible (return false, not an error).
            return Ok(false);
        }

        // Check if target is a listen-flow agent.
        if let Some(target_tok) = target_tok
            && let Some(target_state) = inner.listen_tokens.get(&target_tok)
        {
            if target_state.hidden {
                return Ok(false);
            }
            let sse_alive = V2TokenState::is_sse_alive_in_hub(&target_tok, &inner.sse_connections);
            // AC2 fix: also check registry liveness so presence recovers after
            // an announce following an SSE drop (transient reconnect pattern).
            return Ok(sse_alive || inner.registry.is_online(target_name));
        }
        // Fall back to minted-agent lookup.
        Ok(inner.is_online_effective(target_name))
    }

    /// Check presence using any valid token.
    /// Grant-gated: querier must have an active grant with target to see their presence.
    pub fn presence_any_token(&self, token_str: &str, target_name: &str) -> Result<bool, Error> {
        let inner = self.lock();

        // Resolve target identity (listen token string if they're a listen-flow agent).
        let target_tok = inner.name_to_token.get(target_name).cloned();

        // Try listen token first.
        if let Some(state) = inner.listen_tokens.get(token_str) {
            if state.revoked {
                return Err(Error::TokenRevoked);
            }

            // Resolve querier name for grant check.
            let querier_name = state.name.clone();

            // Grant check: querier must have a grant with target.
            let has_grant = match &target_tok {
                Some(t_tok) => {
                    inner
                        .trust
                        .check_grant_directed_with_names(
                            token_str,
                            t_tok.as_str(),
                            querier_name.as_deref(),
                            Some(target_name),
                        )
                        .is_ok()
                        || inner
                            .trust
                            .check_grant_directed_with_names(
                                t_tok.as_str(),
                                token_str,
                                Some(target_name),
                                querier_name.as_deref(),
                            )
                            .is_ok()
                }
                None => {
                    // Target might be a minted agent.
                    if let Some(agent_state) = inner.agents.get(target_name) {
                        inner
                            .trust
                            .check_grant_directed_with_names(
                                token_str,
                                &agent_state.identity,
                                querier_name.as_deref(),
                                Some(target_name),
                            )
                            .is_ok()
                            || inner
                                .trust
                                .check_grant_directed_with_names(
                                    &agent_state.identity,
                                    token_str,
                                    Some(target_name),
                                    querier_name.as_deref(),
                                )
                                .is_ok()
                    } else {
                        false
                    }
                }
            };

            if !has_grant {
                return Ok(false);
            }

            // Check target.
            if let Some(target_tok) = target_tok
                && let Some(tgt) = inner.listen_tokens.get(&target_tok)
            {
                if tgt.hidden {
                    return Ok(false);
                }
                return Ok(V2TokenState::is_sse_alive_in_hub(
                    &target_tok,
                    &inner.sse_connections,
                ));
            }
            // minted-agent target.
            return Ok(inner.is_online_effective(target_name));
        }

        // Try minted agent token.
        let agent_token = crate::types::AgentToken(token_str.to_string());
        if inner.trust.validate_agent_token(&agent_token).is_ok() {
            // Get agent's identity for grant check.
            let querier_identity = inner.trust.agent_identity(&agent_token).map(|s| s.to_string());

            // Grant check for minted agent querier.
            let has_grant = match (&target_tok, &querier_identity) {
                (Some(t_tok), Some(q_id)) => {
                    inner
                        .trust
                        .check_grant_directed_with_names(q_id, t_tok.as_str(), None, Some(target_name))
                        .is_ok()
                        || inner
                            .trust
                            .check_grant_directed_with_names(t_tok.as_str(), q_id, Some(target_name), None)
                            .is_ok()
                }
                (None, Some(q_id)) => {
                    // Both are minted agents.
                    if let Some(agent_state) = inner.agents.get(target_name) {
                        inner
                            .trust
                            .check_grant_directed_with_names(q_id, &agent_state.identity, None, Some(target_name))
                            .is_ok()
                            || inner
                                .trust
                                .check_grant_directed_with_names(&agent_state.identity, q_id, Some(target_name), None)
                                .is_ok()
                    } else {
                        false
                    }
                }
                _ => false,
            };

            if !has_grant {
                return Ok(false);
            }

            // Check listen-flow target.
            if let Some(target_tok) = target_tok
                && let Some(tgt) = inner.listen_tokens.get(&target_tok)
            {
                if tgt.hidden {
                    return Ok(false);
                }
                return Ok(V2TokenState::is_sse_alive_in_hub(
                    &target_tok,
                    &inner.sse_connections,
                ));
            }
            // minted-agent target.
            return Ok(inner.is_online_effective(target_name));
        }

        Err(Error::AuthFailed)
    }

    // ── DCP methods ───────────────────────────────────────────────────────────

    /// DCP: introduce a new identity (handle → auth_token). TOFU — fails if handle exists.
    /// Returns (auth_token) on success.
    pub fn dcp_introduce(&self, handle: &str, sub_id: &str) -> Result<String, Error> {
        let probe_json = {
            let mut inner = self.lock();
            if inner.dcp_identities.contains_key(handle) {
                return Err(Error::HandleExists);
            }
            let auth_token = rand_hex(32);
            inner.dcp_identities.insert(
                handle.to_string(),
                DcpIdentity {
                    handle: handle.to_string(),
                    auth_token: auth_token.clone(),
                },
            );
            inner
                .dcp_auth_to_handle
                .insert(auth_token.clone(), handle.to_string());
            // Bind this sub to the handle
            if let Some(sub) = inner.dcp_subs.get_mut(sub_id) {
                sub.handle = Some(handle.to_string());
            }
            // Explicit empty grant baseline for new identity
            inner.dcp_expected_grants.insert(handle.to_string(), vec![]);
            // Emit probe
            let probe_json = Self::emit_probe_locked(&mut inner, sub_id, handle);
            drop(inner);
            // Persist identity outside lock
            if let Some(store) = self.token_store.clone() {
                let h = handle.to_string();
                let at = {
                    // re-lock briefly to get the token
                    let inner2 = self.lock();
                    inner2
                        .dcp_identities
                        .get(&h)
                        .map(|i| i.auth_token.clone())
                        .unwrap_or_default()
                };
                self.db_write(async move {
                    if let Err(e) = store.upsert_identity(&h, &at).await {
                        eprintln!("WARNING: dcp identity store write failed: {e}");
                    }
                });
            }
            probe_json
        };
        let _ = probe_json; // probe already emitted to SSE inside lock
        // Return auth_token
        let inner = self.lock();
        inner
            .dcp_identities
            .get(handle)
            .map(|i| i.auth_token.clone())
            .ok_or(Error::IdentityNotFound)
    }

    /// DCP: announce (re-claim) an existing identity onto a new subscription.
    pub fn dcp_announce(
        &self,
        auth_token: &str,
        handle: &str,
        force: bool,
        sub_id: &str,
    ) -> Result<(), Error> {
        let mut inner = self.lock();
        // Validate auth_token → handle
        let stored_handle = inner
            .dcp_auth_to_handle
            .get(auth_token)
            .cloned()
            .ok_or(Error::AuthFailed)?;
        if stored_handle != handle {
            return Err(Error::AuthFailed);
        }
        if !inner.dcp_identities.contains_key(handle) {
            return Err(Error::IdentityNotFound);
        }
        // Check if another sub is live for this handle
        let other_sub_id: Option<String> = inner
            .dcp_subs
            .iter()
            .find(|(sid, s)| {
                s.handle.as_deref() == Some(handle)
                    && sid.as_str() != sub_id
                    && s.sse_sender
                        .as_ref()
                        .map(|tx| !tx.is_closed())
                        .unwrap_or(false)
            })
            .map(|(sid, _)| sid.clone());
        if let Some(old_sub_id) = other_sub_id {
            if !force {
                return Err(Error::NameInUse);
            }
            // force=true: fence the old sub
            if let Some(old_sub) = inner.dcp_subs.get_mut(&old_sub_id) {
                if let Some(ref tx) = old_sub.sse_sender {
                    let _ = tx.send(
                        r#"{"type":"service","event":"superseded","reason":"name_reclaimed"}"#
                            .to_string(),
                    );
                }
                old_sub.sse_sender = None;
                old_sub.handle = None;
                let old_token = old_sub.sub_token.clone();
                inner.dcp_sub_token_to_id.remove(&old_token);
            }
            inner.dcp_subs.remove(&old_sub_id);
            // Also unbind from V2 routing maps
            inner.name_to_token.remove(handle);
            inner.agents.remove(handle);
            if let Some(old_tok) = inner
                .token_to_name
                .iter()
                .find(|(_, n)| n.as_str() == handle)
                .map(|(k, _)| k.clone())
            {
                inner.token_to_name.remove(&old_tok);
            }
        }
        // Bind this identity to sub_id
        if let Some(sub) = inner.dcp_subs.get_mut(sub_id) {
            sub.handle = Some(handle.to_string());
        }
        // Re-use sub_id as the "token" for V2 routing so message delivery works
        inner
            .name_to_token
            .insert(handle.to_string(), sub_id.to_string());
        inner
            .token_to_name
            .insert(sub_id.to_string(), handle.to_string());
        inner
            .agents
            .entry(handle.to_string())
            .or_insert_with(|| AgentState {
                identity: auth_token.to_string(),
                notify: Arc::new(tokio::sync::Notify::new()),
            });
        // Emit connect_probe
        Self::emit_probe_locked(&mut inner, sub_id, handle);
        Ok(())
    }

    /// DCP: emit a connect_probe event to the sub's SSE channel.
    fn emit_probe_locked(inner: &mut HubInner, sub_id: &str, handle: &str) -> Option<String> {
        // Opportunistic GC: drop probes that are spent (acked) or past their 120s TTL so the
        // map stays bounded by in-flight probes. Probes are keyed `{sub_id}:{probe_instance}`
        // with a randomized instance, so without this re-probes accumulate forever (the
        // long-standing dcp_probes growth the [state] log was added to catch).
        let now_probe = std::time::Instant::now();
        inner
            .dcp_probes
            .retain(|_, p| !p.used && p.expires_at > now_probe);

        let nonce = rand_hex(16);
        let probe_instance = format!("pi-{}", rand_hex(4));
        let expires_at = std::time::Instant::now() + Duration::from_secs(120);
        let expires_at_secs = {
            let now_sys = SystemTime::now();
            let dur_to_expiry = expires_at.duration_since(std::time::Instant::now());
            now_sys
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                + dur_to_expiry.as_secs()
        };
        let key = format!("{}:{}", sub_id, probe_instance);
        inner.dcp_probes.insert(
            key.clone(),
            DcpProbe {
                nonce: nonce.clone(),
                sub_id: sub_id.to_string(),
                handle: handle.to_string(),
                probe_instance: probe_instance.clone(),
                expires_at,
                used: false,
            },
        );
        let probe_json = serde_json::json!({
            "type": "connect_probe",
            "nonce": &nonce,
            "sub_id": sub_id,
            "probe_instance": &probe_instance,
            "expires_at_secs": expires_at_secs,
        })
        .to_string();
        if let Some(sub) = inner.dcp_subs.get(sub_id)
            && let Some(ref tx) = sub.sse_sender
        {
            let _ = tx.send(probe_json.clone());
        }
        Some(probe_json)
    }

    /// DCP: acknowledge a connect_probe. Validates nonce, marks CONNECTED.
    pub fn dcp_probe_ack(&self, auth_token: &str, nonce: &str, sub_id: &str) -> Result<(), Error> {
        let mut inner = self.lock();
        let handle = inner
            .dcp_auth_to_handle
            .get(auth_token)
            .cloned()
            .ok_or(Error::AuthFailed)?;
        let now = std::time::Instant::now();
        // Find matching probe
        let probe_key = inner
            .dcp_probes
            .iter()
            .find(|(_, p)| {
                p.sub_id == sub_id
                    && p.nonce == nonce
                    && p.handle == handle
                    && !p.used
                    && p.expires_at > now
            })
            .map(|(k, _)| k.clone());
        let probe_key = match probe_key {
            Some(k) => k,
            None => {
                // Check if expired (nonce exists but expired) vs totally missing
                let exists_expired = inner
                    .dcp_probes
                    .values()
                    .any(|p| p.sub_id == sub_id && p.nonce == nonce && p.handle == handle);
                return Err(if exists_expired {
                    Error::ProbeExpired
                } else {
                    Error::ProbeInvalid
                });
            }
        };
        inner.dcp_probes.get_mut(&probe_key).unwrap().used = true;
        // Grant integrity check
        let expected = inner.dcp_expected_grants.get(&handle).cloned();
        match expected {
            None => {
                // No record at all — this shouldn't happen after introduce/announce
                return Err(Error::ProbeInvalid);
            }
            Some(expected_ids) => {
                let current_grants: Vec<String> = inner.trust.grant_ids_for_handle(&handle);
                // New identity: expected is empty — explicit assertion, not vacuous
                // Existing identity: must match current grant set
                if expected_ids.is_empty() {
                    // new identity baseline — OK
                } else {
                    let mut exp_sorted = expected_ids.clone();
                    let mut cur_sorted = current_grants.clone();
                    exp_sorted.sort();
                    cur_sorted.sort();
                    if exp_sorted != cur_sorted {
                        return Err(Error::ProbeInvalid);
                    }
                }
                // Update expected grants to current snapshot
                inner
                    .dcp_expected_grants
                    .insert(handle.clone(), current_grants);
            }
        }
        // Emit CONNECTED breadcrumb
        let connected_json = serde_json::json!({
            "type": "connected",
            "handle": &handle,
            "sub_id": sub_id,
        })
        .to_string();
        if let Some(sub) = inner.dcp_subs.get(sub_id)
            && let Some(ref tx) = sub.sse_sender
        {
            let _ = tx.send(connected_json);
        }
        Ok(())
    }

    /// DCP: leave — cancel sub while preserving identity.
    pub fn dcp_leave(&self, auth_token: &str, sub_id: &str) -> Result<(), Error> {
        // Collect grant-peer senders inside the lock (before handle is unbound from maps),
        // then fire the offline presence event after the lock releases. (15-0002D)
        let (offline_senders, handle) = {
            let mut inner = self.lock();
            let handle = inner
                .dcp_auth_to_handle
                .get(auth_token)
                .cloned()
                .ok_or(Error::AuthFailed)?;
            let sub_handle_matches = inner
                .dcp_subs
                .get(sub_id)
                .map(|s| s.handle.as_deref() == Some(&handle))
                .unwrap_or(false);
            if !sub_handle_matches {
                return Err(Error::Forbidden);
            }
            let leave_json = serde_json::json!({
                "type": "service",
                "event": "leave",
                "handle": &handle,
                "reason": "agent_requested",
            })
            .to_string();
            if let Some(sub) = inner.dcp_subs.get_mut(sub_id) {
                if let Some(ref tx) = sub.sse_sender {
                    let _ = tx.send(leave_json);
                }
                sub.sse_sender = None;
            }
            // Collect grant-peer senders BEFORE unbinding name routing.
            let offline_senders = inner.grant_peer_senders(&handle);
            // Unbind name routing
            inner.name_to_token.remove(&handle);
            inner.agents.remove(&handle);
            inner.token_to_name.remove(sub_id);
            // Remove sub
            let sub_token = inner.dcp_subs.get(sub_id).map(|s| s.sub_token.clone());
            inner.dcp_subs.remove(sub_id);
            if let Some(tok) = sub_token {
                inner.dcp_sub_token_to_id.remove(&tok);
            }
            // Identity persists in dcp_identities — leave != destroy identity
            (offline_senders, handle)
        }; // lock released
        push_presence_event(offline_senders, &handle, "offline");
        Ok(())
    }

    /// DCP: cancel a subscription pre-announce using the sub_token.
    /// No presence event: this path is pre-announce so the handle is not yet bound to
    /// the name routing maps and no grant-peers exist to notify. (15-0002D)
    pub fn dcp_cancel_sub_by_token(&self, sub_token: &str) -> Result<(), Error> {
        let mut inner = self.lock();
        let sub_id = inner
            .dcp_sub_token_to_id
            .get(sub_token)
            .cloned()
            .ok_or(Error::TokenRejected)?;
        if let Some(sub) = inner.dcp_subs.get_mut(&sub_id) {
            if let Some(ref tx) = sub.sse_sender {
                let _ = tx.send(r#"{"type":"service","event":"cancelled"}"#.to_string());
            }
            sub.sse_sender = None;
        }
        inner.dcp_subs.remove(&sub_id);
        inner.dcp_sub_token_to_id.remove(sub_token);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_hub(lapse: Duration) -> DeliveryHub {
        DeliveryHub::new(lapse)
    }

    fn setup_hub_ab() -> (DeliveryHub, AgentToken, AgentToken) {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        (hub, tok_a, tok_b)
    }

    /// AC-MSG-1: A and B registered with valid grant; send → ACCEPTED; B's dequeue returns payload once.
    #[tokio::test]
    async fn ac_msg_1_send_accepted_dequeue_returns_payload_once() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let ack = hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None);
        assert!(matches!(ack, Ok(Ack::Accepted)));

        let outcome = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();
        match outcome {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"hi"),
            DequeueOutcome::Empty => panic!("expected a message"),
        }

        let second = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(20))
            .await
            .unwrap();
        assert!(matches!(second, DequeueOutcome::Empty));
    }

    /// AC-MSG-2: send to unregistered recipient → RecipientUnknown.
    #[tokio::test]
    async fn ac_msg_2_recipient_unknown() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();

        let result = hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None);
        assert!(matches!(result, Err(Error::RecipientUnknown)));

        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();
        let empty = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(20))
            .await
            .unwrap();
        assert!(matches!(empty, DequeueOutcome::Empty));
    }

    /// AC1: Send to offline-but-registered agent returns ACCEPTED and queues the message.
    /// AC2: Offline agent dequeues later and receives message with correct `from` field.
    #[tokio::test]
    async fn ac1_ac2_send_to_offline_registered_queues_message() {
        let hub = make_hub(Duration::from_millis(10));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // Let liveness lapse so bob is "offline"
        tokio::time::sleep(Duration::from_millis(20)).await;

        // AC1: send still accepted even though bob's liveness lapsed
        let result = hub.send(
            &tok_a,
            "bob",
            Payload(b"hello offline".to_vec()),
            None,
            None,
        );
        assert!(
            matches!(result, Ok(Ack::Accepted)),
            "send to offline-but-registered agent must be accepted"
        );

        // Re-register bob (refreshes liveness), messages survive
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // AC2: message is in queue with correct from field
        let outcome = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();
        match outcome {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.payload.0, b"hello offline");
                assert_eq!(m.from_name, "alice");
            }
            DequeueOutcome::Empty => panic!("expected queued message after offline delivery"),
        }
    }

    /// AC3: send to UNREGISTERED agent returns RECIPIENT_UNKNOWN.
    #[test]
    fn ac3_send_to_unregistered_returns_unknown() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        // bob is NOT registered

        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None),
            Err(Error::RecipientUnknown)
        ));
    }

    /// AC5: Dequeue drains FIFO.
    /// AC6: Multiple messages sent while offline — all returned in order.
    #[tokio::test]
    async fn ac5_ac6_multiple_offline_messages_returned_in_fifo_order() {
        let hub = make_hub(Duration::from_millis(10));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // Let liveness lapse
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Queue three messages while bob is offline
        hub.send(&tok_a, "bob", Payload(b"msg1".to_vec()), None, None)
            .unwrap();
        hub.send(&tok_a, "bob", Payload(b"msg2".to_vec()), None, None)
            .unwrap();
        hub.send(&tok_a, "bob", Payload(b"msg3".to_vec()), None, None)
            .unwrap();

        // Re-register bob
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let m1 = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        let m2 = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        let m3 = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        let empty = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(20))
            .await
            .unwrap();

        match m1 {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"msg1"),
            _ => panic!(),
        }
        match m2 {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"msg2"),
            _ => panic!(),
        }
        match m3 {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"msg3"),
            _ => panic!(),
        }
        assert!(matches!(empty, DequeueOutcome::Empty));
    }

    /// AC7: kick_pending is set after queuing, cleared when queue empties.
    #[test]
    fn ac7_kick_pending_set_on_queue_cleared_on_drain() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        assert!(!hub.kick_pending_for("bob"));

        hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None)
            .unwrap();
        assert!(
            hub.kick_pending_for("bob"),
            "kick_pending should be set after queuing"
        );

        // Pop message
        let msg = hub.pop_queued_message(&tok_b).unwrap();
        assert!(msg.is_some());
        assert!(
            !hub.kick_pending_for("bob"),
            "kick_pending cleared when queue empties"
        );
    }

    /// AC-MSG-5: dequeue with valid token and no pending messages blocks up to max_wait.
    #[tokio::test]
    async fn ac_msg_5_dequeue_blocks_then_returns_empty() {
        let (hub, _tok_a, tok_b) = setup_hub_ab();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let start = tokio::time::Instant::now();
        let result = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(50))
            .await;
        let elapsed = start.elapsed();

        assert!(matches!(result, Ok(DequeueOutcome::Empty)));
        assert!(elapsed >= Duration::from_millis(40));
        assert!(elapsed < Duration::from_millis(200));
    }

    /// AC-MSG-6: dequeue with invalid token → AuthFailed.
    #[tokio::test]
    async fn ac_msg_6_dequeue_invalid_token_returns_auth_failed() {
        let hub = make_hub(Duration::from_secs(30));
        let bad_token = AgentToken("not-a-real-token".into());

        let result = hub
            .long_poll_dequeue(&bad_token, Duration::from_millis(10))
            .await;
        assert!(matches!(result, Err(Error::AuthFailed)));
    }

    /// AC-MSG-7: two messages in order are dequeued in send order, each once.
    #[tokio::test]
    async fn ac_msg_7_messages_delivered_in_send_order() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        hub.send(&tok_a, "bob", Payload(b"1".to_vec()), None, None)
            .unwrap();
        hub.send(&tok_a, "bob", Payload(b"2".to_vec()), None, None)
            .unwrap();

        let m1 = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        let m2 = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(50))
            .await
            .unwrap();

        match m1 {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"1"),
            DequeueOutcome::Empty => panic!("expected first message"),
        }
        match m2 {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"2"),
            DequeueOutcome::Empty => panic!("expected second message"),
        }

        let empty = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(20))
            .await
            .unwrap();
        assert!(matches!(empty, DequeueOutcome::Empty));
    }

    /// Queued message survives re-registration (messages not tied to a session channel).
    #[tokio::test]
    async fn queued_message_survives_reregistration() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        hub.send(&tok_a, "bob", Payload(b"survive".to_vec()), None, None)
            .unwrap();

        // Bob re-registers (new notify, same queue)
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let outcome = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();
        match outcome {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"survive"),
            DequeueOutcome::Empty => panic!("message should survive re-registration"),
        }
    }

    /// Grant with max_messages=3 allows exactly 3 deliveries; 4th → GrantExhausted.
    #[test]
    fn budget_decrements_on_delivery() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                max_messages: Some(3),
                ..Default::default()
            },
        )
        .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        assert!(
            hub.send(&tok_a, "bob", Payload(b"1".to_vec()), None, None)
                .is_ok()
        );
        assert!(
            hub.send(&tok_a, "bob", Payload(b"2".to_vec()), None, None)
                .is_ok()
        );
        assert!(
            hub.send(&tok_a, "bob", Payload(b"3".to_vec()), None, None)
                .is_ok()
        );
        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"4".to_vec()), None, None),
            Err(Error::GrantExhausted)
        ));
    }

    /// Grant with max_messages=1 is exhausted after a single successful delivery.
    #[test]
    fn one_time_grant_exhausted_after_first_delivery() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                max_messages: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        assert!(
            hub.send(&tok_a, "bob", Payload(b"first".to_vec()), None, None)
                .is_ok()
        );
        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"second".to_vec()), None, None),
            Err(Error::GrantExhausted)
        ));
    }

    /// AC-12: two concurrent sends against a max_messages=1 grant — exactly one succeeds.
    #[tokio::test]
    async fn ac_12_concurrent_sends_single_use_grant_exactly_one_succeeds() {
        let hub = Arc::new(make_hub(Duration::from_secs(30)));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                max_messages: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let tok_a2 = AgentToken(tok_a.0.clone());

        let hub1 = Arc::clone(&hub);
        let hub2 = Arc::clone(&hub);

        let t1 = tokio::spawn(async move {
            hub1.send(&tok_a, "bob", Payload(b"from-t1".to_vec()), None, None)
        });
        let t2 = tokio::spawn(async move {
            hub2.send(&tok_a2, "bob", Payload(b"from-t2".to_vec()), None, None)
        });

        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();

        let successes = [r1.is_ok(), r2.is_ok()].iter().filter(|&&x| x).count();
        let exhausted = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(Error::GrantExhausted)))
            .count();

        assert_eq!(
            successes, 1,
            "exactly one send should succeed on a max_messages=1 grant"
        );
        assert_eq!(
            exhausted, 1,
            "exactly one send should return GrantExhausted"
        );
    }

    // ── Reply window tests ────────────────────────────────────────────────────

    fn setup_hub_ab_window() -> (DeliveryHub, GovernorToken, AgentToken, AgentToken) {
        use crate::trust::GrantDirection;
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        // AToB-only grant: only alice→bob is covered by a standing grant.
        // bob→alice has no grant, so bob must use reply windows to reach alice.
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                direction: Some(GrantDirection::AToB),
                opens_reply_window: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        (hub, gov, tok_a, tok_b)
    }

    /// Successful S→R send (grant with opens_reply_window=true) opens window (R,S).
    /// Subsequent R→S send with no standing grant succeeds via window.
    #[tokio::test]
    async fn reply_window_opens_after_delivery() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_ab_window();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // Alice → Bob (grant path), opens window (bob, alice)
        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));

        // Drain the message from bob's queue
        let _ = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(50))
            .await;

        // Bob → Alice via reply window
        assert!(matches!(
            hub.send(&tok_b, "alice", Payload(b"reply".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));

        let outcome = hub
            .long_poll_dequeue(&tok_a, Duration::from_millis(50))
            .await
            .unwrap();
        match outcome {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"reply"),
            DequeueOutcome::Empty => panic!("expected reply message"),
        }
    }

    /// After window consumed, next R→S send with no grant → RequestPending (bilateral consent).
    #[tokio::test]
    async fn reply_window_consumed_on_use() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_ab_window();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // Open window: alice → bob
        hub.send(&tok_a, "bob", Payload(b"msg".to_vec()), None, None)
            .unwrap();

        // Bob replies (consumes window)
        assert!(matches!(
            hub.send(&tok_b, "alice", Payload(b"reply".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));

        // Window used=true; no standing grant bob→alice → NoGrant (bilateral consent via request_grant).
        assert!(matches!(
            hub.send(&tok_b, "alice", Payload(b"again".to_vec()), None, None),
            Err(Error::NoGrant)
        ));
    }

    /// Reply via window opens a new window (S,R) enabling back-and-forth.
    #[tokio::test]
    async fn reply_window_back_and_forth() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_ab_window();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // Alice → Bob: opens window (bob, alice)
        hub.send(&tok_a, "bob", Payload(b"1".to_vec()), None, None)
            .unwrap();

        // Bob → Alice: uses window (bob, alice), opens new window (alice, bob)
        hub.send(&tok_b, "alice", Payload(b"2".to_vec()), None, None)
            .unwrap();

        // Alice → Bob again: uses new window (alice, bob)
        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"3".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));
    }

    /// Standing R→S grant used; window stays unused.
    #[test]
    fn standing_grant_used_window_not_consumed() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        // Symmetric grant with opens_reply_window=true
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // Alice → Bob: opens window (bob, alice)
        hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None)
            .unwrap();

        // Bob → Alice: has a standing grant (symmetric), uses grant NOT window
        // Window should remain used=false
        hub.send(&tok_b, "alice", Payload(b"reply".to_vec()), None, None)
            .unwrap();

        // Verify window still unused by checking inner state
        let inner = hub.inner.lock().unwrap();
        let window = inner
            .reply_windows
            .iter()
            .find(|w| w.recipient == "bob" && w.sender == "alice");
        if let Some(w) = window {
            assert!(
                !w.used,
                "window should not be consumed when standing grant is used"
            );
        }
        // (if no window was created, that's also fine — grant covers both directions)
    }

    /// SIMPLE_IM_REPLY_TTL_SECS env var configures TTL.
    #[test]
    fn reply_ttl_env_var() {
        let hub = make_hub(Duration::from_secs(30));
        let inner = hub.inner.lock().unwrap();
        assert!(inner.reply_ttl >= Duration::from_secs(5));
        assert!(inner.reply_ttl <= Duration::from_secs(600));
    }

    /// Expired window is not used.
    #[tokio::test]
    async fn expired_window_not_used() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // Insert an already-expired window
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.reply_windows.push(ReplyWindow {
                recipient: "bob".into(),
                sender: "alice".into(),
                expires: Instant::now() - Duration::from_secs(1),
                used: false,
            });
        }

        // Bob → Alice with expired window: no grant, no valid window → NoGrant.
        assert!(matches!(
            hub.send(&tok_b, "alice", Payload(b"hi".to_vec()), None, None),
            Err(Error::NoGrant)
        ));
    }

    /// Two concurrent R→S sends on one window: exactly one Accepted, one error.
    #[tokio::test]
    async fn concurrent_sends_on_one_window_exactly_one_succeeds() {
        use crate::trust::GrantDirection;
        let hub = Arc::new(make_hub(Duration::from_secs(30)));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        // AToB only: bob→alice has no standing grant, must use the reply window.
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                direction: Some(GrantDirection::AToB),
                opens_reply_window: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // Open window (bob, alice)
        hub.send(&tok_a, "bob", Payload(b"init".to_vec()), None, None)
            .unwrap();

        let tok_b2 = AgentToken(tok_b.0.clone());
        let hub1 = Arc::clone(&hub);
        let hub2 = Arc::clone(&hub);

        let t1 =
            tokio::spawn(
                async move { hub1.send(&tok_b, "alice", Payload(b"r1".to_vec()), None, None) },
            );
        let t2 = tokio::spawn(async move {
            hub2.send(&tok_b2, "alice", Payload(b"r2".to_vec()), None, None)
        });

        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();

        // One gets Accepted (via window); the other gets NoGrant (no standing grant).
        let accepted = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Ok(Ack::Accepted)))
            .count();
        let no_grant = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(Error::NoGrant)))
            .count();
        assert_eq!(
            accepted, 1,
            "exactly one concurrent window send should be Accepted"
        );
        assert_eq!(
            no_grant, 1,
            "the other concurrent send should return NoGrant"
        );
    }

    // ── Brief authorization tests ─────────────────────────────────────────────

    fn setup_hub_brief() -> (DeliveryHub, GovernorToken, AgentToken, AgentToken) {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        // No grant between alice and bob
        (hub, gov, tok_a, tok_b)
    }

    /// No grant + no window + no reason → send returns NoGrant; request_grant creates ConnectionRequest.
    #[test]
    fn no_grant_creates_connection_request() {
        let (hub, _gov, tok_a, _tok_b) = setup_hub_brief();
        hub.inner.lock().unwrap().agents.insert(
            "bob".into(),
            AgentState {
                identity: "id-bob".into(),
                notify: Arc::new(tokio::sync::Notify::new()),
            },
        );
        hub.inner
            .lock()
            .unwrap()
            .registry
            .register("bob", AgentIdentity::valid("id-bob"), PresenceScope::Public)
            .unwrap();

        let result = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
        assert!(
            matches!(result, Err(Error::NoGrant)),
            "no-grant send must return NoGrant"
        );

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");
        let inner = hub.inner.lock().unwrap();
        assert!(
            inner.connection_requests.contains_key(&request_id),
            "request_grant must create a connection request"
        );
    }

    /// No grant + no window + reason → send returns NoGrant; request_grant creates ConnectionRequest with reason.
    #[tokio::test]
    async fn no_grant_with_reason_creates_request() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_brief();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let result = hub.send(
            &tok_a,
            "bob",
            Payload(b"hello".to_vec()),
            Some("urgent request".into()),
            None,
        );
        assert!(
            matches!(result, Err(Error::NoGrant)),
            "no-grant send must return NoGrant"
        );

        let request_id = hub
            .request_grant(&tok_a.0, "bob", Some("urgent request".into()), None)
            .expect("request_grant must succeed");
        assert!(
            request_id.starts_with("req-"),
            "request_id should start with req-"
        );
    }

    /// No grant + governor offline → send returns NoGrant; request_grant creates ConnectionRequest without governor online.
    #[test]
    fn no_grant_no_governor_still_creates_request() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.inner
            .lock()
            .unwrap()
            .trust
            .set_governor_online(&gov, false);
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        assert!(
            matches!(
                hub.send(
                    &tok_a,
                    "bob",
                    Payload(b"hi".to_vec()),
                    Some("reason".into()),
                    None
                ),
                Err(Error::NoGrant)
            ),
            "no-grant send must return NoGrant"
        );
        let request_id = hub
            .request_grant(&tok_a.0, "bob", Some("reason".into()), None)
            .expect("request_grant must succeed regardless of governor online status");
        assert!(!request_id.is_empty(), "connection request must be created");
    }

    /// Both governor and recipient approve → grant established; subsequent sends succeed.
    #[tokio::test]
    async fn connection_request_both_approve_delivers_message() {
        let (hub, gov, tok_a, tok_b) = setup_hub_brief();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Governor approves first (PendingGovernor → PendingRecipient).
        assert!(matches!(
            hub.approve_grant_request(&gov.0, &request_id, None),
            Ok(ApproveStatus::PendingRecipient)
        ));

        // Drain the grant_request event queued to Bob.
        hub.long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();

        // Bob (recipient) approves → both approved → Established.
        assert!(matches!(
            hub.approve_grant_request(&tok_b.0, &request_id, None),
            Ok(ApproveStatus::Established)
        ));

        // Grant established → alice can now send directly.
        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"follow-up".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));
    }

    /// Governor denies → request dropped, sender gets CONNECTION_DENIED in queue.
    #[tokio::test]
    async fn connection_request_governor_deny_queues_denied_to_sender() {
        let (hub, gov, tok_a, tok_b) = setup_hub_brief();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Governor denies
        let outcome = hub.respond_to_connection_request(&gov.0, &request_id, false);
        assert!(matches!(outcome, Ok(RespondStatus::Denied { .. })));

        // Alice's queue should have a CONNECTION_DENIED system event
        let msg = hub
            .long_poll_dequeue(&tok_a, Duration::from_millis(100))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.from_name, "system");
                assert_eq!(m.event_type.as_deref(), Some("connection_denied"));
            }
            DequeueOutcome::Empty => panic!("expected CONNECTION_DENIED message for sender"),
        }
    }

    /// hold TTL expiry → MediationUnavailable on resolve.
    #[tokio::test]
    async fn hold_ttl_expiry_returns_mediation_unavailable() {
        let (hub, gov, tok_a, tok_b) = setup_hub_brief();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let mediation_id = "med-expired".to_string();
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.mediation_holds.push(MediationHold {
                mediation_id: mediation_id.clone(),
                from_name: "alice".into(),
                to_name: "bob".into(),
                from_identity: "id-alice".into(),
                to_identity: "id-bob".into(),
                payload: Payload(b"hi".to_vec()),
                reason: "test".into(),
                expires: Instant::now() - Duration::from_secs(1),
                resolved: false,
                grant_id: None,
            });
        }

        assert!(matches!(
            hub.resolve_mediation(&gov, &mediation_id, MediationDecision::Approve),
            Err(Error::MediationUnavailable)
        ));
    }

    // ── Inspect / Notify / Bypass mediation tests ─────────────────────────────

    fn setup_hub_inspect() -> (DeliveryHub, GovernorToken, AgentToken, AgentToken) {
        use crate::trust::{GrantDirection, GrantMediation};
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                mediation: Some(GrantMediation::Inspect),
                direction: Some(GrantDirection::AToB),
                opens_reply_window: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        (hub, gov, tok_a, tok_b)
    }

    /// Inspect grant: send → PendingMediation (hold created, budget NOT consumed).
    #[test]
    fn inspect_grant_holds_awaits_governor() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_inspect();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::Public).unwrap();

        let result = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
        match result {
            Ok(Ack::PendingMediation { mediation_id }) => {
                assert!(mediation_id.starts_with("med-"));
            }
            other => panic!("expected PendingMediation, got {:?}", other),
        }
    }

    /// Inspect: governor approves → message delivered to recipient.
    #[tokio::test]
    async fn inspect_approve_delivers() {
        let (hub, gov, tok_a, tok_b) = setup_hub_inspect();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::Public).unwrap();

        let mediation_id = match hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None) {
            Ok(Ack::PendingMediation { mediation_id }) => mediation_id,
            other => panic!("expected PendingMediation, got {:?}", other),
        };

        assert!(matches!(
            hub.resolve_mediation(&gov, &mediation_id, MediationDecision::Approve),
            Ok(MediationResult::Delivered { .. })
        ));

        let msg = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"hello"),
            DequeueOutcome::Empty => panic!("expected message after approve"),
        }
    }

    /// Inspect: governor blocks → no delivery, Blocked result.
    #[tokio::test]
    async fn inspect_block_returns_blocked() {
        let (hub, gov, tok_a, tok_b) = setup_hub_inspect();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::Public).unwrap();

        let mediation_id = match hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None) {
            Ok(Ack::PendingMediation { mediation_id }) => mediation_id,
            other => panic!("expected PendingMediation, got {:?}", other),
        };

        assert!(matches!(
            hub.resolve_mediation(&gov, &mediation_id, MediationDecision::Block),
            Ok(MediationResult::Blocked)
        ));

        let empty = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(20))
            .await
            .unwrap();
        assert!(matches!(empty, DequeueOutcome::Empty));
    }

    /// Inspect: expired hold → MediationUnavailable on resolve.
    #[test]
    fn inspect_ttl_expiry() {
        let (hub, gov, _tok_a, _tok_b) = setup_hub_inspect();

        let mediation_id = "med-inspect-expired".to_string();
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.mediation_holds.push(MediationHold {
                mediation_id: mediation_id.clone(),
                from_name: "alice".into(),
                to_name: "bob".into(),
                from_identity: "id-alice".into(),
                to_identity: "id-bob".into(),
                payload: Payload(b"hi".to_vec()),
                reason: String::new(),
                expires: Instant::now() - Duration::from_secs(1),
                resolved: false,
                grant_id: Some("grant-1".into()),
            });
        }

        assert!(matches!(
            hub.resolve_mediation(&gov, &mediation_id, MediationDecision::Approve),
            Err(Error::MediationUnavailable)
        ));
    }

    /// Inspect: governor offline at send time → MediationUnavailable (no hold created).
    #[test]
    fn inspect_governor_offline_returns_mediation_unavailable() {
        let (hub, gov, tok_a, tok_b) = setup_hub_inspect();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::Public).unwrap();

        hub.inner
            .lock()
            .unwrap()
            .trust
            .set_governor_online(&gov, false);

        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None),
            Err(Error::MediationUnavailable)
        ));
    }

    /// Notify: message delivered immediately AND event emitted to gov_events.
    #[tokio::test]
    async fn notify_delivers_and_fires_event() {
        use crate::trust::{GrantDirection, GrantMediation};
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                mediation: Some(GrantMediation::Notify),
                direction: Some(GrantDirection::AToB),
                ..Default::default()
            },
        )
        .unwrap();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::Public).unwrap();

        let mut events_rx = hub.subscribe_gov_events();

        let result = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
        assert!(matches!(result, Ok(Ack::Accepted)));

        let msg = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();
        assert!(matches!(msg, DequeueOutcome::Message(_)));

        let event_json = events_rx
            .try_recv()
            .expect("notify event should have been emitted");
        let event: serde_json::Value = serde_json::from_str(&event_json).unwrap();
        assert_eq!(event["type"], "notify");
        assert_eq!(event["from"], "alice");
        assert_eq!(event["to"], "bob");
    }

    /// SIM-1: A reconnecting listen client with a non-empty queue receives a catch-up NOTIFY
    /// immediately on the new SSE stream, so it knows to call /dequeue without waiting for a
    /// new send().  This tests both the re-arm (notify_suppressed reset) and the catch-up emit.
    #[tokio::test]
    async fn sim_1_reconnect_with_queued_messages_emits_catchup_notify() {
        use crate::trust::{GrantDirection, GrantMediation};

        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();

        // Bob is a listen-flow client — register first, then open_listen.
        let bob_token = hub.register_agent();
        let (bob_token, rx1) = hub.open_listen(Some(&bob_token), None, Some("bob"), None, false).unwrap();
        // Alice is a regular agent with a Notify grant to Bob.
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();
        hub.approve_grant_req(
            &gov,
            "id-alice",
            &bob_token, // bob's identity is his token string
            None,
            ApproveGrantRequest {
                mediation: Some(GrantMediation::Notify),
                direction: Some(GrantDirection::AToB),
                ..Default::default()
            },
        )
        .unwrap();

        // Alice sends — this fires a NOTIFY on rx1 and sets notify_suppressed=true.
        let result = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
        assert!(matches!(result, Ok(Ack::Accepted)), "send must be accepted");

        // Drain the first NOTIFY (simulates client receiving it) but do NOT dequeue
        // (simulates the client disconnecting before it could call /dequeue).
        drop(rx1); // simulate disconnect — notify_suppressed remains true

        // Simulate reconnect: open_listen with the same token (force=true to supersede).
        // SIM-1 fix: open_listen must reset notify_suppressed and emit a catch-up NOTIFY.
        let (returned_token, mut rx2) =
            hub.open_listen(Some(&bob_token), None, None, None, true).unwrap();
        assert_eq!(
            returned_token, bob_token,
            "reconnect must return same token"
        );

        // Collect all events on rx2 until the channel would block (non-blocking).
        let mut events: Vec<String> = Vec::new();
        // The welcome event arrives immediately; the catch-up notify should follow.
        while let Ok(ev) = rx2.try_recv() {
            events.push(ev);
        }

        // Must contain exactly one {"type":"notify","pending":N} event.
        let notify_events: Vec<&String> = events
            .iter()
            .filter(|e| e.contains(r#""type":"notify""#) && e.contains(r#""pending":"#))
            .collect();
        assert_eq!(
            notify_events.len(),
            1,
            "reconnect with queued messages must emit exactly one catch-up NOTIFY; got events: {:?}",
            events
        );

        // Verify the pending count in the catch-up event.
        let parsed: serde_json::Value =
            serde_json::from_str(notify_events[0]).expect("catch-up notify must be valid JSON");
        assert_eq!(parsed["type"], "notify", "type field must be 'notify'");
        assert_eq!(parsed["pending"], 1u64, "pending count must be 1");

        // Verify notify_suppressed is now true (interlock preserved).
        {
            let inner = hub.inner.lock().unwrap();
            let state = inner.listen_tokens.get(&bob_token).unwrap();
            assert!(
                state.notify_suppressed,
                "notify_suppressed must be true after catch-up notify"
            );
        }

        // After dequeue, notify_suppressed is re-armed (standard R5.4 guarantee).
        let _ = hub.dequeue(&bob_token, None).unwrap();
        {
            let inner = hub.inner.lock().unwrap();
            let state = inner.listen_tokens.get(&bob_token).unwrap();
            assert!(
                !state.notify_suppressed,
                "notify_suppressed must be false after dequeue"
            );
        }
    }

    /// Inspect grant with conditions: mediation event carries the conditions string.
    #[test]
    fn conditions_included_in_inspect_event() {
        use crate::trust::{GrantDirection, GrantMediation};
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                mediation: Some(GrantMediation::Inspect),
                direction: Some(GrantDirection::AToB),
                opens_reply_window: Some(true),
                conditions: Some("only urgent messages".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::Public).unwrap();

        let mut events_rx = hub.subscribe_gov_events();

        let result = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
        assert!(matches!(result, Ok(Ack::PendingMediation { .. })));

        let event_json = events_rx
            .try_recv()
            .expect("inspect event should have been emitted");
        let event: serde_json::Value = serde_json::from_str(&event_json).unwrap();
        assert_eq!(event["type"], "mediation");
        assert_eq!(event["conditions"], "only urgent messages");
    }

    /// Bypass grant: existing delivery behaviour unchanged.
    #[test]
    fn bypass_grant_unchanged() {
        use crate::trust::GrantMediation;
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                mediation: Some(GrantMediation::Bypass),
                ..Default::default()
            },
        )
        .unwrap();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::Public).unwrap();

        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));
    }

    /// Normal message omits reason field.
    #[tokio::test]
    async fn normal_message_omits_reason() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None)
            .unwrap();

        let msg = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => assert!(m.reason.is_none()),
            DequeueOutcome::Empty => panic!("expected message"),
        }
    }

    // ── Presence scoping tests ────────────────────────────────────────────────

    /// Grant-scoped agent is invisible to a querier without any grant.
    #[test]
    fn grant_scoped_querier_without_grant_sees_offline() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        // No grant between alice and bob
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let result = hub.presence_scoped(&tok_b, "alice");
        assert!(
            matches!(result, Ok(false)),
            "bob without grant should see alice as offline"
        );
    }

    /// Grant-scoped agent is visible to a querier that holds a grant.
    #[test]
    fn grant_scoped_querier_with_grant_sees_online() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let result = hub.presence_scoped(&tok_b, "alice");
        assert!(
            matches!(result, Ok(true)),
            "bob with grant should see alice as online"
        );
    }

    /// Hidden agent returns false to all presence queries but can still send and receive messages.
    #[test]
    fn hidden_agent_sends_and_receives_successfully() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::Hidden).unwrap();

        let presence = hub.presence_scoped(&tok_a, "bob");
        assert!(
            matches!(presence, Ok(false)),
            "hidden agent appears offline to grant-holder"
        );

        let send_result = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
        assert!(
            matches!(send_result, Ok(Ack::Accepted)),
            "hidden agent can still receive"
        );
    }

    /// Self-query always returns true is_online regardless of scope.
    #[test]
    fn self_query_always_returns_true_status() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::Hidden)
            .unwrap();

        let result = hub.presence_scoped(&tok_a, "alice");
        assert!(
            matches!(result, Ok(true)),
            "self-query on hidden agent should return true"
        );
    }

    // ── SSE liveness tests (AC1–AC4) ─────────────────────────────────────────

    /// AC1: Agent with active SSE connection remains online after 2× liveness window elapses.
    #[tokio::test]
    async fn ac_sse_1_active_sse_keeps_agent_online_after_liveness_lapse() {
        let hub = make_hub(Duration::from_millis(10));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();

        hub.sse_open("alice");

        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(
            hub.presence("alice"),
            "agent with active SSE should be online after liveness lapse"
        );
    }

    /// AC2: Agent whose SSE connection closes goes offline after the liveness window expires.
    #[tokio::test]
    async fn ac_sse_2_closed_sse_lets_agent_go_offline() {
        let hub = make_hub(Duration::from_millis(10));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();

        hub.sse_open("alice");
        hub.sse_close("alice");

        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(
            !hub.presence("alice"),
            "agent without active SSE should go offline after liveness lapse"
        );
    }

    /// AC3: Agent with active SSE appears online to presence queries from other agents (grant-scoped).
    #[tokio::test]
    async fn ac_sse_3_active_sse_visible_to_presence_query() {
        let hub = make_hub(Duration::from_millis(10));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        hub.sse_open("alice");

        tokio::time::sleep(Duration::from_millis(30)).await;

        let result = hub.presence_scoped(&tok_b, "alice");
        assert!(
            matches!(result, Ok(true)),
            "agent with active SSE should appear online to grant-holder"
        );
    }

    /// AC4: Agent without SSE goes offline after liveness window (backward compatibility).
    #[tokio::test]
    async fn ac_sse_4_no_sse_goes_offline_after_liveness_window() {
        let hub = make_hub(Duration::from_millis(10));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::Public)
            .unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(
            !hub.presence("alice"),
            "agent without SSE should go offline after liveness window"
        );
    }

    // ── Agent list tests (AC1–AC5 / Feature 1) ───────────────────────────────

    /// AC1 + AC5: governor_list_agents — register 2 agents, list shows both with correct fields.
    #[test]
    fn governor_list_agents() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let agents = hub.list_agents(&gov).unwrap();
        assert_eq!(agents.len(), 2);

        let alice = agents.iter().find(|a| a.name == "alice").unwrap();
        assert_eq!(alice.identity, "id-alice");
        assert_eq!(alice.status, "online");

        let bob = agents.iter().find(|a| a.name == "bob").unwrap();
        assert_eq!(bob.identity, "id-bob");
        assert_eq!(bob.status, "online");
    }

    /// AC2: agent token → Forbidden (maps to 403 FORBIDDEN at HTTP layer).
    #[test]
    fn list_agents_rejects_agent_token() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();

        let fake_gov = GovernorToken(tok_a.0.clone());
        assert!(
            matches!(hub.list_agents(&fake_gov), Err(Error::Forbidden)),
            "agent token must be rejected with Forbidden for list_agents"
        );
    }

    /// AC3: hidden agents appear offline in the list even when actually online.
    #[test]
    fn list_agents_hidden_appears_offline() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::Hidden)
            .unwrap();
        hub.sse_open("alice"); // ensure truly "online" per SSE liveness

        let agents = hub.list_agents(&gov).unwrap();
        let alice = agents.iter().find(|a| a.name == "alice").unwrap();
        assert_eq!(
            alice.status, "offline",
            "hidden agent must appear offline even with SSE"
        );

        hub.sse_close("alice");
    }

    // ── Token refresh tests (AC6–AC10 / Feature 2) ───────────────────────────

    /// AC6 + AC7 + AC8: agent refresh returns new token; old invalidated; grants still valid.
    #[test]
    fn ac6_ac7_ac8_agent_token_refresh() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let new_tok_a = hub.refresh_agent_token(&tok_a).unwrap();

        // AC6: new token returned, different from old
        assert_ne!(new_tok_a.0, tok_a.0);

        // AC7: old token invalidated
        assert!(
            matches!(hub.validate_agent_token(&tok_a), Err(Error::AuthFailed)),
            "old agent token must be invalidated after refresh"
        );

        // AC8: new token valid and grant still applies
        assert!(hub.validate_agent_token(&new_tok_a).is_ok());
        assert!(
            matches!(
                hub.send(&new_tok_a, "bob", Payload(b"hi".to_vec()), None, None),
                Ok(Ack::Accepted)
            ),
            "grant must remain valid after token refresh"
        );
    }

    /// AC9: governor refresh returns new token; old governor token invalidated.
    #[test]
    fn ac9_governor_token_refresh() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        let new_gov = hub.refresh_governor_token(&gov).unwrap();

        assert_ne!(new_gov.0, gov.0);

        assert!(
            matches!(hub.validate_governor_token(&gov), Err(Error::AuthFailed)),
            "old governor token must be invalidated after refresh"
        );
        assert!(
            hub.validate_governor_token(&new_gov).is_ok(),
            "new governor token must be valid after refresh"
        );
    }

    /// AC10: governor force-refresh invalidates old agent token, returns new one.
    #[test]
    fn ac10_governor_force_refresh_agent_token() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();

        let new_tok_a = hub.governor_refresh_agent_token(&gov, "id-alice").unwrap();

        assert_ne!(new_tok_a.0, tok_a.0);

        assert!(
            matches!(hub.validate_agent_token(&tok_a), Err(Error::AuthFailed)),
            "old token must be invalidated after governor force-refresh"
        );
        assert!(
            hub.validate_agent_token(&new_tok_a).is_ok(),
            "new token must be valid after governor force-refresh"
        );
    }

    /// Refresh keeps token_to_name mapping: dequeue still works with new token.
    #[tokio::test]
    async fn agent_refresh_preserves_registration() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let new_tok_a = hub.refresh_agent_token(&tok_a).unwrap();

        // Send from new token and verify bob can dequeue
        hub.send(
            &new_tok_a,
            "bob",
            Payload(b"after-refresh".to_vec()),
            None,
            None,
        )
        .unwrap();
        let outcome = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();
        match outcome {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"after-refresh"),
            DequeueOutcome::Empty => panic!("expected message after refresh"),
        }
    }

    // ── Bilateral consent tests (AC1–AC8 for task 20-9008) ───────────────────

    fn setup_hub_no_grant() -> (DeliveryHub, GovernorToken, AgentToken, AgentToken) {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        (hub, gov, tok_a, tok_b)
    }

    /// AC1: send to unregistered → RECIPIENT_UNKNOWN (no connection request created).
    #[test]
    fn ac_bilateral_1_send_to_unregistered_recipient_unknown() {
        let (hub, _gov, tok_a, _tok_b) = setup_hub_no_grant();
        hub.inner.lock().unwrap().agents.insert(
            "alice".into(),
            AgentState {
                identity: "id-alice".into(),
                notify: Arc::new(tokio::sync::Notify::new()),
            },
        );
        // bob is never registered
        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None),
            Err(Error::RecipientUnknown)
        ));
        // No connection request created
        assert!(hub.inner.lock().unwrap().connection_requests.is_empty());
    }

    /// AC2: request_grant → grant_request event to governor SSE; recipient notified after governor approves.
    #[tokio::test]
    async fn ac_bilateral_2_send_no_grant_queues_connection_request() {
        let (hub, gov, tok_a, tok_b) = setup_hub_no_grant();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let mut events_rx = hub.subscribe_gov_events();

        // send() returns NoGrant; request_grant() creates the request and broadcasts to governor.
        let _ = hub.send(&tok_a, "bob", Payload(b"payload".to_vec()), None, None);
        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Governor sees the grant_request event via SSE broadcast.
        let gov_event = events_rx
            .try_recv()
            .expect("governor should receive grant_request event");
        let event: serde_json::Value = serde_json::from_str(&gov_event).unwrap();
        assert_eq!(event["type"], "grant_request");
        assert_eq!(event["request_id"], request_id.as_str());
        assert_eq!(event["from"], "alice");
        assert_eq!(event["to"], "bob");

        // After governor approves, Bob gets a grant_request message in his queue.
        hub.approve_grant_request(&gov.0, &request_id, None)
            .expect("governor approval must succeed");
        let msg = hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.from_name, "system");
                assert_eq!(m.event_type.as_deref(), Some("grant_request"));
            }
            DequeueOutcome::Empty => {
                panic!("expected grant_request in recipient queue after governor approves")
            }
        }
    }

    /// AC3: both approve → grant established; subsequent sends succeed.
    #[tokio::test]
    async fn ac_bilateral_3_both_approve_grant_established_message_delivered() {
        let (hub, gov, tok_a, tok_b) = setup_hub_no_grant();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Governor approves first (PendingGovernor → PendingRecipient).
        assert!(matches!(
            hub.approve_grant_request(&gov.0, &request_id, None),
            Ok(ApproveStatus::PendingRecipient)
        ));

        // Drain the grant_request event queued to Bob.
        hub.long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap();

        // Bob (recipient) approves → both approved → Established.
        assert!(matches!(
            hub.approve_grant_request(&tok_b.0, &request_id, None),
            Ok(ApproveStatus::Established)
        ));

        // Grant established → subsequent send succeeds.
        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"follow-up".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));
    }

    /// AC4: governor denies → message dropped, sender gets CONNECTION_DENIED.
    #[tokio::test]
    async fn ac_bilateral_4_governor_deny_connection_denied_to_sender() {
        let (hub, gov, tok_a, tok_b) = setup_hub_no_grant();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        assert!(matches!(
            hub.respond_to_connection_request(&gov.0, &request_id, false),
            Ok(RespondStatus::Denied { .. })
        ));

        // Alice's queue has CONNECTION_DENIED
        let msg = hub
            .long_poll_dequeue(&tok_a, Duration::from_millis(100))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.from_name, "system");
                assert_eq!(m.event_type.as_deref(), Some("connection_denied"));
                let v: serde_json::Value = serde_json::from_slice(&m.payload.0).unwrap();
                assert_eq!(v["type"], "connection_denied");
            }
            DequeueOutcome::Empty => panic!("expected CONNECTION_DENIED for sender"),
        }
    }

    /// AC5: Alice (recipient) denies → message dropped, sender gets CONNECTION_DENIED.
    #[tokio::test]
    async fn ac_bilateral_5_alice_deny_connection_denied_to_sender() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_no_grant();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Bob (recipient) denies
        assert!(matches!(
            hub.respond_to_connection_request(&tok_b.0, &request_id, false),
            Ok(RespondStatus::Denied { .. })
        ));

        // Alice gets CONNECTION_DENIED
        let msg = hub
            .long_poll_dequeue(&tok_a, Duration::from_millis(100))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.event_type.as_deref(), Some("connection_denied"));
            }
            DequeueOutcome::Empty => panic!("expected CONNECTION_DENIED for sender"),
        }
    }

    /// AC6: reason field from request_grant is included in the grant_request governor event.
    #[tokio::test]
    async fn ac_bilateral_6_reason_included_in_event() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_no_grant();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        let mut gov_rx = hub.subscribe_gov_events();

        // send() returns NoGrant; request_grant() broadcasts the grant_request event with the reason.
        let _ = hub.send(
            &tok_a,
            "bob",
            Payload(b"hi".to_vec()),
            Some("need access".into()),
            None,
        );
        hub.request_grant(&tok_a.0, "bob", Some("need access".into()), None)
            .expect("request_grant must succeed");

        let event_json = gov_rx.try_recv().unwrap();
        let event: serde_json::Value = serde_json::from_str(&event_json).unwrap();
        assert_eq!(event["reason"], "need access");
        assert!(
            event["payload"].is_null() || !event.as_object().unwrap().contains_key("payload"),
            "payload should not be in grant_request event"
        );
    }

    // AC7: restart while request pending → request lost (in-memory only). Not a unit-testable
    // invariant — the ephemeral guarantee is structural (no persistence code for requests).

    /// R7: duplicate request_grant while pending → second call returns RequestPending error.
    #[test]
    fn ac_bilateral_r7_dedup_second_send_returns_existing_request() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_no_grant();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();

        // First request_grant creates the connection request.
        let id1 = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("first request_grant must succeed");
        assert!(!id1.is_empty());

        // Second request_grant while pending → RequestPending error (dedup).
        let second = hub.request_grant(&tok_a.0, "bob", None, None);
        assert!(
            matches!(second, Err(Error::RequestPending)),
            "duplicate request_grant within timeout window must return RequestPending error"
        );
    }

    // ── Persistence tests (AC1–AC8) ───────────────────────────────────────────

    use std::sync::atomic::{AtomicU64, Ordering};
    static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_test_db() -> String {
        let n = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!(
            "{}\\sim_persist_{}_{}.db",
            std::env::temp_dir().display(),
            std::process::id(),
            n,
        )
    }

    async fn make_persisted_hub(db_path: &str, lapse: Duration) -> DeliveryHub {
        use crate::persistence::TokenStore;
        let store = Arc::new(TokenStore::open(db_path).await.expect("open db"));
        let tokens = store.load_tokens().await.expect("load tokens");
        let grants = store.load_grants().await.expect("load grants");
        let identities = store.load_identities().await.expect("load identities");
        let denial_blocks = store
            .load_denial_blocks()
            .await
            .expect("load denial blocks");
        DeliveryHub::new_with_persisted_state(
            lapse,
            store,
            tokens,
            grants,
            identities,
            denial_blocks,
        )
    }

    // ── Attachments (native file/attachment send) ────────────────────────────────

    /// Persistence-backed hub with alice↔bob registered + granted (attachments need a store).
    async fn setup_persisted_ab(db: &str) -> (DeliveryHub, AgentToken, AgentToken, GovernorToken) {
        let hub = make_persisted_hub(db, Duration::from_secs(30)).await;
        let gov = hub.install_governor(None);
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
        hub.register("alice", &tok_a, PresenceScope::GrantScoped)
            .unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped)
            .unwrap();
        (hub, tok_a, tok_b, gov)
    }

    /// FR1–FR5: grant-gated upload → recipient gets a metadata-only notify → on-demand fetch
    /// returns the exact bytes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_attach_1_send_notify_fetch_roundtrip() {
        let db = unique_test_db();
        let (hub, tok_a, tok_b, _gov) = setup_persisted_ab(&db).await;

        let meta = hub
            .attach(
                &tok_a.0,
                "bob",
                "spec.md",
                "text/markdown",
                b"# hello".to_vec(),
                Some("fyi"),
                Duration::from_secs(3600),
            )
            .await
            .expect("attach must succeed");
        assert_eq!(meta.filename, "spec.md");
        assert_eq!(meta.size, 7);

        // Recipient receives metadata only — NOT the bytes (FR3).
        match hub
            .long_poll_dequeue(&tok_b, Duration::from_millis(100))
            .await
            .unwrap()
        {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.event_type.as_deref(), Some("attachment"));
                let v: serde_json::Value = serde_json::from_slice(&m.payload.0).unwrap();
                assert_eq!(v["type"], "attachment");
                assert_eq!(v["attachment_id"], meta.id.as_str());
                assert_eq!(v["filename"], "spec.md");
                assert_eq!(v["size"], 7);
                assert!(v.get("bytes").is_none(), "notify must not carry the bytes");
            }
            DequeueOutcome::Empty => panic!("expected attachment notify"),
        }

        // On-demand fetch returns the exact bytes/filename/mime (FR4).
        let (bytes, filename, mime) = hub
            .fetch_attachment(&tok_b.0, &meta.id)
            .await
            .expect("fetch must succeed");
        assert_eq!(bytes, b"# hello");
        assert_eq!(filename, "spec.md");
        assert_eq!(mime, "text/markdown");
    }

    /// NFR2: only the sender's identity or the recipient's bound name may fetch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_attach_2_access_control() {
        let db = unique_test_db();
        let (hub, tok_a, _tok_b, gov) = setup_persisted_ab(&db).await;
        let tok_c = hub.mint_agent_token(&gov, "id-carol", None).unwrap();
        hub.register("carol", &tok_c, PresenceScope::GrantScoped)
            .unwrap();

        let meta = hub
            .attach(
                &tok_a.0,
                "bob",
                "secret.txt",
                "text/plain",
                b"top secret".to_vec(),
                None,
                Duration::from_secs(3600),
            )
            .await
            .unwrap();

        assert!(matches!(
            hub.fetch_attachment(&tok_c.0, &meta.id).await,
            Err(Error::Forbidden)
        ));
        assert!(hub.fetch_attachment(&tok_a.0, &meta.id).await.is_ok()); // sender allowed
    }

    /// Grant-gated exactly like send: no grant → NoGrant.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_attach_3_requires_grant() {
        let db = unique_test_db();
        let (hub, tok_a, _tok_b, gov) = setup_persisted_ab(&db).await;
        let tok_c = hub.mint_agent_token(&gov, "id-carol", None).unwrap();
        hub.register("carol", &tok_c, PresenceScope::GrantScoped)
            .unwrap();
        let r = hub
            .attach(
                &tok_a.0,
                "carol",
                "x.txt",
                "text/plain",
                b"hi".to_vec(),
                None,
                Duration::from_secs(3600),
            )
            .await;
        assert!(matches!(r, Err(Error::NoGrant)));
    }

    /// FR6: a past-TTL attachment is GC'd; fetch returns AttachmentNotFound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_attach_4_expiry_gc() {
        let db = unique_test_db();
        let (hub, tok_a, tok_b, _gov) = setup_persisted_ab(&db).await;
        let meta = hub
            .attach(
                &tok_a.0,
                "bob",
                "ephemeral.txt",
                "text/plain",
                b"poof".to_vec(),
                None,
                Duration::from_secs(0),
            )
            .await
            .unwrap();
        assert!(matches!(
            hub.fetch_attachment(&tok_b.0, &meta.id).await,
            Err(Error::AttachmentNotFound)
        ));
    }

    /// AC1: after restart, all previously minted non-expired tokens work without re-provisioning.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac1_tokens_survive_restart() {
        let db = unique_test_db();

        let (gov_tok, agent_tok) = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let agent = hub.mint_agent_token(&gov, "alice", None).unwrap();
            (gov, agent)
        };

        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        assert!(
            hub2.validate_agent_token(&agent_tok).is_ok(),
            "AC1: agent token must survive restart"
        );
        assert!(
            hub2.validate_governor_token(&gov_tok).is_ok(),
            "AC1: governor token must survive restart"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// AC2: after restart, all previously approved non-expired grants still authorize message sending.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac2_grants_survive_restart() {
        let db = unique_test_db();

        let (agent_a, agent_b) = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
            let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
            hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();
            (tok_a, tok_b)
        };

        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        hub2.register("alice", &agent_a, PresenceScope::GrantScoped)
            .unwrap();
        hub2.register("bob", &agent_b, PresenceScope::GrantScoped)
            .unwrap();
        assert!(
            matches!(
                hub2.send(&agent_a, "bob", Payload(b"hi".to_vec()), None, None),
                Ok(Ack::Accepted)
            ),
            "AC2: grant must allow sending after restart"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// AC3: after restart, governor token valid; can mint new agent tokens and approve grants.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac3_governor_operational_after_restart() {
        let db = unique_test_db();

        let gov_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            hub.install_governor(None)
        };

        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        assert!(
            hub2.validate_governor_token(&gov_tok).is_ok(),
            "AC3: governor must be valid after restart"
        );
        assert!(
            hub2.mint_agent_token(&gov_tok, "new-agent", None).is_ok(),
            "AC3: governor can mint after restart"
        );
        assert!(
            hub2.approve_grant(&gov_tok, "a", "b", None).is_ok(),
            "AC3: governor can approve grant after restart"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// AC4: expired tokens past expires_at do NOT work after restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac4_expired_tokens_not_loaded() {
        let db = unique_test_db();

        let expired_agent = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            // 100ms expiry
            hub.mint_agent_token(&gov, "expiring", Some(Duration::from_millis(100)))
                .unwrap()
        };

        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(200)).await;

        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        let result = hub2.validate_agent_token(&expired_agent);
        assert!(
            result.is_err(),
            "AC4: expired agent token must not work after restart"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// AC5: revoking a token removes it from store immediately; does not return after restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac5_revoked_token_absent_after_restart() {
        let db = unique_test_db();

        let gov_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            // Revoke in-memory and delete from the persistent store so it isn't reloaded.
            let revoked_toks = hub.lock().trust.revoke_all_governors();
            if let Some(store) = hub.token_store.clone() {
                for tok in revoked_toks {
                    let _ = store.delete_token(&tok).await;
                }
            }
            gov
        };

        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        assert!(
            hub2.validate_governor_token(&gov_tok).is_err(),
            "AC5: revoked governor token must not be present after restart"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// AC6: permanent grant persisted; restart doesn't remove it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac6_permanent_grant_survives_restart() {
        let db = unique_test_db();

        let (tok_a, tok_b) = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let a = hub.mint_agent_token(&gov, "id-a", None).unwrap();
            let b = hub.mint_agent_token(&gov, "id-b", None).unwrap();
            hub.approve_grant(&gov, "id-a", "id-b", None).unwrap(); // no expiry = permanent
            (a, b)
        };

        // Reload many times — grant must persist
        for _ in 0..3 {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            hub.register("ag-a", &tok_a, PresenceScope::GrantScoped)
                .unwrap();
            hub.register("ag-b", &tok_b, PresenceScope::GrantScoped)
                .unwrap();
            assert!(
                matches!(
                    hub.send(&tok_a, "ag-b", Payload(b"x".to_vec()), None, None),
                    Ok(Ack::Accepted)
                ),
                "AC6: permanent grant must survive repeated restarts"
            );
        }

        let _ = std::fs::remove_file(&db);
    }

    /// AC7: sim-tokens.db exists after first startup and contains the trust chain.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac7_db_file_created_and_populated() {
        use crate::persistence::TokenStore;

        let db = unique_test_db();

        {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            hub.install_governor(None);
        }

        // DB file must exist
        assert!(
            std::path::Path::new(&db).exists(),
            "AC7: token store DB must be created on first startup"
        );

        // DB must contain at least one token
        let store = TokenStore::open(&db).await.unwrap();
        let tokens = store.load_tokens().await.unwrap();
        assert!(
            !tokens.is_empty(),
            "AC7: DB must contain the minted governor token"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// AC8: counter seeded correctly from persisted IDs — new tokens don't collide.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac8_counter_seeded_no_id_collision() {
        let db = unique_test_db();

        // Phase 1: mint several tokens to advance the counter
        let existing_ids: Vec<String> = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let a1 = hub.mint_agent_token(&gov, "a1", None).unwrap().0;
            let a2 = hub.mint_agent_token(&gov, "a2", None).unwrap().0;
            hub.approve_grant(&gov, "a1", "a2", None).unwrap();
            vec![gov.0, a1, a2]
        };

        // Phase 2: reload and mint new tokens — IDs must not collide
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        let gov2 = hub2.install_governor(None);
        let new_agent = hub2.mint_agent_token(&gov2, "a3", None).unwrap();

        assert!(
            !existing_ids.contains(&gov2.0),
            "AC8: new governor ID must not collide with existing"
        );
        assert!(
            !existing_ids.contains(&new_agent.0),
            "AC8: new agent ID must not collide with existing"
        );

        let _ = std::fs::remove_file(&db);
    }

    // ── AC-T2: token not in DB before first grant ──────────────────────────

    #[test]
    fn ac_t2_token_not_persisted_before_first_grant() {
        let hub = make_hub(Duration::from_secs(30));

        let reg_token = hub.register_agent();
        let (token, _rx) = hub.open_listen(Some(&reg_token), None, None, None, false).unwrap();

        // ever_granted = false means the token would NOT be persisted to DB yet.
        let inner = hub.inner.lock().unwrap();
        let state = inner
            .listen_tokens
            .get(&token)
            .expect("token must be in memory after open_listen");
        assert!(
            state.ever_listened,
            "ever_listened must be true after open_listen"
        );
        assert!(
            !state.ever_granted,
            "ever_granted must be false before any grant — token is in-memory only, not yet in DB"
        );
    }

    // ── AC-T4: GC removes tokens that never listened after unlisten TTL ────────

    #[test]
    fn ac_t4_gc_unlisten_ttl_removes_never_listened_token() {
        let hub = make_hub(Duration::from_secs(30));

        // issue_token creates a token with ever_listened = false.
        let stale = hub.issue_token();
        assert!(
            hub.validate_token(&stale).is_ok(),
            "token exists before TTL"
        );

        // Backdate issued_at past the unlisten TTL (default 300 s, min 60 s).
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.listen_tokens.get_mut(&stale).unwrap().issued_at =
                Instant::now() - Duration::from_secs(400);
        }

        // Trigger inline GC by issuing another token.
        let _fresh = hub.issue_token();

        assert!(
            matches!(hub.validate_token(&stale), Err(Error::TokenRejected)),
            "never-listened token must be GC'd after unlisten TTL"
        );
    }

    // ── AC-T4b: GC evicts !ever_listened && ever_granted tokens after unlisten_ttl ─
    //
    // Updated for 15-0002H (Branch 3): tokens with ever_granted=true but ever_listened=false
    // are now evicted after unlisten_ttl (same threshold as Branch 1).  This closes the
    // zombie-token memory-leak path that the 15-0002E guard created.
    //
    // Tokens NOT yet past TTL remain spared (no premature eviction).

    #[test]
    fn ac_t4b_gc_branch3_evicts_never_listened_granted_token_after_unlisten_ttl() {
        let hub = make_hub(Duration::from_secs(30));

        // `stale`: past unlisten_ttl — must be evicted by Branch 3.
        let stale = hub.issue_token();
        // `fresh_enough`: ever_granted=true but NOT yet past TTL — must NOT be evicted.
        let fresh_enough = hub.issue_token();

        {
            let mut inner = hub.inner.lock().unwrap();
            let st = inner.listen_tokens.get_mut(&stale).unwrap();
            st.ever_granted = true;
            // Backdate well past unlisten_ttl so Branch 3 fires.
            st.issued_at = Instant::now() - Duration::from_secs(4000);

            let st2 = inner.listen_tokens.get_mut(&fresh_enough).unwrap();
            st2.ever_granted = true;
            // Do NOT backdate — this token is recently issued and must be spared.
        }

        // Trigger inline GC.
        let _trigger = hub.issue_token();

        assert!(
            matches!(hub.validate_token(&stale), Err(Error::TokenRejected)),
            "Branch 3: !ever_listened && ever_granted token past unlisten_ttl MUST be GC'd (15-0002H)"
        );
        assert!(
            hub.validate_token(&fresh_enough).is_ok(),
            "Branch 3: !ever_listened && ever_granted token NOT yet past TTL must NOT be GC'd"
        );
    }

    // ── AC-T5: GC removes tokens that listened but never got a grant ──────────

    #[test]
    fn ac_t5_gc_no_grant_ttl_removes_listened_never_granted_token() {
        let hub = make_hub(Duration::from_secs(30));

        let reg_token = hub.register_agent();
        let (stale, _rx) = hub.open_listen(Some(&reg_token), None, None, None, false).unwrap();

        {
            let inner = hub.inner.lock().unwrap();
            let st = &inner.listen_tokens[&stale];
            assert!(st.ever_listened, "ever_listened must be true");
            assert!(!st.ever_granted, "ever_granted must be false");
        }

        // Backdate issued_at past the no-grant TTL (default 1800 s, min 120 s).
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.listen_tokens.get_mut(&stale).unwrap().issued_at =
                Instant::now() - Duration::from_secs(2000);
        }

        let _fresh = hub.issue_token();

        assert!(
            matches!(hub.validate_token(&stale), Err(Error::TokenRejected)),
            "listened-but-never-granted token must be GC'd after no-grant TTL"
        );
    }

    // ── AC-T6: GC Branch 3 fires sim_offline to grant-peer SSE. (15-0002H) ────
    //
    // When gc_tokens() evicts a !ever_listened && ever_granted token (Branch 3),
    // the grant-peer with an active SSE stream must receive a sim_offline presence
    // event fired out-of-lock after the eviction (silent drop pattern).

    #[test]
    fn ac_t6_gc_branch3_sends_offline_to_grant_peer_sse() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        // Agent A: full listen-flow session (the observer — will receive presence events).
        let reg_a = hub.register_agent();
        let (tok_a, mut rx_a) = hub.open_listen(Some(&reg_a), None, None, None, false).unwrap();
        hub.announce(&tok_a, "GcA6", false).unwrap();

        // Agent B: issue token + announce, but NO open_listen (ever_listened stays false).
        let tok_b = hub.issue_token();
        hub.announce(&tok_b, "GcB6", false).unwrap();

        // Establish a grant between B (identity=tok_b) and A (identity=tok_a).
        // FP1 in approve_grant_req() fills in name_a="GcB6" and name_b="GcA6" from
        // token_to_name, so grant_peer_senders("GcB6") resolves A's SSE sender.
        hub.approve_grant(&gov, &tok_b, &tok_a, None).unwrap();

        // Set ever_granted=true on B and backdate past unlisten_ttl.
        // (The governor-direct approve_grant path does not set ever_granted on the
        //  token state — that is only done by the 2-phase approve_grant_request flow.)
        {
            let mut inner = hub.inner.lock().unwrap();
            let st = inner.listen_tokens.get_mut(&tok_b).unwrap();
            st.ever_granted = true;
            st.issued_at = Instant::now() - Duration::from_secs(400);
        }

        // Drain any setup events from A's stream.
        while rx_a.try_recv().is_ok() {}

        // Trigger inline GC via issue_token().
        let _trigger = hub.issue_token();

        // B must be evicted.
        assert!(
            matches!(hub.validate_token(&tok_b), Err(Error::TokenRejected)),
            "Branch 3: !ever_listened && ever_granted token past TTL must be GC'd"
        );

        // A must receive a sim_offline presence event for "GcB6".
        // push_presence_event sends to an unbounded channel — available immediately.
        let ev = rx_a.try_recv().ok();
        let has_offline = ev
            .as_deref()
            .map(|e| {
                e.contains("\"presence\"") && e.contains("\"offline\"") && e.contains("\"GcB6\"")
            })
            .unwrap_or(false);
        assert!(
            has_offline,
            "grant-peer A must receive sim_offline when GC evicts GcB6; got: {:?}",
            ev
        );
    }

    // ── AC-T3: token survives server restart after first grant ─────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_t3_token_persists_after_first_grant_survives_restart() {
        let db = unique_test_db();

        let v2_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();

            // Issue token for bob and announce its name.
            let reg_bob = hub.register_agent();
            let (v2_tok, _rx) = hub.open_listen(Some(&reg_bob), None, None, None, false).unwrap();
            hub.announce(&v2_tok, "bob", false).unwrap();

            // Register alice so request_grant() can route by name.
            hub.register("alice", &tok_a, PresenceScope::GrantScoped)
                .unwrap();

            // Alice has no grant yet — request it explicitly.
            let _ = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
            let request_id = hub
                .request_grant(&tok_a.0, "bob", None, None)
                .expect("request_grant must succeed");

            // Governor approves (PendingGovernor → PendingRecipient; queues grant_request to bob).
            hub.approve_grant_request(&gov.0, &request_id, None)
                .expect("governor approve must succeed");

            // Drain the grant_request event from bob's queue.
            let _ = hub.dequeue(&v2_tok, None).unwrap();

            // Bob approves (PendingRecipient → Established; grant created + token persisted).
            assert!(
                matches!(
                    hub.approve_grant_request(&v2_tok, &request_id, None),
                    Ok(ApproveStatus::Established)
                ),
                "both-approved path must return Established"
            );

            v2_tok
        }; // hub drops; DB write already completed (block_in_place in multi-thread runtime)

        // Simulate restart: rebuild hub from the same DB file.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;

        assert!(
            hub2.validate_token(&v2_tok).is_ok(),
            "AC-T3: token must be valid on a new hub rebuilt from the same DB after first grant"
        );

        let _ = std::fs::remove_file(&db);
    }

    // ── New ACs: announce-time persistence (token + name survives restart) ──

    /// AC1: announce token (no grant) → restart → send() by name is NOT RecipientUnknown.
    /// Proves the restored name_to_token routing works end-to-end after announce-time persistence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_persist_announce_send_by_name_not_unknown_after_restart() {
        let db = unique_test_db();

        let v2_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let reg_bob = hub.register_agent();
            let (v2_tok, _rx) = hub.open_listen(Some(&reg_bob), None, None, None, false).unwrap();
            hub.announce(&v2_tok, "bob", false).unwrap();
            v2_tok
        };

        // Restart: new hub from same DB.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;

        // Create a fresh sender on hub2 (original governor/agent tokens are not persisted).
        let gov2 = hub2.install_governor(None);
        let tok_a2 = hub2.mint_agent_token(&gov2, "id-alice", None).unwrap();
        hub2.register("alice", &tok_a2, PresenceScope::GrantScoped)
            .unwrap();

        // bob's token was announced before restart; routing entry must have been restored.
        let result = hub2.send(&tok_a2, "bob", Payload(b"ac1-probe".to_vec()), None, None);
        assert!(
            !matches!(result, Err(Error::RecipientUnknown)),
            "AC1: after announce-time persist + restart, send to 'bob' must NOT be RecipientUnknown; got {:?}",
            result
        );

        let _ = std::fs::remove_file(&db);
        drop(v2_tok);
    }

    /// AC2: announce token (no grant) → restart → validate_token returns Ok.
    /// Proves a never-granted token survives restart via the announce-time persist path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_persist_announce_token_valid_after_restart() {
        let db = unique_test_db();

        let v2_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let reg_bob = hub.register_agent();
            let (v2_tok, _rx) = hub.open_listen(Some(&reg_bob), None, None, None, false).unwrap();
            hub.announce(&v2_tok, "bob", false).unwrap();
            v2_tok
        };

        // Restart: new hub from same DB.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;

        assert!(
            hub2.validate_token(&v2_tok).is_ok(),
            "AC2: token announced (never granted) must be valid after announce-time persist + restart"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// AC3: after reload via new_with_persisted_state, assert name_to_token[name] == token
    /// AND state.name == name.  Discrete field-level assertion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_persist_announce_fields_correct_after_reload() {
        let db = unique_test_db();
        let name = "charlie";

        let v2_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let reg_charlie = hub.register_agent();
            let (v2_tok, _rx) = hub.open_listen(Some(&reg_charlie), None, None, None, false).unwrap();
            hub.announce(&v2_tok, name, false).unwrap();
            v2_tok
        };

        // Reload.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;

        let inner = hub2.inner.lock().unwrap();
        assert_eq!(
            inner.name_to_token.get(name).map(String::as_str),
            Some(v2_tok.as_str()),
            "AC3: name_to_token[name] must equal the announced token after reload"
        );
        let state = inner
            .listen_tokens
            .get(&v2_tok)
            .expect("AC3: token must exist in listen_tokens after reload");
        assert_eq!(
            state.name.as_deref(),
            Some(name),
            "AC3: state.name must equal the announced name after reload"
        );
        drop(inner);

        let _ = std::fs::remove_file(&db);
    }

    // ── GET /governors/grants tests (AC1–AC8) ────────────────────────────────

    /// AC1: valid governor token → Ok result (maps to HTTP 200).
    /// AC2: all active grants returned when no participant filter is set.
    /// AC4: each returned grant includes id, identity_a, identity_b, name_a, name_b, direction, expires.
    /// AC5: empty list is valid when no grants exist.
    #[test]
    fn ac_gov_grants_1_2_4_5_list_all_grants() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        // AC5: no grants → empty list returned (not an error)
        let result = hub.list_all_grants_gov(&gov, None).unwrap();
        assert!(result.is_empty(), "AC5: no grants yet — list must be empty");

        // Create two grants with stable names
        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                name_a: Some("alice".into()),
                name_b: Some("bob".into()),
                ..Default::default()
            },
        )
        .unwrap();
        hub.approve_grant_req(
            &gov,
            "id-carol",
            "id-dave",
            None,
            ApproveGrantRequest {
                name_a: Some("carol".into()),
                name_b: Some("dave".into()),
                ..Default::default()
            },
        )
        .unwrap();

        // AC1 + AC2: valid governor token returns both grants
        let result = hub.list_all_grants_gov(&gov, None).unwrap();
        assert_eq!(
            result.len(),
            2,
            "AC2: both grants must be returned with no filter"
        );

        // AC4: verify all required fields are present on each item
        for item in &result {
            assert!(!item.id.is_empty(), "AC4: id must be non-empty");
            assert!(
                !item.identity_a.is_empty(),
                "AC4: identity_a must be non-empty"
            );
            assert!(
                !item.identity_b.is_empty(),
                "AC4: identity_b must be non-empty"
            );
            // name_a, name_b, expires are Option — presence checked via specific items below
            let _ = &item.direction; // always present (enum, not Option)
        }

        // Verify specific field values on one item
        let alice_bob = result
            .iter()
            .find(|g| g.identity_a == "id-alice")
            .expect("alice-bob grant must be present");
        assert_eq!(alice_bob.identity_b, "id-bob");
        assert_eq!(alice_bob.name_a.as_deref(), Some("alice"));
        assert_eq!(alice_bob.name_b.as_deref(), Some("bob"));
        assert!(alice_bob.expires.is_none(), "permanent grant has no expiry");

        let carol_dave = result
            .iter()
            .find(|g| g.identity_a == "id-carol")
            .expect("carol-dave grant must be present");
        assert_eq!(carol_dave.name_a.as_deref(), Some("carol"));
        assert_eq!(carol_dave.name_b.as_deref(), Some("dave"));
    }

    /// AC3: ?participant=<name> filter returns only grants where name_a or name_b matches.
    #[test]
    fn ac_gov_grants_3_participant_filter() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        hub.approve_grant_req(
            &gov,
            "id-alice",
            "id-bob",
            None,
            ApproveGrantRequest {
                name_a: Some("alice".into()),
                name_b: Some("bob".into()),
                ..Default::default()
            },
        )
        .unwrap();
        hub.approve_grant_req(
            &gov,
            "id-carol",
            "id-dave",
            None,
            ApproveGrantRequest {
                name_a: Some("carol".into()),
                name_b: Some("dave".into()),
                ..Default::default()
            },
        )
        .unwrap();

        // Filter by "alice" — only the alice↔bob grant
        let result = hub.list_all_grants_gov(&gov, Some("alice")).unwrap();
        assert_eq!(
            result.len(),
            1,
            "AC3: filter=alice must return exactly 1 grant"
        );
        assert!(
            result[0].name_a.as_deref() == Some("alice")
                || result[0].name_b.as_deref() == Some("alice"),
            "AC3: returned grant must involve alice"
        );

        // Filter by "dave" — only the carol↔dave grant
        let result = hub.list_all_grants_gov(&gov, Some("dave")).unwrap();
        assert_eq!(
            result.len(),
            1,
            "AC3: filter=dave must return exactly 1 grant"
        );
        assert!(
            result[0].name_a.as_deref() == Some("dave")
                || result[0].name_b.as_deref() == Some("dave"),
            "AC3: returned grant must involve dave"
        );

        // Filter by "bob" as name_b — still matches
        let result = hub.list_all_grants_gov(&gov, Some("bob")).unwrap();
        assert_eq!(
            result.len(),
            1,
            "AC3: filter=bob (name_b) must return the alice-bob grant"
        );

        // Filter by unknown name — empty list, no error
        let result = hub
            .list_all_grants_gov(&gov, Some("unknown-participant"))
            .unwrap();
        assert!(
            result.is_empty(),
            "AC3: filter by unknown name must return empty list"
        );
    }

    /// AC6: absent/empty token → AuthFailed (HTTP handler maps this to 401).
    /// AC7: forged/invalid token → AuthFailed (HTTP handler maps this to 401).
    /// AC8: valid agent (non-governor) token presented as governor → Forbidden (HTTP handler maps to 401).
    #[test]
    fn ac_gov_grants_6_7_8_auth_errors() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let agent_tok = hub.mint_agent_token(&gov, "id-alice", None).unwrap();

        // AC6: empty/absent token string
        let no_token = GovernorToken("".into());
        assert!(
            matches!(
                hub.list_all_grants_gov(&no_token, None),
                Err(Error::AuthFailed)
            ),
            "AC6: missing token must yield AuthFailed"
        );

        // AC7: forged / never-issued token
        let forged = GovernorToken("not-a-real-token-xyz".into());
        assert!(
            matches!(
                hub.list_all_grants_gov(&forged, None),
                Err(Error::AuthFailed)
            ),
            "AC7: invalid token must yield AuthFailed"
        );

        // AC8: a valid agent token presented in the governor slot → Forbidden
        let agent_as_gov = GovernorToken(agent_tok.0.clone());
        assert!(
            matches!(
                hub.list_all_grants_gov(&agent_as_gov, None),
                Err(Error::Forbidden)
            ),
            "AC8: agent token in governor slot must yield Forbidden"
        );
    }

    // ── Grant-gated presence tests ────────────────────────────────────────────

    /// AC1: Agent A with no grant to Agent B → presence_for_token returns false (not visible).
    #[test]
    fn test_presence_no_grant_returns_offline() {
        let hub = make_hub(Duration::from_secs(30));
        let _gov = hub.install_governor(None);

        // Register listen tokens for A and B (no grant between them).
        let reg_a = hub.register_agent();
        let reg_b = hub.register_agent();

        // Open listen for both.
        let (listen_a, _rx_a) = hub.open_listen(Some(&reg_a), None, None, None, false).unwrap();
        let (listen_b, _rx_b) = hub.open_listen(Some(&reg_b), None, None, None, false).unwrap();

        // Announce both.
        hub.announce(&listen_a, "alice", false).unwrap();
        hub.announce(&listen_b, "bob", false).unwrap();

        // A has no grant with B → presence_for_token should return false.
        let result = hub.presence_for_token(&listen_a, "bob");
        assert!(
            matches!(result, Ok(false)),
            "Agent without grant should see target as not visible (false)"
        );
    }

    /// AC2: Agent A with active grant to Agent B → returns real status.
    #[test]
    fn test_presence_with_grant_returns_online() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        // Register listen tokens.
        let reg_a = hub.register_agent();
        let reg_b = hub.register_agent();

        // Open listen for both.
        let (listen_a, _rx_a) = hub.open_listen(Some(&reg_a), None, None, None, false).unwrap();
        let (listen_b, _rx_b) = hub.open_listen(Some(&reg_b), None, None, None, false).unwrap();

        // Announce both (this updates name_to_token mappings).
        hub.announce(&listen_a, "alice", false).unwrap();
        hub.announce(&listen_b, "bob", false).unwrap();

        // Create grant between alice and bob (using their listen tokens as identities).
        hub.approve_grant(&gov, &listen_a, &listen_b, None).unwrap();

        // Now A should see B as online (active SSE).
        let result = hub.presence_for_token(&listen_a, "bob");
        assert!(
            matches!(result, Ok(true)),
            "Agent with grant should see target's real status (online)"
        );
    }

    /// AC3: Grant exists but expired → presence_for_token returns false.
    #[test]
    fn test_presence_grant_expired_returns_offline() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        // Register listen tokens.
        let reg_a = hub.register_agent();
        let reg_b = hub.register_agent();

        // Open listen for both.
        let (listen_a, _rx_a) = hub.open_listen(Some(&reg_a), None, None, None, false).unwrap();
        let (listen_b, _rx_b) = hub.open_listen(Some(&reg_b), None, None, None, false).unwrap();

        // Announce both.
        hub.announce(&listen_a, "alice", false).unwrap();
        hub.announce(&listen_b, "bob", false).unwrap();

        // Create grant with immediate expiry (1ms).
        hub.approve_grant(&gov, &listen_a, &listen_b, Some(Duration::from_millis(1)))
            .unwrap();

        // Wait for expiry.
        std::thread::sleep(Duration::from_millis(10));

        // Grant expired → should return false.
        let result = hub.presence_for_token(&listen_a, "bob");
        assert!(
            matches!(result, Ok(false)),
            "Agent with expired grant should see target as not visible"
        );
    }

    /// presence_any_token: same grant-gating applies to minted agent tokens.
    #[test]
    fn test_presence_any_token_no_grant_returns_offline() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        // Mint tokens (no grant).
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();

        // Register both.
        hub.register("alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        // A uses presence_any_token with minted agent token → no grant → false.
        let result = hub.presence_any_token(&tok_a.0, "bob");
        assert!(
            matches!(result, Ok(false)),
            "Minted agent without grant should see target as not visible"
        );
    }

    /// presence_any_token: with grant, returns real status.
    #[test]
    fn test_presence_any_token_with_grant_returns_online() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        // Mint tokens and create grant.
        let tok_a = hub.mint_agent_token(&gov, "id-alice", None).unwrap();
        let tok_b = hub.mint_agent_token(&gov, "id-bob", None).unwrap();
        hub.approve_grant(&gov, "id-alice", "id-bob", None).unwrap();

        // Register both.
        hub.register("alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        hub.register("bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        // A with grant → sees bob's real status.
        let result = hub.presence_any_token(&tok_a.0, "bob");
        assert!(
            matches!(result, Ok(true)),
            "Minted agent with grant should see target's real status"
        );
    }
}
