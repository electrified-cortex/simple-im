use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use rand::Rng;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::error::Error;
use crate::persistence::{
    PersistedDenialBlock, PersistedGrant, PersistedToken, StoredAttachment, TokenStore,
};
use crate::registry::{ParticipantIdentity, PresenceScope, Registry};
use crate::rooms::RoomStore;
use crate::trust::{ApproveGrantRequest, GrantMediation, TrustChain};
use crate::types::{GovernorToken, ParticipantToken, Payload, QueuedMessage};

/// Grace period after `register_participant()` during which a token with
/// `pending_first_listen=true` is immune from Branch-1 GC.  After this window,
/// the token is eligible for normal `unlisten_ttl` eviction — bounding worst-case
/// accumulation from unauthenticated `/register` calls to `60s / registration_rate`.
/// See: sim-gc-registration-grace-cap.
const REGISTRATION_GRACE: Duration = Duration::from_secs(60);

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
pub struct ParticipantInfo {
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
    Waiting { approved: usize, required: usize },
    /// All required votes approved; the candidate is now governor.
    Established {
        candidate_name: String,
        governor_token: String,
    },
    /// At least one required voter rejected the claim.
    Rejected { candidate_name: String },
}

/// Result of approving a grant request at one of the two required stages.
pub enum ApproveStatus {
    /// Governor approved; waiting for the recipient to also approve.
    PendingRecipient,
    /// Both parties approved; the grant is now active.
    Established,
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
struct ListenTokenState {
    /// Last time this token was actively used (POST /listen, /announce, or a dequeue). Age-GC is
    /// measured from `last_active` (not creation time), so an actively-used token never expires by
    /// age alone. (15-0029 addenda / GC bug fix)
    last_active: Instant,
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
    /// Set by `register_participant()` to protect the token from Branch-1 GC until the
    /// first successful `open_listen()` call.  Cleared the moment the SSE stream opens.
    /// This eliminates the TOCTOU window where GC evicts a freshly minted token before
    /// the client can call `open_listen()`.  See: sim-gc-race-register-open-listen.
    pending_first_listen: bool,
    /// Whether this subscription opted-in to push presence events.
    /// Set at open_listen time; not persisted.
    presence_push: bool,
}

impl ListenTokenState {
    fn new() -> Self {
        let (msg_id_tx, _msg_id_rx) = watch::channel(0u64);
        ListenTokenState {
            last_active: Instant::now(),
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
            pending_first_listen: false,
            presence_push: false,
        }
    }

    fn is_sse_alive_in_hub(token: &str, sse_connections: &HashMap<String, usize>) -> bool {
        sse_connections.get(token).copied().unwrap_or(0) > 0
    }
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

struct ParticipantState {
    identity: String,
    /// Wakes up any `dequeue()` long-poll waiting for this agent.
    notify: Arc<tokio::sync::Notify>,
}

struct HubInner {
    trust: TrustChain,
    registry: Registry,
    agents: HashMap<String, ParticipantState>,
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
    listen_tokens: HashMap<String, ListenTokenState>,
    /// Permanent identity roster (15-0029 / FG-7). A name is inserted on first announce and
    /// NEVER removed by GC, revoke, or token expiry. Loaded from the `identities` DB table at
    /// startup. Checked by announce()/open_listen() to detect a registered name with no active
    /// binding (orphaned name → NAME_IN_USE; governor rebind is the only reclaim path).
    identities: HashSet<String>,
    /// Maps announced name → token (for name-claim lookup).
    name_to_token: HashMap<String, String>,
    /// Active SSE connection count per token.
    sse_connections: HashMap<String, usize>,
    /// TTL for never-listened tokens.
    gc_ttl_unlisten: Duration,
    /// TTL for listened-but-never-granted tokens.
    gc_ttl_no_grant: Duration,
    /// Grace window for `pending_first_listen` tokens; overridable in tests.
    /// After this duration, even tokens with `pending_first_listen=true` become
    /// eligible for `unlisten_ttl` eviction. (sim-gc-registration-grace-cap)
    gc_registration_grace: Duration,
    /// Persistent denial blocks keyed on (from_identity, to_name).
    denial_blocks: HashMap<(String, String), DenialBlock>,
    /// Guards the one-time startup announce: true after sim_online has been sent.
    startup_announced: bool,
    /// In-memory governance claims (election / transfer); ephemeral — lost on restart.
    pending_claims: HashMap<String, GovernanceClaim>,
    /// Monotonic counter for claim IDs.
    claim_counter: u64,
    /// Per-name pending settle timers. Sender is stored; dropping it cancels the receiver.
    settle_tasks: HashMap<String, oneshot::Sender<()>>,
    /// Duration before a settle timer fires an offline event. Default 30s.
    settle_window: Duration,
    /// Room store shared with the HTTP layer — used for room-based presence visibility.
    /// Presence is visible to a peer if they hold a PERMISSIVE grant OR share a room.
    /// Deny grants override both (hard block). (15-0028)
    room_store: Arc<RoomStore>,
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

    /// Collect active SSE senders for all grant-peers of `name` (unfiltered).
    /// Used for non-presence broadcast; grant-presence delivery uses grant_peer_presence_senders.
    /// Used to push presence events (online/offline) after a name is bound or unbound.
    ///
    /// Two resolution paths (15-0002F fix):
    ///   1. Name path — look up counterparty by name in `name_to_token` → `listen_tokens`.
    ///      Covers listen-flow agents and minted agents whose name was stored in the grant.
    ///   2. Identity path — try the counterparty's raw identity as a key in `listen_tokens`
    ///      directly.  listen-flow agents have identity == listen token, so this covers
    ///      grants where only the identity (not the name) was stored at creation time.
    ///
    /// Minted-agent grant-peers (registered via /register, not /listen) have no stored SSE
    /// sender in HubInner and fall through both paths silently.
    /// TODO(15-0002F): pushing to minted-agent grant-peers requires a separate sender registry.
    #[allow(dead_code)]
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
            // listen-flow agents store identity == listen token, so this covers grants
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

    /// Like grant_peer_senders but ONLY returns senders for tokens with presence_push=true.
    ///
    /// Visibility rule (15-0028 / operator-final-rule):
    ///   A peer receives presence events for `name` if and only if:
    ///   (A) the peer holds a PERMISSIVE grant with `name`, OR
    ///   (B) the peer currently shares a room with `name`.
    ///   DENY-GRANT OVERRIDE: if a denial block exists for (peer_identity → name), the
    ///   peer is excluded even if condition A or B would otherwise apply.
    fn grant_peer_presence_senders(&self, name: &str) -> Vec<mpsc::UnboundedSender<String>> {
        let identity = self
            .agents
            .get(name)
            .map(|s| s.identity.as_str())
            .unwrap_or(name);
        let counterparties = self.trust.grant_counterparties_for(name, identity);
        let mut senders = Vec::new();
        let mut seen_tokens: HashSet<String> = HashSet::new();

        // ── (A) Grant-based counterparties ──────────────────────────────────────────
        for (cp_name, cp_identity) in counterparties {
            // Deny-grant override: if peer has a denial block against this subject, skip.
            if self.is_denial_active(&cp_identity, name) {
                continue;
            }
            let tok_opt = cp_name
                .as_deref()
                .and_then(|n| self.name_to_token.get(n))
                .cloned();
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
                && st.presence_push
                && let Some(ref tx) = st.sse_sender
                && !tx.is_closed()
            {
                senders.push(tx.clone());
            }
        }

        // ── (B) Room-based counterparties (no grant required) ──────────────────────
        // Collect room-peer names first to avoid holding room lock while iterating name_to_token.
        let room_peers: Vec<String> = self
            .name_to_token
            .keys()
            .filter(|cp_name| {
                cp_name.as_str() != name && self.room_store.shares_room(name, cp_name)
            })
            .cloned()
            .collect();
        for cp_name in room_peers {
            if let Some(cp_tok) = self.name_to_token.get(&cp_name) {
                // For listen-flow agents, token == identity.
                if self.is_denial_active(cp_tok, name) {
                    continue;
                }
                if seen_tokens.insert(cp_tok.clone())
                    && let Some(st) = self.listen_tokens.get(cp_tok)
                    && st.presence_push
                    && let Some(ref tx) = st.sse_sender
                    && !tx.is_closed()
                {
                    senders.push(tx.clone());
                }
            }
        }

        senders
    }

    /// Returns true if a non-expired denial block exists for (from_identity → to_name).
    /// Used to enforce deny-grant override in presence fanout and pull checks.
    fn is_denial_active(&self, from_identity: &str, to_name: &str) -> bool {
        let key = (from_identity.to_string(), to_name.to_string());
        match self.denial_blocks.get(&key) {
            None => false,
            Some(block) => match block.expires_at {
                None => true,
                Some(exp) => {
                    let now_secs = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    exp > now_secs
                }
            },
        }
    }

    /// Start an offline settle timer for `name`. Cancels any existing timer.
    /// Returns (senders, cancel_rx) if there are opted-in grant-peers; None otherwise.
    fn begin_settle_offline(
        &mut self,
        name: &str,
    ) -> Option<(Vec<mpsc::UnboundedSender<String>>, oneshot::Receiver<()>)> {
        let senders = self.grant_peer_presence_senders(name);
        if senders.is_empty() {
            return None;
        }
        // Cancel any existing settle task (old sender drops → old receiver gets Err)
        self.settle_tasks.remove(name);
        let (tx, rx) = oneshot::channel();
        self.settle_tasks.insert(name.to_string(), tx);
        Some((senders, rx))
    }

    /// Cancel a pending settle timer for `name` (participant came back online).
    fn cancel_settle_online(&mut self, name: &str) {
        self.settle_tasks.remove(name); // drops sender → settle task sees Err and stops
    }

    /// Returns true if there is an active (not-yet-fired) offline settle timer for `name`.
    /// Uses `Sender::is_closed()` to detect whether the spawned task's receiver has been
    /// dropped — this happens when the timeout fires (the task exits, dropping cancel_rx).
    /// A closed sender means the settle task has already fired; we must NOT treat the name
    /// as "in settle" in that case. (15-0028)
    fn is_settle_pending(&self, name: &str) -> bool {
        self.settle_tasks
            .get(name)
            .map(|tx| !tx.is_closed())
            .unwrap_or(false)
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
        let is_alive = ListenTokenState::is_sse_alive_in_hub(&token, &self.sse_connections);
        let state = self.listen_tokens.get_mut(&token)?;
        if state.notify_suppressed || !is_alive {
            return None;
        }
        state.notify_suppressed = true;
        let pending = self.message_queues.get(name).map(|q| q.len()).unwrap_or(0);
        let sender = state.sse_sender.clone()?;
        Some((sender, pending))
    }

    /// Mark a token actively used (resets the age-GC clock). (15-0029 addenda)
    fn touch_token(&mut self, token: &str) {
        if let Some(st) = self.listen_tokens.get_mut(token) {
            st.last_active = Instant::now();
        }
    }

    /// Bind `name` to `token` atomically (name registry + agents map + token state).
    /// Caller is responsible for evicting any stale holder first.
    fn bind_name(&mut self, token: &str, name: &str) {
        // FG-7: a freshly bound name is a permanent identity (combined listen+announce path).
        self.identities.insert(name.to_string());
        self.name_to_token
            .insert(name.to_string(), token.to_string());
        self.token_to_name
            .insert(token.to_string(), name.to_string());
        let notify = Arc::new(tokio::sync::Notify::new());
        self.agents.insert(
            name.to_string(),
            ParticipantState {
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
    /// Returns a list of `(name, senders, cancel_rx)` tuples for Branch-3 evictions.  Each entry
    /// represents a token that was `!ever_listened && ever_granted` and has now been
    /// evicted.  The **caller must call `spawn_settle_task(name, senders, settle_window, cancel_rx)`
    /// for every entry after releasing the lock** (out-of-lock, silent drop). (15-0002H)
    fn gc_tokens(
        &mut self,
    ) -> Vec<(
        String,
        Vec<mpsc::UnboundedSender<String>>,
        oneshot::Receiver<()>,
    )> {
        let now = Instant::now();
        let unlisten_ttl = self.gc_ttl_unlisten;
        let no_grant_ttl = self.gc_ttl_no_grant;
        let registration_grace = self.gc_registration_grace;

        // Branch 1: !ever_listened && !ever_granted — never subscribed and never granted; safe to
        //   collect after unlisten_ttl. No presence event: ever_granted=false → no grant-peers.
        // Branch 2: ever_listened && !ever_granted — subscribed but never granted; collect after
        //   no_grant_ttl. No presence event: ever_granted=false → no grant-peers.
        // Branch 3: !ever_listened && ever_granted — received a grant before /listen was called,
        //   then vanished (session dropped, no explicit revocation). Evict after unlisten_ttl and
        //   fire sim_offline to grant-peers out-of-lock (caller responsibility). (15-0002H)
        //   Branches 1 and 3 share the same TTL threshold; the filter is unified as !ever_listened.
        //
        // GC-race guard: if `pending_first_listen` is true AND the token is still within the
        //   REGISTRATION_GRACE window, skip Branch-1 eviction — the client has not yet called
        //   `open_listen()`.  After the grace window expires the token falls through to the
        //   normal `unlisten_ttl` path so that abandoned `/register` calls cannot accumulate
        //   indefinitely. (sim-gc-race-register-open-listen, sim-gc-registration-grace-cap)
        // 15-0029 addenda (GC bug fix): age is measured from `last_active` (reset on any active
        // use), and two classes are EXEMPT from age-GC entirely:
        //   (a) governor listen tokens (long-lived control credentials), and
        //   (b) identity-bound tokens — a token whose name is a registered identity. Only
        //       truly-abandoned tokens (registered/listened but never identity-bound, idle past
        //       their TTL) are reaped.
        let identities = &self.identities;
        let to_remove: Vec<String> = self
            .listen_tokens
            .iter()
            .filter(|(_, st)| !st.revoked)
            .filter(|(_, st)| {
                // (a) governor listen sessions are never age-GC'd.
                if st.governor_id.is_some() {
                    return false;
                }
                // (b) identity-bound tokens (registered name) are never age-GC'd.
                if let Some(ref name) = st.name
                    && identities.contains(name)
                {
                    return false;
                }
                if st.pending_first_listen {
                    // Registered but never used for open_listen(): the grace window is the TTL.
                    now.duration_since(st.last_active) > registration_grace
                } else if !st.ever_listened {
                    // Branches 1 (no grant) and 3 (ever_granted): both use unlisten_ttl.
                    now.duration_since(st.last_active) > unlisten_ttl
                } else if !st.ever_granted && st.name.is_none() {
                    // Branch 2: listened but never granted and name-unbound — idle past no_grant_ttl.
                    now.duration_since(st.last_active) > no_grant_ttl
                } else {
                    false
                }
            })
            .map(|(tok, _)| tok.clone())
            .collect();

        let mut offline_events: Vec<(
            String,
            Vec<mpsc::UnboundedSender<String>>,
            oneshot::Receiver<()>,
        )> = Vec::new();

        for tok in to_remove {
            self.sse_connections.remove(&tok);
            if let Some(st) = self.listen_tokens.remove(&tok)
                && let Some(ref name) = st.name
            {
                // Branch 3: begin settle BEFORE removing from agents map.
                // INVARIANT: grant_peer_presence_senders() must be called while agents[name]
                // still exists (see grant_peer_senders() doc). (15-0002H)
                if st.ever_granted
                    && let Some((senders, cancel_rx)) = self.begin_settle_offline(name.as_str())
                {
                    offline_events.push((name.clone(), senders, cancel_rx));
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

/// Spawn an async settle task that fires an offline presence event after `settle_window`,
/// unless cancelled first via the `cancel_rx` oneshot.
/// Uses try_current() so sync test contexts silently skip the spawn.
fn spawn_settle_task(
    name: String,
    senders: Vec<mpsc::UnboundedSender<String>>,
    settle_window: Duration,
    cancel_rx: oneshot::Receiver<()>,
) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(settle_window) => {
                    push_presence_event(senders, &name, "offline");
                }
                _ = cancel_rx => {}
            }
        });
    }
    // No runtime (sync test context): drop cancel_rx, settle never fires.
    // settle_tasks already holds the sender which will be cleaned up on next online.
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
        let gc_ttl_unlisten = clamp_env_secs("SIMPLE_IM_GC_UNLISTEN_SECS", 60, 3600, 300);
        let gc_ttl_no_grant = clamp_env_secs("SIMPLE_IM_GC_NO_GRANT_SECS", 120, 7200, 1800);
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
                identities: HashSet::new(),
                name_to_token: HashMap::new(),
                sse_connections: HashMap::new(),
                gc_ttl_unlisten,
                gc_ttl_no_grant,
                gc_registration_grace: REGISTRATION_GRACE,
                denial_blocks: HashMap::new(),
                startup_announced: false,
                pending_claims: HashMap::new(),
                claim_counter: 0,
                settle_tasks: HashMap::new(),
                settle_window: clamp_env_secs("SIMPLE_IM_SETTLE_WINDOW_SECS", 1, 300, 30),
                room_store: Arc::new(RoomStore::new()),
            }),
            token_store: None,
        }
    }

    /// Returns the shared room store for integration with the HTTP layer.
    pub fn room_store(&self) -> Arc<RoomStore> {
        Arc::clone(&self.lock().room_store)
    }

    /// Construct a hub pre-loaded with persisted tokens and grants, backed by `token_store`.
    pub fn new_with_persisted_state(
        lapse_after: Duration,
        token_store: Arc<TokenStore>,
        persisted_tokens: Vec<PersistedToken>,
        persisted_grants: Vec<PersistedGrant>,
        persisted_denial_blocks: Vec<PersistedDenialBlock>,
        persisted_identities: Vec<crate::persistence::PersistedIdentity>,
    ) -> Self {
        let mut hub = Self::new(lapse_after);
        {
            let mut inner = hub.inner.lock().unwrap();
            // BLOCKER-5: partition participant tokens into the listen-token map. The "listen"
            // inclusion is a belt-and-suspenders guard — after migration all rows are
            // "participant", but the fallback ensures a first post-upgrade startup cannot
            // discard valid sessions if the predicate runs before migration completes.
            let (listen_toks, regular_toks): (Vec<PersistedToken>, Vec<PersistedToken>) =
                persisted_tokens
                    .into_iter()
                    .partition(|t| t.token_type == "participant" || t.token_type == "listen");
            inner.trust.load_from_store(regular_toks, persisted_grants);
            // Populate the permanent identity roster (FG-7) BEFORE token restore so guards see it.
            for row in persisted_identities {
                inner.identities.insert(row.name);
            }
            for t in listen_toks {
                let mut state = ListenTokenState::new();
                state.ever_listened = true;
                state.ever_granted = true;
                // BLOCKER-5 name restore: for participant tokens the `identity` column holds the
                // name (post-migration). Fall back to the retired `name` column only for a stale
                // pre-migration row whose identity still equals the token.
                let resolved_name: Option<String> =
                    if !t.identity.is_empty() && t.identity != t.token {
                        Some(t.identity.clone())
                    } else {
                        t.name.clone()
                    };
                // Restore name bindings so the agent is reachable while offline.
                // If two persisted tokens share a name (shouldn't happen), last-write-wins.
                if let Some(name) = resolved_name {
                    state.name = Some(name.clone());
                    inner.identities.insert(name.clone());
                    inner.name_to_token.insert(name.clone(), t.token.clone());
                    inner.token_to_name.insert(t.token.clone(), name.clone());
                    inner.agents.insert(
                        name.clone(),
                        ParticipantState {
                            identity: t.token.clone(),
                            notify: Arc::new(tokio::sync::Notify::new()),
                        },
                    );
                }
                inner.listen_tokens.insert(t.token, state);
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

    /// Override the Branch-1 GC TTL for testing purposes.
    ///
    /// Allows tests to use very short TTLs without waiting for the production minimums
    /// enforced by `clamp_env_secs`.  Only compiled in `#[cfg(test)]`.
    #[cfg(test)]
    fn set_gc_unlisten_ttl_for_test(&self, ttl: Duration) {
        self.lock().gc_ttl_unlisten = ttl;
    }

    /// Trigger GC inline (test seam).  Returns the count of tokens evicted.
    ///
    /// Provides a deterministic way to flush expired tokens in tests without going through
    /// a public API call that happens to trigger GC as a side-effect.
    #[cfg(test)]
    fn trigger_gc_for_test(&self) -> usize {
        // Count tokens before and after to capture ALL evictions, including Branch-1
        // (never-listened, no offline event) and not just Branch-3 offline events.
        let before = self.lock().listen_tokens.len();
        let _ = self.lock().gc_tokens();
        let after = self.lock().listen_tokens.len();
        before.saturating_sub(after)
    }

    /// Override the `pending_first_listen` grace window for testing purposes.
    ///
    /// Allows tests to exercise grace-cap expiry without sleeping 60 real seconds.
    /// Only compiled in `#[cfg(test)]`.  (sim-gc-registration-grace-cap)
    #[cfg(test)]
    fn set_gc_registration_grace_for_test(&self, grace: Duration) {
        self.lock().gc_registration_grace = grace;
    }

    /// Override the settle window for testing purposes.
    pub fn set_settle_window_for_test(&self, window: Duration) {
        self.lock().settle_window = window;
    }

    /// Subscribe to the governor event broadcast channel (governance notices, concurrent-use alerts).
    pub fn subscribe_gov_events(&self) -> broadcast::Receiver<String> {
        self.lock().gov_events.subscribe()
    }

    /// Debug (15-DEBUG): snapshot of in-memory collection sizes for leak/OOM diagnosis.
    /// Logged every 30s by the periodic task in `main::run`. A steadily rising count on
    /// any one collection points at the leak; flat counts across a crash interval argue
    /// against OOM (look at panic log).
    pub fn debug_state_sizes(&self) -> String {
        let inner = self.lock();
        let queued_msgs: usize = inner.message_queues.values().map(|q| q.len()).sum();
        format!(
            "listen_tokens={} agents={} \
             name_to_token={} token_to_name={} queues={} queued_msgs={} conn_reqs={} \
             reply_windows={} mediation_holds={} denial_blocks={} sse_conns={} active_sse={}",
            inner.listen_tokens.len(),
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
                                ListenTokenState::is_sse_alive_in_hub(tok, &inner.sse_connections)
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
                    let ntx = inner.take_notify(voter_name);
                    notify_pairs.push((voter_name.clone(), msg_json, notify, ntx));
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
        for (_, _, notify, ntx) in notify_pairs {
            if let Some(n) = notify {
                n.notify_one();
            }
            if let Some((sender, pending)) = ntx {
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
                let ntx = inner.take_notify(&candidate_name);

                return {
                    drop(inner);
                    if let Some(n) = notify {
                        n.notify_one();
                    }
                    if let Some((sender, pending)) = ntx {
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
            let ntx = inner.take_notify(&candidate_name);
            let _ = candidate_token; // used above for token resolution; captured for completeness

            (
                ClaimResolution::Established {
                    candidate_name: candidate_name.clone(),
                    governor_token: gov_tok_str.clone(),
                },
                Some((gov_tok_str, notify, ntx)),
            )
        }; // lock released

        if let Some((tok, notify, ntx)) = post_lock {
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
            if let Some((sender, pending)) = ntx {
                let _ = sender.send(format!(r#"{{"type":"notify","pending":{}}}"#, pending));
            }
        }

        Ok(resolution)
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

    /// Send a message (§5.3). Implements the full authorization pipeline:
    /// grant → reply window → brief auth (hold) or BriefRequired.
    /// Queues the message for registered recipients regardless of online status.
    #[allow(clippy::type_complexity)] // deliberate: local tuple extracts grant/notify state atomically under the lock
    pub fn send(
        &self,
        from_token: &ParticipantToken,
        to_name: &str,
        payload: Payload,
        _reason: Option<String>,
        thread_id: Option<String>,
    ) -> Result<Ack, Error> {
        let (notify_arc, consumed_grant_id, notify_val): (
            Option<Arc<tokio::sync::Notify>>,
            Option<String>,
            Option<(mpsc::UnboundedSender<String>, usize)>,
        ) = {
            let mut inner = self.lock();
            inner.prune_expired();
            // Sender auth: agents registered via /listen+/announce live in listen_tokens.
            let agent_state = inner
                .listen_tokens
                .get(&from_token.0)
                .ok_or(Error::AuthFailed)?;
            if agent_state.revoked {
                return Err(Error::TokenRevoked);
            }
            // Fix 1: name must be bound at send time (durable registry); ghost messages rejected.
            let from_name = inner
                .token_to_name
                .get(&from_token.0)
                .cloned()
                .ok_or(Error::AnnounceRequired)?;
            let from_identity = from_token.0.clone();

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
                        let ntx = inner.take_notify(to_name);
                        (
                            inner.agents.get(to_name).map(|s| Arc::clone(&s.notify)),
                            Some(gid),
                            ntx,
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
                        let ntx = inner.take_notify(to_name);
                        (
                            inner.agents.get(to_name).map(|s| Arc::clone(&s.notify)),
                            Some(gid),
                            ntx,
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
                        let ntx = inner.take_notify(to_name);
                        (
                            inner.agents.get(to_name).map(|s| Arc::clone(&s.notify)),
                            None,
                            ntx,
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
                                let sender_notify_val = if !already_pending {
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
                                if let Some((sender, pending)) = sender_notify_val {
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
        if let Some((sender, pending)) = notify_val {
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
            let st = inner
                .listen_tokens
                .get(from_token)
                .ok_or(Error::AuthFailed)?;
            if st.revoked {
                return Err(Error::TokenRevoked);
            }
            let from_name = inner
                .token_to_name
                .get(from_token)
                .cloned()
                .ok_or(Error::AnnounceRequired)?;
            let from_identity = from_token.to_string();
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
        let (notify_arc, notify_val) = {
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
            let ntx_result = inner.take_notify(to_name);
            let arc = inner.agents.get(to_name).map(|s| Arc::clone(&s.notify));
            (arc, ntx_result)
        };

        // Phase 4 — fire wakeups out of lock.
        if let Some(n) = notify_arc {
            n.notify_one();
        }
        if let Some((sender, pending)) = notify_val {
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
            let st = inner.listen_tokens.get(token).ok_or(Error::AuthFailed)?;
            if st.revoked {
                return Err(Error::TokenRevoked);
            }
            (token.to_string(), inner.token_to_name.get(token).cloned())
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
        let (to_name, notify, consumed_grant_id, ntx) = {
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
            let ntx = inner.take_notify(&to_name_clone);
            (to_name_clone, notify, consumed_grant_id, ntx)
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

        if let Some((sender, pending)) = ntx {
            let event = format!(r#"{{"type":"notify","pending":{}}}"#, pending);
            let _ = sender.send(event);
        }

        Ok(MediationResult::Delivered { to_name })
    }

    /// Governor-deregisters a minted agent AND revokes their listen token (if any), atomically.
    /// The SSE revocation event and presence settle task are started after the lock releases.
    pub fn revoke_by_name(&self, name: &str, gov: &GovernorToken) -> Result<(), Error> {
        let (sse_sender, settle_opt, settle_window) = {
            let mut inner = self.lock();
            inner.trust.validate_governor_token(gov)?;
            // Begin settle BEFORE removing name from maps (presence push AC4 / TR4).
            let settle_opt = inner.begin_settle_offline(name);
            let settle_window = inner.settle_window;
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
            let sse_sender = if let Some(listen_tok) = inner.name_to_token.remove(name) {
                if let Some(state) = inner.listen_tokens.get_mut(&listen_tok) {
                    state.revoked = true;
                    state.sse_sender.take()
                } else {
                    None
                }
            } else {
                None
            };
            (sse_sender, settle_opt, settle_window)
        }; // lock released

        if let Some(tx) = sse_sender {
            let _ = tx.send(r#"{"type":"service","event":"revoked"}"#.to_string());
        }
        // Spawn settle task for opted-in grant-peers with active SSE streams.
        if let Some((senders, cancel_rx)) = settle_opt {
            spawn_settle_task(name.to_string(), senders, settle_window, cancel_rx);
        }
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

    /// Returns true if the named agent is currently within its liveness window, holds an active SSE
    /// connection, or is within the offline settle window after a recent disconnect.
    pub fn presence(&self, name: &str) -> bool {
        let inner = self.lock();
        inner.is_online_effective(name) || inner.is_settle_pending(name)
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

    /// Validates the governor token.
    pub fn validate_governor_token(&self, token: &GovernorToken) -> Result<(), Error> {
        self.lock().trust.validate_governor_token(token)
    }

    /// List all registered agents with their identity and effective status.
    /// Requires a valid governor token; returns Forbidden for agent tokens.
    /// Hidden agents always appear offline even to governors.
    pub fn list_participants(&self, gov: &GovernorToken) -> Result<Vec<ParticipantInfo>, Error> {
        let inner = self.lock();
        inner.trust.validate_governor_token(gov)?;
        let mut result: Vec<ParticipantInfo> = inner
            .agents
            .iter()
            .map(|(name, state)| {
                // Fix 3: listen-flow agents track SSE by token, not by name — check sse_connections first.
                // AC2 fix: also check registry liveness so roster shows online after announce+SSE-drop.
                let is_online = if let Some(tok) = inner.name_to_token.get(name) {
                    let listen_hidden = inner
                        .listen_tokens
                        .get(tok)
                        .map(|s| s.hidden)
                        .unwrap_or(false);
                    !listen_hidden
                        && (ListenTokenState::is_sse_alive_in_hub(tok, &inner.sse_connections)
                            || inner.registry.is_online(name))
                } else {
                    let scope = inner.presence_scope_effective(name);
                    match scope {
                        Some(PresenceScope::Hidden) => false,
                        Some(_) => inner.is_online_effective(name),
                        None => false,
                    }
                };
                ParticipantInfo {
                    name: name.clone(),
                    identity: state.identity.clone(),
                    status: if is_online { "online" } else { "offline" },
                }
            })
            .collect();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }

    /// Initiate a governor transfer. Returns a one-time transfer token to deliver to the recipient.
    pub fn transfer_governor(
        &self,
        from: &GovernorToken,
        to_identity: Option<&str>,
    ) -> Result<String, Error> {
        self.lock().trust.transfer_governor(from, to_identity)
    }

    /// Accept a pending governor transfer (FG-5 / security-MAJOR-3). The claiming identity is
    /// derived from the **verified participant bearer** — never from the request body. `bearer`
    /// must be a current named participant token. Revokes the initiating governor; returns the
    /// new governor token.
    ///
    /// Errors: `AuthFailed` (bearer is not a named participant), `RecipientUnknown` (transfer
    /// token not found or already consumed → 404), `Forbidden` (transfer's to_identity is set
    /// and does not match the bearer's name → 403).
    pub fn accept_governor_transfer(
        &self,
        bearer: &str,
        transfer_token: &str,
    ) -> Result<GovernorToken, Error> {
        let (new_token, expiry_instant) = {
            let mut inner = self.lock();
            // Resolve the claiming identity from the verified participant bearer.
            let is_live_participant = inner
                .listen_tokens
                .get(bearer)
                .map(|s| !s.revoked)
                .unwrap_or(false);
            if !is_live_participant {
                return Err(Error::AuthFailed);
            }
            let name = inner
                .token_to_name
                .get(bearer)
                .cloned()
                .ok_or(Error::AuthFailed)?;
            let new_token = match inner.trust.accept_governor_transfer(transfer_token, &name) {
                Ok(t) => t,
                // trust returns AuthFailed when the transfer token is unknown/consumed → 404.
                Err(Error::AuthFailed) => return Err(Error::RecipientUnknown),
                Err(e) => return Err(e),
            };
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

    /// Operator-anchored governor reset (POST /admin/governor/reset). In one locked section:
    /// revoke every current governor, clear all pending transfers (so an in-flight transfer
    /// cannot bypass the revoke), and install a fresh governor. The state change is committed to
    /// SQLite in a single transaction (DELETE old governors + INSERT new) — no crash window
    /// between revoke and install. Returns the new governor token. (security-MAJOR-1/2, M2)
    pub fn admin_reset_governor(&self) -> GovernorToken {
        let (new_token, revoked) = {
            let mut inner = self.lock();
            let revoked = inner.trust.revoke_all_governors();
            inner.trust.clear_pending_transfers();
            let new_token = inner.trust.install_governor(None);
            (new_token, revoked)
        };
        if let Some(store) = self.token_store.clone() {
            let new = new_token.0.clone();
            self.db_write(async move {
                if let Err(e) = store.reset_governors(&revoked, &new).await {
                    eprintln!("WARNING: admin governor reset DB write failed: {e}");
                }
            });
        }
        new_token
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
            let st = inner
                .listen_tokens
                .get(from_token_str)
                .ok_or(Error::AuthFailed)?;
            if st.revoked {
                return Err(Error::TokenRevoked);
            }
            let from_name = inner
                .token_to_name
                .get(from_token_str)
                .cloned()
                .unwrap_or_default();
            let from_identity = from_token_str.to_string();

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
                let ntx = inner.take_notify(to_name);
                let notify = inner.agents.get(to_name).map(|s| Arc::clone(&s.notify));
                (request_id, Some((notify, ntx)))
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
        if let Some((notify, ntx)) = recipient_notify {
            if let Some(n) = notify {
                n.notify_one();
            }
            if let Some((sender, _pending)) = ntx {
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
        let (status, notify_opt, listen_to_persist, identity_senders, persist_grant) = {
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
                    // Recipient is valid if their token IS their identity (listen-flow).
                    let is_recipient = inner.listen_tokens.contains_key(token_str)
                        && token_str == to_identity.as_str();
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
                    let ntx = inner.take_notify(&to_name);
                    let notify = inner.agents.get(&to_name).map(|s| Arc::clone(&s.notify));
                    (
                        ApproveStatus::PendingRecipient,
                        notify,
                        vec![],
                        ntx.into_iter().map(|(s, _)| s).collect::<Vec<_>>(),
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
                    let mut listen_to_persist: Vec<(String, String)> = Vec::new();
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
                            listen_to_persist.push((tok, name.clone()));
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
                        listen_to_persist,
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
        if !listen_to_persist.is_empty()
            && let Some(store) = self.token_store.clone()
        {
            for (tok, name) in listen_to_persist {
                let store2 = store.clone();
                self.db_write(async move {
                    // 15-0029: participant rows write identity=name (never identity=token).
                    if let Err(e) = store2
                        .upsert_token(&tok, &name, "participant", None, None)
                        .await
                    {
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
        let (from_name, from_identity, to_name, ntx, notify) = {
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
            let is_recipient = !is_governor
                && inner.listen_tokens.contains_key(token_str)
                && token_str == to_identity.as_str();
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
            let ntx = inner.take_notify(&from_name);
            let notify = inner.agents.get(&from_name).map(|s| Arc::clone(&s.notify));
            (from_name, from_identity, to_name, ntx, notify)
        };
        let _ = from_name;
        if let Some(n) = notify {
            n.notify_one();
        }
        if let Some((sender, pending)) = ntx {
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
        let (ntx, notify) = {
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
            let is_recipient = !is_governor
                && inner.listen_tokens.contains_key(token_str)
                && token_str == to_identity.as_str();
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
            let ntx = inner.take_notify(&from_name);
            let notify = inner.agents.get(&from_name).map(|s| Arc::clone(&s.notify));
            (ntx, notify)
        };
        if let Some(n) = notify {
            n.notify_one();
        }
        if let Some((sender, pending)) = ntx {
            let _ = sender.send(format!(r#"{{"type":"notify","pending":{}}}"#, pending));
        }
        Ok(())
    }

    /// Returns true if this agent has at least one queued message (kick pending).
    /// Used by the SSE handler to fire an immediate kick on reconnect.
    pub fn kick_pending_for(&self, name: &str) -> bool {
        self.lock().kick_pending.contains(name)
    }

    // ── Listen-flow methods ───────────────────────────────────────────────────

    /// Issue a new listen token (random 8-12 digit numeric string).
    pub fn issue_token(&self) -> String {
        let (tok, gc_offline_events, settle_window) = {
            let mut inner = self.lock();
            let gc_offline_events = inner.gc_tokens();
            let settle_window = inner.settle_window;
            let mut rng = rand::thread_rng();
            let tok = loop {
                let digits: u64 = rng.gen_range(10_000_000..=999_999_999_999);
                let tok = digits.to_string();
                if !inner.listen_tokens.contains_key(&tok) {
                    inner
                        .listen_tokens
                        .insert(tok.clone(), ListenTokenState::new());
                    break tok;
                }
            };
            (tok, gc_offline_events, settle_window)
        }; // lock released
        // Spawn settle tasks for Branch-3 GC evictions after the lock is released. (15-0002H)
        for (name, senders, cancel_rx) in gc_offline_events {
            spawn_settle_task(name, senders, settle_window, cancel_rx);
        }
        tok
    }

    /// Register a new agent and obtain a listen token without opening an SSE stream.
    ///
    /// Use this token with `open_listen()` to start listening.
    /// This replaces the old anonymous /listen flow — now agents must register first.
    ///
    /// The returned token is marked `pending_first_listen = true`, which prevents Branch-1
    /// GC from evicting it before `open_listen()` is called.  See: sim-gc-race-register-open-listen.
    pub fn register_participant(&self) -> String {
        let mut inner = self.lock();
        let mut rng = rand::thread_rng();
        loop {
            let digits: u64 = rng.gen_range(10_000_000..=999_999_999_999);
            let tok = digits.to_string();
            if !inner.listen_tokens.contains_key(&tok) {
                let mut state = ListenTokenState::new();
                state.pending_first_listen = true;
                inner.listen_tokens.insert(tok.clone(), state);
                return tok;
            }
        }
    }

    /// True if `token` is a current (non-revoked) participant (listen) token. Used by the
    /// /register handler to return 403 (not 401) when a participant presents its own token.
    pub fn is_participant_token(&self, token: &str) -> bool {
        self.lock()
            .listen_tokens
            .get(token)
            .map(|s| !s.revoked)
            .unwrap_or(false)
    }

    /// Governor-gated participant token issuance (POST /register, FG-2). The governor is
    /// validated first.
    ///
    /// - `name == None`: mint a fresh, unbound participant token.
    /// - `name == Some(existing)`: ATOMIC governor rebind — in one locked section, invalidate the
    ///   identity's current token (sending it a `superseded/governor_rebind` SSE), mint a new
    ///   participant token, and bind it to the name. The identity record and all name-keyed grants
    ///   are unchanged. Returns `(new_token, Some(name))`.
    ///
    /// Errors: `AuthFailed`/`TokenExpired` (governor invalid), `Forbidden` (bearer is a
    /// participant), `RecipientUnknown` (name given but not a registered identity).
    pub fn issue_participant_token(
        &self,
        gov: &GovernorToken,
        name: Option<&str>,
    ) -> Result<(String, Option<String>), Error> {
        let (new_token, bound, old_token) = {
            let mut inner = self.lock();
            inner.trust.validate_governor_token(gov)?;

            let mut rng = rand::thread_rng();
            let mint = |inner: &mut HubInner, rng: &mut rand::rngs::ThreadRng| -> String {
                loop {
                    let digits: u64 = rng.gen_range(10_000_000..=999_999_999_999);
                    let tok = digits.to_string();
                    if !inner.listen_tokens.contains_key(&tok) {
                        let mut state = ListenTokenState::new();
                        state.pending_first_listen = true;
                        inner.listen_tokens.insert(tok.clone(), state);
                        break tok;
                    }
                }
            };

            match name {
                None => {
                    let tok = mint(&mut inner, &mut rng);
                    (tok, None, None)
                }
                Some(name) => {
                    if !inner.identities.contains(name) {
                        return Err(Error::RecipientUnknown);
                    }
                    // Invalidate the identity's current token (if any) and notify it.
                    let old_token = inner.name_to_token.get(name).cloned();
                    if let Some(ref old) = old_token {
                        if let Some(st) = inner.listen_tokens.get(old)
                            && let Some(ref tx) = st.sse_sender
                        {
                            let _ = tx.send(
                                r#"{"type":"service","event":"superseded","reason":"governor_rebind"}"#
                                    .to_string(),
                            );
                        }
                        inner.listen_tokens.remove(old);
                        inner.sse_connections.remove(old);
                        inner.token_to_name.remove(old);
                    }
                    inner.name_to_token.remove(name);
                    inner.agents.remove(name);
                    inner.registry.force_deregister(name);

                    // Mint a new token and bind it to the name atomically.
                    let tok = mint(&mut inner, &mut rng);
                    if let Some(st) = inner.listen_tokens.get_mut(&tok) {
                        st.ever_granted = true;
                    }
                    inner.bind_name(&tok, name);
                    (tok, Some(name.to_string()), old_token)
                }
            }
        };

        if let Some(store) = self.token_store.clone() {
            let new = new_token.clone();
            let bound_name = bound.clone();
            let old = old_token.clone();
            self.db_write(async move {
                if let Some(old) = old {
                    let _ = store.delete_token(&old).await;
                }
                if let Some(ref nm) = bound_name {
                    if let Err(e) = store.upsert_identity(nm).await {
                        eprintln!("WARNING: identity store write failed: {e}");
                    }
                    if let Err(e) = store
                        .upsert_token(&new, nm, "participant", None, None)
                        .await
                    {
                        eprintln!("WARNING: token store write failed: {e}");
                    }
                }
            });
        }

        Ok((new_token, bound))
    }

    /// Opens an SSE listen stream for a token.
    ///
    /// Token is REQUIRED. If no token or unknown token, returns `AuthFailed`.
    /// Use `register_participant()` to obtain a token before calling this.
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
        presence_push: bool,
    ) -> Result<(String, mpsc::UnboundedReceiver<String>), Error> {
        // Token is required.
        let provided_token = token_opt.ok_or(Error::AuthFailed)?;
        let _observed_host_str = observed_host.unwrap_or_default();
        let (token, rx, bound_name_for_persist, gc_offline_events, settle_window) = {
            let mut inner = self.lock();
            let gc_offline_events = inner.gc_tokens();
            let settle_window = inner.settle_window;

            // Token must exist in listen_tokens (pre-registered via register_participant).
            let token = if inner.listen_tokens.contains_key(provided_token) {
                if inner
                    .listen_tokens
                    .get(provided_token)
                    .map(|s| s.revoked)
                    .unwrap_or(false)
                {
                    drop(inner);
                    // Spawn settle tasks for Branch-3 GC evictions before returning. (15-0002H)
                    for (name, senders, cancel_rx) in gc_offline_events {
                        spawn_settle_task(name, senders, settle_window, cancel_rx);
                    }
                    return Err(Error::TokenRevoked);
                }

                // Single-subscription enforcement: check if already has active SSE.
                let has_active_sse =
                    ListenTokenState::is_sse_alive_in_hub(provided_token, &inner.sse_connections);
                if has_active_sse && !force {
                    drop(inner);
                    for (name, senders, cancel_rx) in gc_offline_events {
                        spawn_settle_task(name, senders, settle_window, cancel_rx);
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
                    inner
                        .listen_tokens
                        .insert(new_tok.clone(), ListenTokenState::new());
                    new_tok
                } else {
                    // Not a listen token or governor token — auth failed.
                    drop(inner);
                    for (name, senders, cancel_rx) in gc_offline_events {
                        spawn_settle_task(name, senders, settle_window, cancel_rx);
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
                    .or_insert_with(|| ParticipantState {
                        identity: token.clone(),
                        notify: Arc::new(tokio::sync::Notify::new()),
                    });
            }

            // Concurrent-use detection: if new IP differs from last IP within window.
            {
                let alert_opt: Option<String> = {
                    let state = inner
                        .listen_tokens
                        .get_mut(&token)
                        .expect("listen_token entry must exist during open_listen");
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
            // Also clear pending_first_listen: the GC-race guard is no longer needed once
            // the stream is open.  (sim-gc-race-register-open-listen)
            // Store the presence_push opt-in flag for this subscription.
            if let Some(state) = inner.listen_tokens.get_mut(&token) {
                state.ever_listened = true;
                state.pending_first_listen = false;
                state.presence_push = presence_push;
                state.last_active = Instant::now(); // 15-0029 addenda: reset age-GC clock
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
            let (_name_in_use, _holder_identity, bound_name_for_persist) =
                if let Some(name) = name_to_bind {
                    if inner.name_to_token.get(name).map(|t| t.as_str()) == Some(token.as_str()) {
                        // Already bound to this token — idempotent.
                        // Cancel any pending settle task (participant reconnected).
                        inner.cancel_settle_online(name);
                        (false, None::<String>, None::<String>)
                    } else if inner.name_to_token.contains_key(name) {
                        // BLOCKER-4: held by a DIFFERENT token (the same-token case returned
                        // above). Whether the holder's SSE is live or stale, no cross-token
                        // eviction occurs — force-reclaim is removed (FG-1). NAME_IN_USE.
                        (true, Some(name.to_string()), None)
                    } else if inner.registry.is_online(name) {
                        // A minted agent holds this name.
                        (true, Some(name.to_string()), None)
                    } else if inner.identities.contains(name) {
                        // BLOCKER-3: registered identity with no active binding (orphaned) —
                        // governor rebind is the only reclaim path. NAME_IN_USE.
                        (true, Some(name.to_string()), None)
                    } else {
                        inner.bind_name(&token, name);
                        // Cancel any pending settle task (name is being freshly bound).
                        inner.cancel_settle_online(name);
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
            // AC8: Both paths now include subscription_id for unambiguous subscription identity.
            {
                let name_opt = inner.listen_tokens.get(&token).and_then(|s| s.name.clone());
                let governor_minted = token.as_str() != provided_token;
                let welcome = if governor_minted {
                    serde_json::json!({
                        "type": "service",
                        "event": "welcome",
                        "subscription_id": &token,
                        "token": &token,
                        "name": name_opt,
                        "instructions": "Call POST /announce to register your name. You will receive notify events when messages arrive — call POST /messages/dequeue to retrieve them.",
                    })
                } else {
                    serde_json::json!({
                        "type": "service",
                        "event": "welcome",
                        "subscription_id": &token,
                        "name": name_opt,
                        "instructions": "Call POST /announce to register your name. You will receive notify events when messages arrive — call POST /messages/dequeue to retrieve them.",
                    })
                }
                .to_string();
                let _ = tx.send(welcome);
            }

            // Emit sub event: provides last_message_id for gap detection on reconnect.
            {
                let last_msg_id = inner
                    .listen_tokens
                    .get(&token)
                    .map(|st| *st.msg_id_watch.borrow())
                    .unwrap_or(0);
                let sub_event = serde_json::json!({
                    "type": "sub",
                    "last_message_id": last_msg_id,
                })
                .to_string();
                let _ = tx.send(sub_event);
            }

            // Startup announce: fire sim_online exactly once on first SSE subscription.
            if !inner.startup_announced {
                inner.startup_announced = true;
                let sim_online = serde_json::json!({
                    "type": "service",
                    "event": "sim_online",
                })
                .to_string();
                let _ = tx.send(sim_online);
            }

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

            (
                token,
                rx,
                bound_name_for_persist,
                gc_offline_events,
                settle_window,
            )
        }; // lock released

        // Spawn settle tasks for Branch-3 GC evictions after the lock is released. (15-0002H)
        for (name, senders, cancel_rx) in gc_offline_events {
            spawn_settle_task(name, senders, settle_window, cancel_rx);
        }

        // Persist the newly bound name outside the lock (mirrors announce pattern).
        if let Some(name) = bound_name_for_persist
            && let Some(store) = self.token_store.clone()
        {
            let tok = token.clone();
            self.db_write(async move {
                // FG-7: persist the permanent identity record (idempotent; created_at preserved).
                if let Err(e) = store.upsert_identity(&name).await {
                    eprintln!("WARNING: identity store write failed: {e}");
                }
                // BLOCKER-1 / FG-3: write identity=name, type="participant" (name col retired).
                if let Err(e) = store
                    .upsert_token(&tok, &name, "participant", None, None)
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
    /// drop (AC3 / TR3) — start a settle timer for grant-peers.
    pub fn close_listen(&self, token: &str) {
        let (settle_opt, settle_window, dropped_name) = {
            let mut inner = self.lock();

            // Determine whether this close triggers a settle timer.
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
            let settle_opt = if let Some(ref name) = dropped_name {
                inner.begin_settle_offline(name)
            } else {
                None
            };
            let settle_window = inner.settle_window;

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

            (settle_opt, settle_window, dropped_name)
        };

        // Start settle timer for grant-peers on unexpected connection drop.
        if let Some(ref name) = dropped_name
            && let Some((senders, cancel_rx)) = settle_opt
        {
            spawn_settle_task(name.clone(), senders, settle_window, cancel_rx);
        }
    }

    /// Cancel (unsubscribe) an active listen session for `token`.
    /// Closes the SSE stream, unbinds the name, and marks the agent offline.
    /// Returns Ok(()) on success, Err if the token is unknown/revoked or has no active subscription.
    pub fn cancel_listen(&self, token: &str) -> Result<(), Error> {
        let (sender_opt, settle_opt, settle_window, cancelled_name) = {
            let mut inner = self.lock();
            let state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
            if state.revoked {
                return Err(Error::TokenRevoked);
            }
            if !ListenTokenState::is_sse_alive_in_hub(token, &inner.sse_connections) {
                return Err(Error::RecipientOffline);
            }
            // Begin settle BEFORE unbinding (presence push TR2).
            let cancelled_name = inner.token_to_name.get(token).cloned();
            let settle_opt = if let Some(ref name) = cancelled_name {
                inner.begin_settle_offline(name)
            } else {
                None
            };
            let settle_window = inner.settle_window;
            // Remove SSE connection tracking.
            inner.sse_connections.remove(token);
            // Unbind name from all lookup maps.
            // Also deregister from the liveness registry so that a subsequent announce()
            // for the same name by a different token does not hit the minted-agent conflict
            // check (registry.is_online && !name_to_token) and return NameInUse. (15-0028)
            if let Some(name) = inner.token_to_name.remove(token) {
                inner.name_to_token.remove(&name);
                inner.agents.remove(&name);
                inner.registry.force_deregister(&name);
            }
            // Clear name and SSE sender from token state.
            let st = inner
                .listen_tokens
                .get_mut(token)
                .expect("listen_token state must exist during close_listen");
            st.name = None;
            let sender_opt = st.sse_sender.take();
            (sender_opt, settle_opt, settle_window, cancelled_name)
        };
        if let Some(tx) = sender_opt {
            let _ = tx.send(r#"{"type":"service","event":"cancelled"}"#.to_string());
        }
        // Spawn settle task for all opted-in grant-peers with active SSE streams.
        if let Some(ref name) = cancelled_name
            && let Some((senders, cancel_rx)) = settle_opt
        {
            spawn_settle_task(name.clone(), senders, settle_window, cancel_rx);
        }
        Ok(())
    }

    /// Announce a name for a listen token.
    ///
    /// If `force` is true and the name is held by a live session (listen-token or
    /// minted agent), the holder is evicted immediately and the name is claimed.
    /// The evicted holder receives a `{"type":"service","event":"superseded",
    /// "reason":"name_reclaimed"}` event on their SSE stream.
    pub fn announce(&self, token: &str, name: &str) -> Result<AnnounceResult, Error> {
        let mut inner = self.lock();
        // gc_tokens() returns Branch-3 settle tasks that must be spawned after the lock
        // releases.  We use std::mem::take at each early-return path to fire any pending
        // settle tasks before returning, even if announce itself fails. (15-0002H)
        let mut gc_offline_events = inner.gc_tokens();
        let settle_window = inner.settle_window;

        // Validate token.
        if !inner.listen_tokens.contains_key(token) {
            drop(inner);
            for (n, senders, cancel_rx) in gc_offline_events {
                spawn_settle_task(n, senders, settle_window, cancel_rx);
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
            for (n, senders, cancel_rx) in gc_offline_events {
                spawn_settle_task(n, senders, settle_window, cancel_rx);
            }
            return Err(Error::TokenRevoked);
        }

        // 15-0029 addenda: announce is active use — reset the age-GC clock.
        inner.touch_token(token);

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
                ParticipantIdentity::valid(token),
                PresenceScope::GrantScoped,
            );
            // Cancel any pending settle task (participant re-announced while settling).
            inner.cancel_settle_online(name);
            drop(inner);
            for (n, senders, cancel_rx) in std::mem::take(&mut gc_offline_events) {
                spawn_settle_task(n, senders, settle_window, cancel_rx);
            }
            return Ok(AnnounceResult::Bound);
        }

        // FG-1: self-service takeover is removed. A name held by a DIFFERENT token — whether its
        // SSE is live or stale — is NAME_IN_USE. The same-token reconnect (live or stale self) was
        // already handled by the idempotent re-announce check above, so any holder reached here
        // is a different token. No cross-token takeover occurs. (BLOCKER-4)
        if let Some(existing_token) = inner.name_to_token.get(name).cloned() {
            if inner.listen_tokens.contains_key(&existing_token) {
                let resolution_stream = format!("/sessions/{}/events", name);
                drop(inner);
                for (n, senders, cancel_rx) in std::mem::take(&mut gc_offline_events) {
                    spawn_settle_task(n, senders, settle_window, cancel_rx);
                }
                return Ok(AnnounceResult::NameInUse { resolution_stream });
            } else {
                // Dangling name_to_token entry with no live token state — clean it up and
                // fall through to the orphan/identity guard below.
                inner.name_to_token.remove(name);
            }
        }

        // A minted agent holding this name is NAME_IN_USE (no takeover path; FG-1).
        if inner.registry.is_online(name) && !inner.name_to_token.contains_key(name) {
            let resolution_stream = format!("/sessions/{}/events", name);
            drop(inner);
            for (n, senders, cancel_rx) in std::mem::take(&mut gc_offline_events) {
                spawn_settle_task(n, senders, settle_window, cancel_rx);
            }
            return Ok(AnnounceResult::NameInUse { resolution_stream });
        }

        // BLOCKER-3: a registered identity with no active binding is orphaned (its token was
        // GC'd or revoked). Only a governor rebind (POST /register {name}) may reclaim it — any
        // other token claiming it is impersonation.
        if !inner.name_to_token.contains_key(name) && inner.identities.contains(name) {
            let resolution_stream = format!("/sessions/{}/events", name);
            drop(inner);
            for (n, senders, cancel_rx) in std::mem::take(&mut gc_offline_events) {
                spawn_settle_task(n, senders, settle_window, cancel_rx);
            }
            return Ok(AnnounceResult::NameInUse { resolution_stream });
        }

        // Claim the name (atomic under the Mutex).
        // FG-7: record the permanent identity on first announce. The roster entry is never
        // removed by GC/revoke/expiry; it is the authority the BLOCKER-3 orphan guard checks.
        inner.identities.insert(name.to_string());
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
            ParticipantState {
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
            ParticipantIdentity::valid(token),
            PresenceScope::GrantScoped,
        );

        // Governor breadcrumb: if this session was opened with a governor token as bearer,
        // enqueue the role breadcrumb once so the governor knows its responsibilities.
        let gov_notify_val = {
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

        // Presence push (AC1 / TR1): cancel any pending settle task and collect opted-in
        // grant-peer SSE senders before releasing the lock. (Force-reclaim is removed, so there
        // is no longer an eviction-offline path that would suppress this cancel.)
        inner.cancel_settle_online(name);
        let online_senders = inner.grant_peer_presence_senders(name);

        drop(inner);

        // Spawn settle tasks for Branch-3 GC evictions after the lock is released. (15-0002H)
        for (n, senders, cancel_rx) in gc_offline_events {
            spawn_settle_task(n, senders, settle_window, cancel_rx);
        }

        // Persist at announce so the name survives server restart even before the first grant.
        // gc_listen_tokens reaps listen-only (never-announced) tokens, so we only persist on announce.
        if let Some(store) = self.token_store.clone() {
            let tok = token.to_string();
            let name_s = name.to_string();
            self.db_write(async move {
                // FG-7: persist the permanent identity record (idempotent; created_at preserved).
                if let Err(e) = store.upsert_identity(&name_s).await {
                    eprintln!("WARNING: identity store write failed: {e}");
                }
                // BLOCKER-1 / FG-3: write identity=name, type="participant" (name col retired).
                if let Err(e) = store
                    .upsert_token(&tok, &name_s, "participant", None, None)
                    .await
                {
                    eprintln!("WARNING: token store write failed: {e}");
                }
            });
        }

        // Fire SSE NOTIFY for breadcrumb outside the lock.
        if let Some((sender, pending)) = gov_notify_val {
            let event = format!(r#"{{"type":"notify","pending":{}}}"#, pending);
            let _ = sender.send(event);
        }

        // Fire presence "online" event immediately to opted-in grant-peers.
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

        // 15-0029 addenda: a dequeue is active use — reset the age-GC clock.
        inner.touch_token(token);

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
    /// Also starts a settle task for opted-in grant-peers (AC4 / TR4).
    pub fn revoke_token(&self, token: &str, gov: &GovernorToken) -> Result<(), Error> {
        let (sender, settle_opt, settle_window, revoked_name) = {
            let mut inner = self.lock();
            inner.trust.validate_governor_token(gov)?;
            // Collect the bound name and begin settle BEFORE marking revoked.
            let revoked_name = inner.token_to_name.get(token).cloned();
            let settle_opt = if let Some(ref name) = revoked_name {
                inner.begin_settle_offline(name)
            } else {
                None
            };
            let settle_window = inner.settle_window;
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
            (sender, settle_opt, settle_window, revoked_name)
        };

        if let Some(tx) = sender {
            let event = r#"{"type":"service","event":"revoked"}"#.to_string();
            let _ = tx.send(event);
            // tx dropped here → receiver sees None → SSE stream ends.
        }
        // Spawn settle task for opted-in grant-peers.
        if let Some(ref name) = revoked_name
            && let Some((senders, cancel_rx)) = settle_opt
        {
            spawn_settle_task(name.clone(), senders, settle_window, cancel_rx);
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

    /// Resolve a bearer token to the agent's announced name.
    ///
    /// Works for both listen tokens and minted agent tokens.
    /// Returns `None` if the token is unknown, revoked, or has no announced name.
    pub fn name_for_bearer_token(&self, token: &str) -> Option<String> {
        let inner = self.lock();
        // listen token path — check revocation explicitly.
        if let Some(state) = inner.listen_tokens.get(token) {
            return if state.revoked {
                None
            } else {
                state.name.clone()
            };
        }
        // Minted agent token path — presence in token_to_name implies validity.
        inner.token_to_name.get(token).cloned()
    }

    /// Return `true` if any active grant exists (in either direction) between
    /// the agent identified by `from_token` and the agent named `to_name`.
    pub fn has_any_grant_with(&self, from_token: &str, to_name: &str) -> bool {
        let inner = self.lock();

        // Resolve caller identity + announced name.
        let Some(state) = inner.listen_tokens.get(from_token) else {
            return false;
        };
        if state.revoked {
            return false;
        }
        let (from_id, from_name): (String, Option<String>) =
            (from_token.to_string(), state.name.clone());

        // Resolve target: prefer the listen-token identity (which == token) if
        // the agent is a listen-flow agent; fall back to the minted identity.
        let (to_id, to_tok): (String, Option<String>) =
            if let Some(t_tok) = inner.name_to_token.get(to_name).cloned() {
                (t_tok.clone(), Some(t_tok))
            } else if let Some(agent) = inner.agents.get(to_name) {
                (agent.identity.clone(), None)
            } else {
                return false;
            };

        let to_id_str: &str = to_tok.as_deref().unwrap_or(&to_id);

        inner
            .trust
            .check_grant_directed_with_names(
                &from_id,
                to_id_str,
                from_name.as_deref(),
                Some(to_name),
            )
            .is_ok()
            || inner
                .trust
                .check_grant_directed_with_names(
                    to_id_str,
                    &from_id,
                    Some(to_name),
                    from_name.as_deref(),
                )
                .is_ok()
    }

    /// Send a message addressed by token (AC-S3). Looks up the announced name for `to_token`,
    /// then delegates to `send()` with the normal grant-check pipeline.
    /// Returns RECIPIENT_UNKNOWN if the token is revoked, GC'd, or not yet announced.
    pub fn send_to_token(
        &self,
        from_token: &ParticipantToken,
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
    ///
    /// Visibility rule (15-0028 / operator-final-rule):
    ///   A querier sees a target's presence if and only if:
    ///   (A) the querier holds a PERMISSIVE grant with the target, OR
    ///   (B) the querier currently shares a room with the target.
    ///   DENY-GRANT OVERRIDE: if a denial block exists for (querier_identity → target_name),
    ///   returns false regardless of grant or room membership.
    pub fn presence_for_token(&self, token: &str, target_name: &str) -> Result<bool, Error> {
        let inner = self.lock();
        // Validate querier token.
        let querier_state = inner.listen_tokens.get(token).ok_or(Error::TokenRejected)?;
        if querier_state.revoked {
            return Err(Error::TokenRevoked);
        }

        // Resolve querier name for grant/room checks.
        let querier_name = querier_state.name.clone();

        // Deny-grant override: hard block regardless of grant or room.
        // Key: (querier_identity → target_name); for listen-flow, identity == token.
        if inner.is_denial_active(token, target_name) {
            return Ok(false);
        }

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

        // Room membership check: alternative to grant for co-present participants.
        let shares_room = if let Some(ref qn) = querier_name {
            inner.room_store.shares_room(qn, target_name)
        } else {
            false
        };

        if !has_grant && !shares_room {
            // Neither grant nor shared room → target not visible.
            return Ok(false);
        }

        // Check if target is a listen-flow agent.
        if let Some(target_tok) = target_tok
            && let Some(target_state) = inner.listen_tokens.get(&target_tok)
        {
            if target_state.hidden {
                return Ok(false);
            }
            let sse_alive =
                ListenTokenState::is_sse_alive_in_hub(&target_tok, &inner.sse_connections);
            // AC2 fix: also check registry liveness so presence recovers after
            // an announce following an SSE drop (transient reconnect pattern).
            // Also consider settle window: still "present" while offline settle timer runs.
            let in_settle = inner.is_settle_pending(target_name);
            return Ok(sse_alive || inner.registry.is_online(target_name) || in_settle);
        }
        // Fall back to minted-agent lookup.
        let in_settle = inner.is_settle_pending(target_name);
        Ok(inner.is_online_effective(target_name) || in_settle)
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

    /// Test-only: register + announce a listen-flow participant with the given name,
    /// Test-only replacement for the deleted `mint_participant_token`: mints a raw
    /// listen-flow token via the live `register_participant()`, unbound to any name.
    ///
    /// IMPORTANT: unlike the deleted minted-agent path (where identity was an arbitrary
    /// string decoupled from the token, e.g. "id-alice"), listen-flow identity == token.
    /// Any grant approved for a participant produced by this helper must key on the
    /// returned token's string (`tok.0`), not a synthetic "id-alice"-style identity, or
    /// `send()`'s grant check will not match.
    /// Returns `Result` (even though registration cannot fail) so existing
    /// `.unwrap()`-suffixed call sites (a holdover from the old fallible
    /// `mint_participant_token`) keep compiling unchanged.
    fn test_mint(hub: &DeliveryHub) -> Result<ParticipantToken, Error> {
        Ok(ParticipantToken(hub.register_participant()))
    }

    /// Test-only replacement for the deleted `DeliveryHub::register`: binds `name` to a
    /// listen-flow token via the live `announce()`, then applies `scope`.
    ///
    /// NOTE: the live presence path (`presence_for_token`, 15-0028 / operator-final-rule)
    /// gates visibility uniformly by grant-or-shared-room for every agent, and separately
    /// consults only the listen token's `hidden` flag (set via `hide()`/`show()`) — it does
    /// NOT consult `Registry`'s stored `PresenceScope`, which only still matters for the
    /// pre-listen-flow (minted-agent) branch of `list_participants`/`presence_scope_effective`.
    /// So `Public` and `GrantScoped` are behaviorally identical here (both "not hidden";
    /// visibility still requires a grant or shared room) — only `Hidden` changes behavior.
    fn test_bind(
        hub: &DeliveryHub,
        name: &str,
        tok: &ParticipantToken,
        scope: PresenceScope,
    ) -> Result<(), Error> {
        hub.announce(&tok.0, name)?;
        match scope {
            PresenceScope::Hidden => hub.hide(&tok.0)?,
            PresenceScope::Public | PresenceScope::GrantScoped => hub.show(&tok.0)?,
        }
        Ok(())
    }

    /// Test-only long-poll dequeue built on the live listen-flow primitives (`dequeue()` +
    /// the per-name `Notify` handle). Production has no long-poll dequeue route (only the
    /// non-blocking `handle_dequeue` / `dequeue()`); this preserves pre-existing test
    /// timing/assertions without reintroducing the dead trust-only `long_poll_dequeue`
    /// that used to serve them (it validated only against `TrustChain.agents`, never
    /// `listen_tokens`, and had no HTTP route).
    async fn test_long_poll_dequeue(
        hub: &DeliveryHub,
        token: &ParticipantToken,
        max_wait: Duration,
    ) -> Result<DequeueOutcome, Error> {
        // Fetch the notify handle BEFORE the fast-path check: tokio::sync::Notify stores a
        // wake permit if notify_one() fires before we await, so no message is lost even if
        // it arrives between the fast-path check and the slow-path wait below.
        let notify_arc = {
            let inner = hub.lock();
            inner
                .token_to_name
                .get(&token.0)
                .and_then(|name| inner.agents.get(name))
                .map(|s| Arc::clone(&s.notify))
        };
        let (msg, _remaining) = hub.dequeue(&token.0, None)?;
        if let Some(msg) = msg {
            return Ok(DequeueOutcome::Message(msg));
        }
        if let Some(notify) = notify_arc {
            let _ = tokio::time::timeout(max_wait, notify.notified()).await;
        } else {
            tokio::time::sleep(max_wait).await;
        }
        let (msg, _remaining) = hub.dequeue(&token.0, None)?;
        Ok(match msg {
            Some(m) => DequeueOutcome::Message(m),
            None => DequeueOutcome::Empty,
        })
    }

    fn setup_hub_ab() -> (DeliveryHub, ParticipantToken, ParticipantToken) {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant(&gov, &tok_a.0, &tok_b.0, None).unwrap();
        (hub, tok_a, tok_b)
    }

    /// AC-MSG-1: A and B registered with valid grant; send → ACCEPTED; B's dequeue returns payload once.
    #[tokio::test]
    async fn ac_msg_1_send_accepted_dequeue_returns_payload_once() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let ack = hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None);
        assert!(matches!(ack, Ok(Ack::Accepted)));

        let outcome = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
            .await
            .unwrap();
        match outcome {
            DequeueOutcome::Message(m) => assert_eq!(m.payload.0, b"hi"),
            DequeueOutcome::Empty => panic!("expected a message"),
        }

        let second = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(20))
            .await
            .unwrap();
        assert!(matches!(second, DequeueOutcome::Empty));
    }

    /// AC-MSG-2: send to unregistered recipient → RecipientUnknown.
    #[tokio::test]
    async fn ac_msg_2_recipient_unknown() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();

        let result = hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None);
        assert!(matches!(result, Err(Error::RecipientUnknown)));

        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();
        let empty = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(20))
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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant(&gov, &tok_a.0, &tok_b.0, None).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        // AC2: message is in queue with correct from field
        let outcome = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
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
        let tok_a = test_mint(&hub).unwrap();
        test_mint(&hub).unwrap();
        hub.approve_grant(&gov, &tok_a.0, "id-bob", None).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant(&gov, &tok_a.0, &tok_b.0, None).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let m1 = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        let m2 = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        let m3 = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        let empty = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(20))
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        assert!(!hub.kick_pending_for("bob"));

        hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None)
            .unwrap();
        assert!(
            hub.kick_pending_for("bob"),
            "kick_pending should be set after queuing"
        );

        // Pop message
        let (msg, _remaining) = hub.dequeue(&tok_b.0, None).unwrap();
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
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let start = tokio::time::Instant::now();
        let result = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(50)).await;
        let elapsed = start.elapsed();

        assert!(matches!(result, Ok(DequeueOutcome::Empty)));
        assert!(elapsed >= Duration::from_millis(40));
        assert!(elapsed < Duration::from_millis(200));
    }

    /// AC-MSG-6: dequeue with invalid token → TokenRejected.
    ///
    /// Updated for 15-0030: the deleted trust-only `long_poll_dequeue` validated via
    /// `TrustChain.validate_participant_token`, which returned `AuthFailed` for an unknown
    /// token. The live listen-flow `dequeue()` (which `test_long_poll_dequeue` is built on)
    /// returns `TokenRejected` for an unknown token instead — the same error an unknown
    /// token gets everywhere else on the listen-flow path (`pending_count`,
    /// `latest_message_id`, `drain_queue`, etc.).
    #[tokio::test]
    async fn ac_msg_6_dequeue_invalid_token_returns_auth_failed() {
        let hub = make_hub(Duration::from_secs(30));
        let bad_token = ParticipantToken("not-a-real-token".into());

        let result = test_long_poll_dequeue(&hub, &bad_token, Duration::from_millis(10)).await;
        assert!(matches!(result, Err(Error::TokenRejected)));
    }

    /// AC-MSG-7: two messages in order are dequeued in send order, each once.
    #[tokio::test]
    async fn ac_msg_7_messages_delivered_in_send_order() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        hub.send(&tok_a, "bob", Payload(b"1".to_vec()), None, None)
            .unwrap();
        hub.send(&tok_a, "bob", Payload(b"2".to_vec()), None, None)
            .unwrap();

        let m1 = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(50))
            .await
            .unwrap();
        let m2 = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(50))
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

        let empty = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(20))
            .await
            .unwrap();
        assert!(matches!(empty, DequeueOutcome::Empty));
    }

    /// Queued message survives re-registration (messages not tied to a session channel).
    #[tokio::test]
    async fn queued_message_survives_reregistration() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        hub.send(&tok_a, "bob", Payload(b"survive".to_vec()), None, None)
            .unwrap();

        // Bob re-registers (new notify, same queue)
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let outcome = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
            None,
            ApproveGrantRequest {
                max_messages: Some(3),
                ..Default::default()
            },
        )
        .unwrap();
        // send() requires the sender to be announced (token_to_name), unlike the deleted
        // minted-agent path where an unannounced sender's from_name simply defaulted to "".
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
            None,
            ApproveGrantRequest {
                max_messages: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
            None,
            ApproveGrantRequest {
                max_messages: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let tok_a2 = ParticipantToken(tok_a.0.clone());

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

    fn setup_hub_ab_window() -> (
        DeliveryHub,
        GovernorToken,
        ParticipantToken,
        ParticipantToken,
    ) {
        use crate::trust::GrantDirection;
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        // AToB-only grant: only alice→bob is covered by a standing grant.
        // bob→alice has no grant, so bob must use reply windows to reach alice.
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        // Alice → Bob (grant path), opens window (bob, alice)
        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"hi".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));

        // Drain the message from bob's queue
        let _ = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(50)).await;

        // Bob → Alice via reply window
        assert!(matches!(
            hub.send(&tok_b, "alice", Payload(b"reply".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));

        let outcome = test_long_poll_dequeue(&hub, &tok_a, Duration::from_millis(50))
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        // Symmetric grant with opens_reply_window=true
        hub.approve_grant(&gov, &tok_a.0, &tok_b.0, None).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        let _gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        // AToB only: bob→alice has no standing grant, must use the reply window.
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
            None,
            ApproveGrantRequest {
                direction: Some(GrantDirection::AToB),
                opens_reply_window: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        // Open window (bob, alice)
        hub.send(&tok_a, "bob", Payload(b"init".to_vec()), None, None)
            .unwrap();

        let tok_b2 = ParticipantToken(tok_b.0.clone());
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

    fn setup_hub_brief() -> (
        DeliveryHub,
        GovernorToken,
        ParticipantToken,
        ParticipantToken,
    ) {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        // No grant between alice and bob
        (hub, gov, tok_a, tok_b)
    }

    /// No grant + no window + no reason → send returns NoGrant; request_grant creates ConnectionRequest.
    #[test]
    fn no_grant_creates_connection_request() {
        let (hub, _gov, tok_a, _tok_b) = setup_hub_brief();
        // send() requires the sender to be announced (token_to_name), unlike the deleted
        // minted-agent path where an unannounced sender's from_name simply defaulted to "".
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        hub.inner.lock().unwrap().agents.insert(
            "bob".into(),
            ParticipantState {
                identity: "id-bob".into(),
                notify: Arc::new(tokio::sync::Notify::new()),
            },
        );
        hub.inner
            .lock()
            .unwrap()
            .registry
            .register(
                "bob",
                ParticipantIdentity::valid("id-bob"),
                PresenceScope::Public,
            )
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.inner
            .lock()
            .unwrap()
            .trust
            .set_governor_online(&gov, false);
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Governor approves first (PendingGovernor → PendingRecipient).
        assert!(matches!(
            hub.approve_grant_request(&gov.0, &request_id, None),
            Ok(ApproveStatus::PendingRecipient)
        ));

        // Drain the grant_request event queued to Bob.
        test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Governor denies (live path: respond_to_connection_request was superseded by the
        // approve/deny/hold_grant_request trio operating on the same connection_requests map).
        let outcome = hub.deny_grant_request(&gov.0, &request_id, "declined", None);
        assert!(outcome.is_ok());

        // Alice's queue should have a GRANT_DENIED system event.
        let msg = test_long_poll_dequeue(&hub, &tok_a, Duration::from_millis(100))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.from_name, "system");
                assert_eq!(m.event_type.as_deref(), Some("grant_denied"));
            }
            DequeueOutcome::Empty => panic!("expected GRANT_DENIED message for sender"),
        }
    }

    /// hold TTL expiry → MediationUnavailable on resolve.
    #[tokio::test]
    async fn hold_ttl_expiry_returns_mediation_unavailable() {
        let (hub, gov, tok_a, tok_b) = setup_hub_brief();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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

    fn setup_hub_inspect() -> (
        DeliveryHub,
        GovernorToken,
        ParticipantToken,
        ParticipantToken,
    ) {
        use crate::trust::{GrantDirection, GrantMediation};
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::Public).unwrap();

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
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::Public).unwrap();

        let mediation_id = match hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None) {
            Ok(Ack::PendingMediation { mediation_id }) => mediation_id,
            other => panic!("expected PendingMediation, got {:?}", other),
        };

        assert!(matches!(
            hub.resolve_mediation(&gov, &mediation_id, MediationDecision::Approve),
            Ok(MediationResult::Delivered { .. })
        ));

        let msg = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::Public).unwrap();

        let mediation_id = match hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None) {
            Ok(Ack::PendingMediation { mediation_id }) => mediation_id,
            other => panic!("expected PendingMediation, got {:?}", other),
        };

        assert!(matches!(
            hub.resolve_mediation(&gov, &mediation_id, MediationDecision::Block),
            Ok(MediationResult::Blocked)
        ));

        let empty = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(20))
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::Public).unwrap();

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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
            None,
            ApproveGrantRequest {
                mediation: Some(GrantMediation::Notify),
                direction: Some(GrantDirection::AToB),
                ..Default::default()
            },
        )
        .unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::Public).unwrap();

        let mut events_rx = hub.subscribe_gov_events();

        let result = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
        assert!(matches!(result, Ok(Ack::Accepted)));

        let msg = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
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
        let tok_a = test_mint(&hub).unwrap();

        // Bob is a listen-flow client — register first, then open_listen.
        let bob_token = hub.register_participant();
        let (bob_token, rx1) = hub
            .open_listen(Some(&bob_token), None, Some("bob"), None, false, false)
            .unwrap();
        // Alice is a regular agent with a Notify grant to Bob.
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
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
        let (returned_token, mut rx2) = hub
            .open_listen(Some(&bob_token), None, None, None, true, false)
            .unwrap();
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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::Public).unwrap();

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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant_req(
            &gov,
            &tok_a.0,
            &tok_b.0,
            None,
            ApproveGrantRequest {
                mediation: Some(GrantMediation::Bypass),
                ..Default::default()
            },
        )
        .unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::Public).unwrap();

        assert!(matches!(
            hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None),
            Ok(Ack::Accepted)
        ));
    }

    /// Normal message omits reason field.
    #[tokio::test]
    async fn normal_message_omits_reason() {
        let (hub, tok_a, tok_b) = setup_hub_ab();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None)
            .unwrap();

        let msg = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(50))
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
        let _gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        // No grant between alice and bob
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let result = hub.presence_for_token(&tok_b.0, "alice");
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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant(&gov, &tok_a.0, &tok_b.0, None).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let result = hub.presence_for_token(&tok_b.0, "alice");
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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant(&gov, &tok_a.0, &tok_b.0, None).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::Hidden).unwrap();

        let presence = hub.presence_for_token(&tok_a.0, "bob");
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

    // NOTE: the old `self_query_always_returns_true_status` test (self-query on a hidden
    // agent always reports online) tested a `TrustChain.agents`-era `presence_scoped`
    // special case that has no live equivalent: `presence_for_token` (15-0028 /
    // operator-final-rule) gates ALL presence queries — including self-queries — uniformly
    // by grant-or-shared-room, with no self-query bypass. Removed rather than "fixed" since
    // the behavior it asserted no longer exists on the reachable path (confirmed by reading
    // presence_for_token in full: no querier==target special case).

    // ── SSE liveness tests (AC1–AC4) ─────────────────────────────────────────

    /// AC1: Agent with active SSE connection remains online after 2× liveness window elapses.
    #[tokio::test]
    async fn ac_sse_1_active_sse_keeps_agent_online_after_liveness_lapse() {
        let hub = make_hub(Duration::from_millis(10));
        let _gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();

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
        let _gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();

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
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant(&gov, &tok_a.0, &tok_b.0, None).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        // presence_for_token's listen-flow branch checks the token-keyed `sse_connections`
        // map (populated by open_listen), not the name-keyed `active_sse_connections` that
        // `sse_open()`/`presence()` use — so simulate an active SSE stream the same way
        // open_listen does, directly under the lock, rather than via a real SSE connection.
        {
            let mut inner = hub.lock();
            *inner.sse_connections.entry(tok_a.0.clone()).or_insert(0) += 1;
        }

        tokio::time::sleep(Duration::from_millis(30)).await;

        let result = hub.presence_for_token(&tok_b.0, "alice");
        assert!(
            matches!(result, Ok(true)),
            "agent with active SSE should appear online to grant-holder"
        );
    }

    /// AC4: Agent without SSE goes offline after liveness window (backward compatibility).
    #[tokio::test]
    async fn ac_sse_4_no_sse_goes_offline_after_liveness_window() {
        let hub = make_hub(Duration::from_millis(10));
        let _gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::Public).unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(
            !hub.presence("alice"),
            "agent without SSE should go offline after liveness window"
        );
    }

    // ── Agent list tests (AC1–AC5 / Feature 1) ───────────────────────────────

    /// AC1 + AC5: governor_list_participants — register 2 agents, list shows both with correct fields.
    #[test]
    fn governor_list_participants() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let agents = hub.list_participants(&gov).unwrap();
        assert_eq!(agents.len(), 2);

        // Listen-flow identity == token (unlike the deleted minted-agent path).
        let alice = agents.iter().find(|a| a.name == "alice").unwrap();
        assert_eq!(alice.identity, tok_a.0);
        assert_eq!(alice.status, "online");

        let bob = agents.iter().find(|a| a.name == "bob").unwrap();
        assert_eq!(bob.identity, tok_b.0);
        assert_eq!(bob.status, "online");
    }

    /// AC2: agent token → Forbidden (maps to 403 FORBIDDEN at HTTP layer).
    #[test]
    fn list_participants_rejects_participant_token() {
        // Updated for 15-0030: see ac_gov_grants_6_7_8_auth_errors's AC8 comment — the
        // Forbidden-for-agent-token distinction was specific to the now-deleted
        // TrustChain.agents-backed minted agent token; a listen token here yields AuthFailed.
        let hub = make_hub(Duration::from_secs(30));
        let _gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();

        let fake_gov = GovernorToken(tok_a.0.clone());
        assert!(
            matches!(hub.list_participants(&fake_gov), Err(Error::AuthFailed)),
            "participant token must be rejected with AuthFailed for list_participants"
        );
    }

    /// AC3: hidden agents appear offline in the list even when actually online.
    #[test]
    fn list_participants_hidden_appears_offline() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::Hidden).unwrap();
        hub.sse_open("alice"); // ensure truly "online" per SSE liveness

        let agents = hub.list_participants(&gov).unwrap();
        let alice = agents.iter().find(|a| a.name == "alice").unwrap();
        assert_eq!(
            alice.status, "offline",
            "hidden agent must appear offline even with SSE"
        );

        hub.sse_close("alice");
    }

    // ── Token refresh tests (AC6–AC10 / Feature 2) ───────────────────────────
    //
    // AC6-AC8 (self-service `refresh_participant_token`) and AC10 (governor
    // `governor_refresh_participant_token`) tested the deleted minted-agent token-rotation
    // methods. Self-service rotation has no live equivalent (listen-flow agents keep one
    // token for the session; there is no "rotate my own token" primitive). The governor-forced
    // case (AC10) IS live — as `issue_participant_token(gov, Some(name))`, the atomic
    // invalidate-old / mint-new / rebind-to-name "governor rebind" — and it already has
    // dedicated coverage below (test_grants_survive_rebind,
    // test_rebind_invalidates_old_token_new_can_announce,
    // test_issue_participant_token_unknown_name, test_issue_participant_token_requires_governor).
    // So AC6-AC8, AC10, and the token-rotation-specific `agent_refresh_preserves_registration`
    // are removed rather than converted: there is nothing left to convert them to that isn't
    // already tested.

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

    // ── Bilateral consent tests (AC1–AC8 for task 20-9008) ───────────────────

    fn setup_hub_no_grant() -> (
        DeliveryHub,
        GovernorToken,
        ParticipantToken,
        ParticipantToken,
    ) {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        (hub, gov, tok_a, tok_b)
    }

    /// AC1: send to unregistered → RECIPIENT_UNKNOWN (no connection request created).
    #[test]
    fn ac_bilateral_1_send_to_unregistered_recipient_unknown() {
        let (hub, _gov, tok_a, _tok_b) = setup_hub_no_grant();
        // Announce alice properly (rather than poking `inner.agents` directly) so
        // token_to_name is populated too — send() requires the sender to be announced.
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        let msg = test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Governor approves first (PendingGovernor → PendingRecipient).
        assert!(matches!(
            hub.approve_grant_request(&gov.0, &request_id, None),
            Ok(ApproveStatus::PendingRecipient)
        ));

        // Drain the grant_request event queued to Bob.
        test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Live path: deny_grant_request supersedes respond_to_connection_request(approve=false).
        assert!(
            hub.deny_grant_request(&gov.0, &request_id, "declined", None)
                .is_ok()
        );

        // Alice's queue has GRANT_DENIED
        let msg = test_long_poll_dequeue(&hub, &tok_a, Duration::from_millis(100))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.from_name, "system");
                assert_eq!(m.event_type.as_deref(), Some("grant_denied"));
                let v: serde_json::Value = serde_json::from_slice(&m.payload.0).unwrap();
                assert_eq!(v["type"], "grant_denied");
            }
            DequeueOutcome::Empty => panic!("expected GRANT_DENIED for sender"),
        }
    }

    /// AC5: Alice (recipient) denies → message dropped, sender gets CONNECTION_DENIED.
    #[tokio::test]
    async fn ac_bilateral_5_alice_deny_connection_denied_to_sender() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_no_grant();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

        let request_id = hub
            .request_grant(&tok_a.0, "bob", None, None)
            .expect("request_grant must succeed");

        // Bob (recipient) denies (live path: deny_grant_request accepts either the governor
        // or the recipient's listen token as caller — see its is_recipient check).
        assert!(
            hub.deny_grant_request(&tok_b.0, &request_id, "declined", None)
                .is_ok()
        );

        // Alice gets GRANT_DENIED
        let msg = test_long_poll_dequeue(&hub, &tok_a, Duration::from_millis(100))
            .await
            .unwrap();
        match msg {
            DequeueOutcome::Message(m) => {
                assert_eq!(m.event_type.as_deref(), Some("grant_denied"));
            }
            DequeueOutcome::Empty => panic!("expected GRANT_DENIED for sender"),
        }
    }

    /// AC6: reason field from request_grant is included in the grant_request governor event.
    #[tokio::test]
    async fn ac_bilateral_6_reason_included_in_event() {
        let (hub, _gov, tok_a, tok_b) = setup_hub_no_grant();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();

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
        let denial_blocks = store
            .load_denial_blocks()
            .await
            .expect("load denial blocks");
        let identities = store.load_identities().await.expect("load identities");
        DeliveryHub::new_with_persisted_state(
            lapse,
            store,
            tokens,
            grants,
            denial_blocks,
            identities,
        )
    }

    // ── 15-0029 S1: schema / migration / startup ──────────────────────────────

    use crate::persistence::TokenStore;

    async fn build_hub_from_store(store: Arc<TokenStore>, lapse: Duration) -> DeliveryHub {
        let tokens = store.load_tokens().await.expect("load tokens");
        let grants = store.load_grants().await.expect("load grants");
        let denial = store.load_denial_blocks().await.expect("load denial");
        let identities = store.load_identities().await.expect("load identities");
        DeliveryHub::new_with_persisted_state(lapse, store, tokens, grants, denial, identities)
    }

    /// S1-AC-1: the `identities` table exists on a fresh DB.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_identities_table_created() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store.upsert_identity("Scout7").await.expect("upsert id");
        let ids = store.load_identities().await.expect("load");
        assert!(ids.iter().any(|i| i.name == "Scout7"));
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-2: running migrate twice does not error (idempotent).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_migrate_idempotent() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store.migrate_for_test().await.expect("migrate 2");
        store.migrate_for_test().await.expect("migrate 3");
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-3: a listen-type token with a name backfills the identities table on migration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_identities_populated_from_listen_tokens() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("111111111", "111111111", "listen", Some("Scout7"))
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        let ids = store.load_identities().await.expect("load");
        assert!(ids.iter().any(|i| i.name == "Scout7"));
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-4: listen-type rows are renamed to participant.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_listen_renamed_to_participant() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("222222222", "222222222", "listen", Some("Scout7"))
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        assert_eq!(store.count_tokens_by_type("listen").await.unwrap(), 0);
        assert_eq!(
            store.token_type_of("222222222").await.unwrap().as_deref(),
            Some("participant")
        );
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-5: v2-type rows become participant (via v2→listen→participant). No dead v2→participant.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_v2_renamed_via_listen() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("333333333", "333333333", "v2", Some("Unit12"))
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        assert_eq!(store.count_tokens_by_type("v2").await.unwrap(), 0);
        assert_eq!(
            store.token_type_of("333333333").await.unwrap().as_deref(),
            Some("participant")
        );
        let ids = store.load_identities().await.expect("load");
        assert!(ids.iter().any(|i| i.name == "Unit12"));
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-6: agent-type rows are deleted on migration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_agent_tokens_purged() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("agent-1", "id-alpha", "agent", None)
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        assert_eq!(store.count_tokens_by_type("agent").await.unwrap(), 0);
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-7: participant tokens with identity == token (never announced) are purged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_orphan_tokens_purged() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        // identity == token, no name → orphan.
        store
            .seed_raw_token("444444444", "444444444", "listen", None)
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        assert_eq!(store.token_identity("444444444").await.unwrap(), None);
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-7b: a named token survives the Step-6 purge — including a numeric-name edge case.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_named_token_survives_purge() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        // Correct post-FG3 row: identity already holds the name.
        store
            .seed_raw_token("555555555", "Scout7", "listen", None)
            .await
            .expect("seed named");
        // Adversarial edge case: the name string is itself numeric (but != the token).
        store
            .seed_raw_token("666666666", "42", "listen", None)
            .await
            .expect("seed numeric-named");
        store.migrate_for_test().await.expect("migrate");
        assert_eq!(
            store.token_identity("555555555").await.unwrap().as_deref(),
            Some("Scout7")
        );
        assert_eq!(
            store.token_identity("666666666").await.unwrap().as_deref(),
            Some("42")
        );
        // Both name bindings must load into memory.
        let hub = build_hub_from_store(Arc::new(store), Duration::from_secs(30)).await;
        let inner = hub.lock();
        assert_eq!(
            inner.name_to_token.get("Scout7").map(String::as_str),
            Some("555555555")
        );
        assert_eq!(
            inner.name_to_token.get("42").map(String::as_str),
            Some("666666666")
        );
        drop(inner);
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-9: startup partition matches participant tokens; they load into listen_tokens.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_startup_partition_on_participant() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("777777777", "Scout7", "participant", None)
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        let hub = build_hub_from_store(Arc::new(store), Duration::from_secs(30)).await;
        let inner = hub.lock();
        assert!(inner.listen_tokens.contains_key("777777777"));
        drop(inner);
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-10: startup restores the name binding from the identity column (not the name column).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_startup_reads_identity_for_name() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("888888888", "Scout7", "participant", None)
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        let hub = build_hub_from_store(Arc::new(store), Duration::from_secs(30)).await;
        let inner = hub.lock();
        assert_eq!(
            inner.name_to_token.get("Scout7").map(String::as_str),
            Some("888888888")
        );
        assert_eq!(
            inner.token_to_name.get("888888888").map(String::as_str),
            Some("Scout7")
        );
        drop(inner);
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-11: the in-memory identities HashSet is populated from the identities table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_identities_hashset_loaded() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store.upsert_identity("Scout7").await.expect("seed id");
        let hub = build_hub_from_store(Arc::new(store), Duration::from_secs(30)).await;
        let inner = hub.lock();
        assert!(inner.identities.contains("Scout7"));
        drop(inner);
        let _ = std::fs::remove_file(&db);
    }

    async fn assert_announce_writes_identity_not_token() {
        let db = unique_test_db();
        let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
        let reg = hub.register_participant();
        let (tok, _rx) = hub
            .open_listen(Some(&reg), None, None, None, false, false)
            .unwrap();
        hub.announce(&tok, "Alice").unwrap();
        let store = hub.token_store.clone().unwrap();
        assert_eq!(
            store.token_identity(&tok).await.unwrap().as_deref(),
            Some("Alice")
        );
        assert_eq!(
            store.token_type_of(&tok).await.unwrap().as_deref(),
            Some("participant")
        );
        let _ = std::fs::remove_file(&db);
    }

    /// S1-AC-12 / EPIC-AC-23: announce writes identity=name and token_type="participant".
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s1_upsert_writes_participant_type_and_name_identity() {
        assert_announce_writes_identity_not_token().await;
    }

    /// EPIC-AC-23 alias: after announce, the token row stores identity=name (not the token).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upsert_token_writes_identity_not_token() {
        assert_announce_writes_identity_not_token().await;
    }

    /// EPIC-AC-10: an agent-type seed row leaves zero agent rows after startup migration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_no_agent_rows_after_startup() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("agent-7", "id-x", "agent", None)
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        assert_eq!(store.count_tokens_by_type("agent").await.unwrap(), 0);
        let _ = std::fs::remove_file(&db);
    }

    /// EPIC-AC-11: a listen-type seed row leaves zero listen rows after startup migration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_no_listen_rows_after_startup() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("999999999", "Scout7", "listen", None)
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        assert_eq!(store.count_tokens_by_type("listen").await.unwrap(), 0);
        let _ = std::fs::remove_file(&db);
    }

    /// EPIC-AC-12: a participant token with a name populates the identities table on startup.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_identities_table_populated_on_startup() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("121212121", "Alice", "participant", None)
            .await
            .expect("seed");
        store.migrate_for_test().await.expect("migrate");
        let ids = store.load_identities().await.expect("load");
        assert!(ids.iter().any(|i| i.name == "Alice"));
        let _ = std::fs::remove_file(&db);
    }

    /// EPIC-AC-24: a participant session is restored (token + name binding) after restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_participant_sessions_restored_after_restart() {
        let db = unique_test_db();
        let tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let reg = hub.register_participant();
            let (tok, _rx) = hub
                .open_listen(Some(&reg), None, None, None, false, false)
                .unwrap();
            hub.announce(&tok, "Alice").unwrap();
            tok
        };
        // Restart.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        assert!(
            hub2.validate_token(&tok).is_ok(),
            "token must be valid after restart"
        );
        {
            let inner = hub2.lock();
            assert_eq!(
                inner.name_to_token.get("Alice").map(String::as_str),
                Some(tok.as_str()),
                "name binding 'Alice' must be restored"
            );
        }
        // A message addressed to Alice must route (not RecipientUnknown).
        let _gov = hub2.install_governor(None);
        let sender = test_mint(&hub2).unwrap();
        test_bind(&hub2, "sender", &sender, PresenceScope::GrantScoped).unwrap();
        let res = hub2.send(&sender, "Alice", Payload(b"hi".to_vec()), None, None);
        assert!(
            !matches!(res, Err(Error::RecipientUnknown)),
            "message to restored 'Alice' must not be RecipientUnknown; got {:?}",
            res
        );
        let _ = std::fs::remove_file(&db);
    }

    // ── 15-0029 S4: force removal + identity guards ────────────────────────────

    /// EPIC-AC-6 / S4-AC-2: a live name-holder is never evicted by another token.
    #[test]
    fn test_announce_live_holder_not_evicted() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let reg_a = hub.register_participant();
        let (tok_a, _rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        hub.announce(&tok_a, "Alice").unwrap();

        let reg_b = hub.register_participant();
        let (tok_b, _rx_b) = hub
            .open_listen(Some(&reg_b), None, None, None, false, false)
            .unwrap();
        assert!(
            matches!(
                hub.announce(&tok_b, "Alice"),
                Ok(AnnounceResult::NameInUse { .. })
            ),
            "live holder A must not be evicted; B must get NAME_IN_USE"
        );
        assert_eq!(
            hub.validate_token(&tok_a).unwrap().as_deref(),
            Some("Alice"),
            "A must still hold Alice"
        );
        assert_eq!(
            hub.lock().name_to_token.get("Alice").map(String::as_str),
            Some(tok_a.as_str())
        );
    }

    /// EPIC-AC-5 / S4-AC-1: a force flag cannot evict a holder (force argument is gone).
    #[test]
    fn test_announce_force_body_ignored_returns_409() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let reg_a = hub.register_participant();
        let (tok_a, _rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        hub.announce(&tok_a, "Alice").unwrap();
        let reg_b = hub.register_participant();
        let (tok_b, _rx_b) = hub
            .open_listen(Some(&reg_b), None, None, None, false, false)
            .unwrap();
        // The HTTP layer ignores any `force` field; the hub method has no force parameter at all.
        assert!(matches!(
            hub.announce(&tok_b, "Alice"),
            Ok(AnnounceResult::NameInUse { .. })
        ));
        assert_eq!(
            hub.validate_token(&tok_a).unwrap().as_deref(),
            Some("Alice")
        );
    }

    /// EPIC-AC-20 / S4-AC-7: an orphaned registered name (token GC'd/revoked) → NAME_IN_USE.
    #[test]
    fn test_orphaned_name_returns_name_in_use() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let reg_a = hub.register_participant();
        let (tok_a, _rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        hub.announce(&tok_a, "Alice").unwrap();
        // Simulate revoke + GC: clear A's live binding and token, KEEP the identity record.
        {
            let mut inner = hub.lock();
            inner.name_to_token.remove("Alice");
            inner.token_to_name.remove(&tok_a);
            inner.agents.remove("Alice");
            inner.listen_tokens.remove(&tok_a);
            inner.sse_connections.remove(&tok_a);
            assert!(inner.identities.contains("Alice"));
        }
        let reg_b = hub.register_participant();
        let (tok_b, _rx_b) = hub
            .open_listen(Some(&reg_b), None, None, None, false, false)
            .unwrap();
        assert!(
            matches!(
                hub.announce(&tok_b, "Alice"),
                Ok(AnnounceResult::NameInUse { .. })
            ),
            "orphaned registered name must be NAME_IN_USE (governor rebind required)"
        );
    }

    /// EPIC-AC-21 / S4-AC-8: a stale holder is not evicted across tokens.
    #[test]
    fn test_stale_cross_token_eviction_blocked() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let reg_a = hub.register_participant();
        let (tok_a, rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        hub.announce(&tok_a, "Alice").unwrap();
        // Make A's SSE stale (drop the connection; the name binding remains).
        drop(rx_a);
        hub.close_listen(&tok_a);

        let reg_b = hub.register_participant();
        let (tok_b, _rx_b) = hub
            .open_listen(Some(&reg_b), None, None, None, false, false)
            .unwrap();
        assert!(
            matches!(
                hub.announce(&tok_b, "Alice"),
                Ok(AnnounceResult::NameInUse { .. })
            ),
            "stale cross-token takeover must be blocked (BLOCKER-4)"
        );
        assert_eq!(
            hub.validate_token(&tok_a).unwrap().as_deref(),
            Some("Alice"),
            "stale A's binding must survive"
        );
    }

    /// EPIC-AC-22 / S4-AC-9: same-token stale reconnect succeeds.
    #[test]
    fn test_same_token_stale_reconnect_succeeds() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let reg_a = hub.register_participant();
        let (tok_a, rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        hub.announce(&tok_a, "Alice").unwrap();
        drop(rx_a);
        hub.close_listen(&tok_a); // stale SSE; binding remains
        assert!(
            matches!(hub.announce(&tok_a, "Alice"), Ok(AnnounceResult::Bound)),
            "same-token stale reconnect must succeed"
        );
    }

    fn read_crate_src(rel: &str) -> String {
        std::fs::read_to_string(format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel))
            .unwrap_or_else(|e| panic!("read {rel}: {e}"))
    }

    /// S4-AC-3: AnnounceBody has no `force` field.
    #[test]
    fn test_s4_no_force_field_in_announcebody() {
        let src = read_crate_src("src/http.rs");
        let start = src
            .find("struct AnnounceBody")
            .expect("AnnounceBody struct present");
        let rest = &src[start..];
        let end = rest.find('}').expect("struct close brace");
        let body = &rest[..end];
        assert!(
            !body.contains("force"),
            "AnnounceBody must not declare a force field: {body}"
        );
    }

    /// EPIC-AC-17 / S4-AC-4 / S4-AC-6: no force-eviction / force-reclaim in the announce path.
    #[test]
    fn test_no_force_announce_code_path() {
        let src = read_crate_src("src/delivery.rs");
        let start = src.find("pub fn announce(").expect("announce fn present");
        let rest = &src[start..];
        let end = rest
            .find("pub fn dequeue(")
            .expect("next fn after announce");
        let announce_body = &rest[..end];
        for line in announce_body.lines() {
            let has_force = line.contains("force");
            let has_evict_or_reclaim = line.contains("evict") || line.contains("reclaim");
            assert!(
                !(has_force && has_evict_or_reclaim),
                "announce() must contain no force-eviction/force-reclaim line: {line}"
            );
        }
        // The HTTP announce handler must not extract a force field from the body.
        let http = read_crate_src("src/http.rs");
        let hstart = http
            .find("async fn handle_announce(")
            .expect("handle_announce present");
        assert!(
            !http[hstart..].contains("body.get(\"force\")"),
            "handle_announce must not extract a force field"
        );
    }

    /// S4-AC-11 / EPIC-AC-23 grep: no participant/listen upsert writes identity==token.
    /// (Governor tokens legitimately store identity=token — a governor's identity IS its token.)
    #[test]
    fn test_s4_no_identity_equals_token_upsert() {
        let src = read_crate_src("src/delivery.rs");
        for line in src.lines() {
            let identity_eq_token = line.contains("upsert_token(&tok, &tok")
                || line.contains("upsert_token(&tok2, &tok2")
                || line.contains("upsert_token(&t, &t");
            let participantish = line.contains("\"listen\"") || line.contains("\"participant\"");
            assert!(
                !(identity_eq_token && participantish),
                "no participant/listen identity==token upsert may remain: {line}"
            );
        }
        // And specifically the spec's listen-type form must be gone entirely.
        assert!(!src.contains("upsert_token(&tok, &tok, \"listen\""));
    }

    // ── 15-0029 S3: governor rebind (issue_participant_token) ──────────────────

    /// EPIC-AC-8 / S3-AC-7: the identities row's created_at is unchanged across a governor rebind.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_identity_record_unchanged_after_rebind() {
        let db = unique_test_db();
        let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
        let gov = hub.install_governor(None);
        let reg = hub.register_participant();
        let (t1, _rx) = hub
            .open_listen(Some(&reg), None, None, None, false, false)
            .unwrap();
        hub.announce(&t1, "Alice").unwrap();
        let store = hub.token_store.clone().unwrap();
        let before = store.identity_created_at("Alice").await.unwrap();
        assert!(
            before.is_some(),
            "identity record must exist after announce"
        );

        // Governor rebind.
        let (t2, name) = hub.issue_participant_token(&gov, Some("Alice")).unwrap();
        assert_eq!(name.as_deref(), Some("Alice"));
        assert_ne!(t2, t1);

        let after = store.identity_created_at("Alice").await.unwrap();
        assert_eq!(before, after, "created_at must be unchanged by rebind");
        let _ = std::fs::remove_file(&db);
    }

    /// EPIC-AC-9 / S3-AC-8: a name-keyed grant survives a governor rebind of one party's token.
    #[test]
    fn test_grants_survive_rebind() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let gov = hub.install_governor(None);

        let reg_a = hub.register_participant();
        let (t1, _ra) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        hub.announce(&t1, "Alice").unwrap();
        let reg_b = hub.register_participant();
        let (tb, _rb) = hub
            .open_listen(Some(&reg_b), None, None, None, false, false)
            .unwrap();
        hub.announce(&tb, "Bob").unwrap();
        // Grant (Alice, Bob) — names auto-filled from token_to_name.
        hub.approve_grant(&gov, &t1, &tb, None).unwrap();

        // Rebind Alice → T2 (T1 invalidated).
        let (t2, _name) = hub.issue_participant_token(&gov, Some("Alice")).unwrap();
        assert!(
            hub.validate_token(&t1).is_err(),
            "old token must be invalid"
        );

        // The grant (keyed on names) still authorizes Alice→Bob from the new token.
        assert!(
            matches!(
                hub.send(
                    &ParticipantToken(t2.clone()),
                    "Bob",
                    Payload(b"hi".to_vec()),
                    None,
                    None
                ),
                Ok(Ack::Accepted)
            ),
            "grant must survive rebind"
        );
    }

    /// S3 rebind invalidates the old token and the new token can announce (hub-level EPIC-AC-7).
    #[test]
    fn test_rebind_invalidates_old_token_new_can_announce() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let reg = hub.register_participant();
        let (t1, _rx) = hub
            .open_listen(Some(&reg), None, None, None, false, false)
            .unwrap();
        hub.announce(&t1, "Alice").unwrap();

        let (t2, _name) = hub.issue_participant_token(&gov, Some("Alice")).unwrap();
        assert_ne!(t2, t1);
        // Old token: announce now rejected.
        assert!(matches!(
            hub.announce(&t1, "Alice"),
            Err(Error::TokenRejected)
        ));
        // New token: bound, announce is idempotent Bound.
        assert!(matches!(
            hub.announce(&t2, "Alice"),
            Ok(AnnounceResult::Bound)
        ));
    }

    /// S3-AC-9: governor rebind of an unknown name → RecipientUnknown (404).
    #[test]
    fn test_issue_participant_token_unknown_name() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        assert!(matches!(
            hub.issue_participant_token(&gov, Some("Nobody")),
            Err(Error::RecipientUnknown)
        ));
    }

    /// S3: issue_participant_token requires a valid governor (a participant token → AuthFailed).
    #[test]
    fn test_issue_participant_token_requires_governor() {
        let hub = DeliveryHub::new(Duration::from_secs(30));
        let forged = GovernorToken("not-a-governor".to_string());
        assert!(hub.issue_participant_token(&forged, None).is_err());
    }

    // ── 15-0029 S5: admin reset durability ─────────────────────────────────────

    /// S5-AC-6: admin reset commits revoke + install in one transaction; after a restart the new
    /// governor is durable and the old one is gone (no permanently-open bootstrap window).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s5_admin_reset_single_transaction() {
        let db = unique_test_db();
        let (old_gov, new_gov) = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let g1 = hub.install_governor(None);
            let g2 = hub.admin_reset_governor();
            assert_ne!(g1.0, g2.0);
            // In-memory: old revoked, new valid.
            assert!(hub.validate_governor_token(&g1).is_err());
            assert!(hub.validate_governor_token(&g2).is_ok());
            (g1, g2)
        };

        // Restart from the same DB: the new governor is durable; the old is gone.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        assert!(
            hub2.validate_governor_token(&new_gov).is_ok(),
            "new governor must be durable after restart"
        );
        assert!(
            hub2.validate_governor_token(&old_gov).is_err(),
            "old governor must be gone after restart"
        );
        assert!(
            hub2.has_active_governor(),
            "a governor must exist after restart (no permanently-open bootstrap)"
        );
        let _ = std::fs::remove_file(&db);
    }

    // ── 15-0029 S7: migration seal (startup parity) ────────────────────────────

    /// S7-AC-1: the migration emits a `sim_migrate:` count line with every field present.
    #[test]
    fn test_s7_migration_log_counts() {
        let src = read_crate_src("src/persistence.rs");
        let i = src
            .find("sim_migrate:")
            .expect("migration log line present");
        let window = &src[i..i + 200];
        for field in [
            "identities_created",
            "listen_renamed",
            "agent_purged",
            "orphan_purged",
            "name_col_dropped",
        ] {
            assert!(
                window.contains(field),
                "sim_migrate log must include {field}"
            );
        }
    }

    /// S7-AC-2: after loading a migrated DB, the in-memory TrustChain holds no agent-type tokens
    /// (Step-5 purge means none are loaded). (The structural removal of the agents map is S2.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s7_no_agent_in_trustchain() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        store
            .seed_raw_token("agent-3", "id-x", "agent", None)
            .await
            .expect("seed agent");
        store
            .seed_raw_token("131313131", "Scout7", "participant", None)
            .await
            .expect("seed participant");
        store.migrate_for_test().await.expect("migrate");
        // The agent token is gone at the DB level; only governor/participant rows remain.
        assert_eq!(store.count_tokens_by_type("agent").await.unwrap(), 0);
        let hub = build_hub_from_store(Arc::new(store), Duration::from_secs(30)).await;
        // A forged agent-style token does not validate as a participant.
        assert!(hub.validate_token("agent-3").is_err());
        let _ = std::fs::remove_file(&db);
    }

    /// S7-AC-3: legacy-DB upgrade — a named listen token survives migration, can /listen and
    /// re-/announce; the agent token is purged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s7_legacy_db_upgrade() {
        let db = unique_test_db();
        let store = TokenStore::open(&db).await.expect("open");
        // Legacy rows: a named, announced listen token (identity==token, name set) + an agent token.
        store
            .seed_raw_token("141414141", "141414141", "listen", Some("Scout7"))
            .await
            .expect("seed legacy listen");
        store
            .seed_raw_token("agent-9", "id-legacy", "agent", None)
            .await
            .expect("seed legacy agent");
        store.migrate_for_test().await.expect("migrate");

        let hub = build_hub_from_store(Arc::new(store), Duration::from_secs(30)).await;
        // The previously-valid participant token still works.
        let (tok, _rx) = hub
            .open_listen(Some("141414141"), None, None, None, false, false)
            .unwrap();
        assert_eq!(tok, "141414141");
        // Name binding restored → re-announce is idempotent (204-equivalent Bound).
        assert!(matches!(
            hub.announce("141414141", "Scout7"),
            Ok(AnnounceResult::Bound)
        ));
        // The agent token was purged.
        assert!(hub.validate_token("agent-9").is_err());
        let _ = std::fs::remove_file(&db);
    }

    /// S7-AC-4: the hub flow works against BOTH a fresh DB and a migrated legacy DB.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_s7_fresh_and_migrated_db_both_pass() {
        // Fresh DB.
        {
            let db = unique_test_db();
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let reg = hub.register_participant();
            let (tok, _rx) = hub
                .open_listen(Some(&reg), None, None, None, false, false)
                .unwrap();
            hub.announce(&tok, "FreshOne").unwrap();
            assert_eq!(
                hub.validate_token(&tok).unwrap().as_deref(),
                Some("FreshOne")
            );
            let _ = std::fs::remove_file(&db);
        }
        // Migrated legacy DB.
        {
            let db = unique_test_db();
            let store = TokenStore::open(&db).await.expect("open");
            store
                .seed_raw_token("151515151", "151515151", "listen", Some("LegacyOne"))
                .await
                .expect("seed");
            store.migrate_for_test().await.expect("migrate");
            let hub = build_hub_from_store(Arc::new(store), Duration::from_secs(30)).await;
            assert_eq!(
                hub.validate_token("151515151").unwrap().as_deref(),
                Some("LegacyOne")
            );
            let _ = std::fs::remove_file(&db);
        }
    }

    // ── Attachments (native file/attachment send) ────────────────────────────────

    /// Persistence-backed hub with alice↔bob registered + granted (attachments need a store).
    async fn setup_persisted_ab(
        db: &str,
    ) -> (
        DeliveryHub,
        ParticipantToken,
        ParticipantToken,
        GovernorToken,
    ) {
        let hub = make_persisted_hub(db, Duration::from_secs(30)).await;
        let gov = hub.install_governor(None);
        let tok_a = test_mint(&hub).unwrap();
        let tok_b = test_mint(&hub).unwrap();
        hub.approve_grant(&gov, &tok_a.0, &tok_b.0, None).unwrap();
        test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();
        test_bind(&hub, "bob", &tok_b, PresenceScope::GrantScoped).unwrap();
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
        match test_long_poll_dequeue(&hub, &tok_b, Duration::from_millis(100))
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
        let (hub, tok_a, _tok_b, _gov) = setup_persisted_ab(&db).await;
        let tok_c = test_mint(&hub).unwrap();
        test_bind(&hub, "carol", &tok_c, PresenceScope::GrantScoped).unwrap();

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
        let (hub, tok_a, _tok_b, _gov) = setup_persisted_ab(&db).await;
        let tok_c = test_mint(&hub).unwrap();
        test_bind(&hub, "carol", &tok_c, PresenceScope::GrantScoped).unwrap();
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

    /// AC1: after restart, previously persisted non-expired credentials work without
    /// re-provisioning. (15-0029 final form: participant = listen token; minted agents removed.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac1_tokens_survive_restart() {
        let db = unique_test_db();

        let (gov_tok, participant_tok) = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let reg = hub.register_participant();
            let (tok, _rx) = hub
                .open_listen(Some(&reg), None, None, None, false, false)
                .unwrap();
            hub.announce(&tok, "alice").unwrap();
            (gov, tok)
        };

        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        assert!(
            hub2.validate_token(&participant_tok).is_ok(),
            "AC1: participant token must survive restart"
        );
        assert!(
            hub2.validate_governor_token(&gov_tok).is_ok(),
            "AC1: governor token must survive restart"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// AC2: after restart, previously approved non-expired grants still authorize message sending.
    /// (15-0029 final form: grants are keyed on stable names from the participant listen flow.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac2_grants_survive_restart() {
        let db = unique_test_db();

        let (tok_a, _tok_b) = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let reg_a = hub.register_participant();
            let (a, _ra) = hub
                .open_listen(Some(&reg_a), None, None, None, false, false)
                .unwrap();
            hub.announce(&a, "alice").unwrap();
            let reg_b = hub.register_participant();
            let (b, _rb) = hub
                .open_listen(Some(&reg_b), None, None, None, false, false)
                .unwrap();
            hub.announce(&b, "bob").unwrap();
            hub.approve_grant(&gov, &a, &b, None).unwrap();
            (a, b)
        };

        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        assert!(
            matches!(
                hub2.send(
                    &ParticipantToken(tok_a.clone()),
                    "bob",
                    Payload(b"hi".to_vec()),
                    None,
                    None
                ),
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
            test_mint(&hub2).is_ok(),
            "AC3: governor can mint after restart"
        );
        assert!(
            hub2.approve_grant(&gov_tok, "a", "b", None).is_ok(),
            "AC3: governor can approve grant after restart"
        );

        let _ = std::fs::remove_file(&db);
    }

    // NOTE: the old `ac4_expired_tokens_not_loaded` test asserted that a minted agent token
    // with a fixed `expires_at` does not survive past its expiry after a restart. Listen
    // tokens have no `expiry: Option<Duration>` mint parameter at all — staleness is handled
    // entirely by GC aging (age/unlisten/no-grant TTLs), which already has dedicated
    // live-path coverage below (ac_t4_gc_unlisten_ttl_removes_never_listened_token,
    // ac_t5_gc_no_grant_ttl_removes_listened_never_granted_token, test_abandoned_token_gcd).
    // Removed rather than converted: there is no fixed-expiry mechanism left to test.

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
    /// (15-0029 final form: participant listen flow + name-keyed grant.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac6_permanent_grant_survives_restart() {
        let db = unique_test_db();

        let tok_a = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let reg_a = hub.register_participant();
            let (a, _ra) = hub
                .open_listen(Some(&reg_a), None, None, None, false, false)
                .unwrap();
            hub.announce(&a, "ag-a").unwrap();
            let reg_b = hub.register_participant();
            let (b, _rb) = hub
                .open_listen(Some(&reg_b), None, None, None, false, false)
                .unwrap();
            hub.announce(&b, "ag-b").unwrap();
            hub.approve_grant(&gov, &a, &b, None).unwrap(); // no expiry = permanent
            a
        };

        // Reload many times — grant must persist.
        for _ in 0..3 {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            assert!(
                matches!(
                    hub.send(
                        &ParticipantToken(tok_a.clone()),
                        "ag-b",
                        Payload(b"x".to_vec()),
                        None,
                        None
                    ),
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
            let a1 = test_mint(&hub).unwrap().0;
            let a2 = test_mint(&hub).unwrap().0;
            hub.approve_grant(&gov, "a1", "a2", None).unwrap();
            vec![gov.0, a1, a2]
        };

        // Phase 2: reload and mint new tokens — IDs must not collide
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;
        let gov2 = hub2.install_governor(None);
        let new_agent = test_mint(&hub2).unwrap();

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

        let reg_token = hub.register_participant();
        let (token, _rx) = hub
            .open_listen(Some(&reg_token), None, None, None, false, false)
            .unwrap();

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
            inner.listen_tokens.get_mut(&stale).unwrap().last_active =
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
            st.last_active = Instant::now() - Duration::from_secs(4000);

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

        let reg_token = hub.register_participant();
        let (stale, _rx) = hub
            .open_listen(Some(&reg_token), None, None, None, false, false)
            .unwrap();

        {
            let inner = hub.inner.lock().unwrap();
            let st = &inner.listen_tokens[&stale];
            assert!(st.ever_listened, "ever_listened must be true");
            assert!(!st.ever_granted, "ever_granted must be false");
        }

        // Backdate issued_at past the no-grant TTL (default 1800 s, min 120 s).
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.listen_tokens.get_mut(&stale).unwrap().last_active =
                Instant::now() - Duration::from_secs(2000);
        }

        let _fresh = hub.issue_token();

        assert!(
            matches!(hub.validate_token(&stale), Err(Error::TokenRejected)),
            "listened-but-never-granted token must be GC'd after no-grant TTL"
        );
    }

    // ── P0 fix: name-bound tokens are exempt from Branch-2 (no-grant) age-GC ──
    //
    // Regression guard for gc-named-token-exemption (P0 hotfix).
    // A token that has listened AND announced a name must NOT be evicted by
    // gc_tokens even when issued_at exceeds no_grant_ttl.  The guard
    // `!ever_granted && st.name.is_none()` ensures name-bound tokens
    // (established identity participants, governor listen tokens) fall through
    // to the `false` branch and are never age-evicted.

    #[test]
    fn ac_t5b_gc_named_token_exempt_from_no_grant_gc() {
        let hub = make_hub(Duration::from_secs(30));

        let reg_token = hub.register_participant();
        let (tok, _rx) = hub
            .open_listen(Some(&reg_token), None, None, None, false, false)
            .unwrap();
        // Announce a name — sets st.name = Some("GcExempt").
        hub.announce(&tok, "GcExempt").unwrap();

        {
            let inner = hub.inner.lock().unwrap();
            let st = &inner.listen_tokens[&tok];
            assert!(st.ever_listened, "ever_listened must be true");
            assert!(
                !st.ever_granted,
                "no grant issued — ever_granted must be false"
            );
            assert!(st.name.is_some(), "name must be bound after announce");
        }

        // Backdate issued_at well past the default no-grant TTL (1800 s).
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.listen_tokens.get_mut(&tok).unwrap().last_active =
                Instant::now() - Duration::from_secs(2000);
        }

        hub.trigger_gc_for_test();

        assert!(
            hub.validate_token(&tok).is_ok(),
            "name-bound token must NOT be GC'd by the no-grant branch even past no_grant_ttl"
        );
    }

    // ── AC-T6: identity-bound tokens are exempt from age-GC. (15-0029 addenda) ────
    //
    // Supersedes the prior Branch-3 age-GC-of-named-tokens behaviour: a token whose name is a
    // registered identity is now PERMANENT (the identity outlives the credential). Such a token
    // is never age-evicted, even when !ever_listened && ever_granted and idle far past its TTL.
    // No spurious sim_offline is emitted to grant-peers from age-GC.

    #[tokio::test(start_paused = true)]
    async fn ac_t6_gc_branch3_identity_bound_token_exempt() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        hub.set_settle_window_for_test(Duration::from_secs(1));

        // Agent A: observer with an opted-in push stream.
        let reg_a = hub.register_participant();
        let (tok_a, mut rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, true)
            .unwrap();
        hub.announce(&tok_a, "GcA6").unwrap();

        // Agent B: issue token + announce a name (→ identity-bound), but NO open_listen.
        let tok_b = hub.issue_token();
        hub.announce(&tok_b, "GcB6").unwrap();
        hub.approve_grant(&gov, &tok_b, &tok_a, None).unwrap();

        // Set ever_granted=true and backdate far past unlisten_ttl.
        {
            let mut inner = hub.inner.lock().unwrap();
            let st = inner.listen_tokens.get_mut(&tok_b).unwrap();
            st.ever_granted = true;
            st.last_active = Instant::now() - Duration::from_secs(4000);
        }

        while rx_a.try_recv().is_ok() {}

        // Trigger inline GC.
        let _trigger = hub.issue_token();

        // The identity-bound token must SURVIVE (it is exempt from age-GC).
        assert!(
            hub.validate_token(&tok_b).is_ok(),
            "identity-bound token must be exempt from age-GC"
        );

        // No sim_offline must fire for the exempt token.
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;
        let drained: Vec<String> = std::iter::from_fn(|| rx_a.try_recv().ok()).collect();
        assert!(
            !drained
                .iter()
                .any(|e| e.contains("\"offline\"") && e.contains("\"GcB6\"")),
            "no sim_offline may fire for an exempt identity-bound token; got: {drained:?}"
        );
    }

    // ── 15-0029 addenda: last_active GC + identity/governor exemptions ─────────

    /// GC-AC-4: an identity-bound participant token is exempt from age-GC.
    #[test]
    fn test_identity_bound_token_not_gcd() {
        let hub = make_hub(Duration::from_secs(30));
        let reg = hub.register_participant();
        let (tok, _rx) = hub
            .open_listen(Some(&reg), None, None, None, false, false)
            .unwrap();
        hub.announce(&tok, "Bound").unwrap();
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.listen_tokens.get_mut(&tok).unwrap().last_active =
                Instant::now() - Duration::from_secs(100_000);
        }
        let _ = hub.trigger_gc_for_test();
        assert!(
            hub.validate_token(&tok).is_ok(),
            "identity-bound token must survive age-GC"
        );
    }

    /// GC-AC-1: an active (listening + identity-bound) token survives well beyond an hour.
    #[test]
    fn test_active_token_survives_one_hour() {
        let hub = make_hub(Duration::from_secs(30));
        hub.set_gc_unlisten_ttl_for_test(Duration::from_secs(1));
        let reg = hub.register_participant();
        let (tok, _rx) = hub
            .open_listen(Some(&reg), None, None, None, false, false)
            .unwrap();
        hub.announce(&tok, "Active").unwrap();
        // Simulate > 1h since creation; the token remains identity-bound and recently active.
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.listen_tokens.get_mut(&tok).unwrap().last_active =
                Instant::now() - Duration::from_secs(3700);
        }
        let _ = hub.trigger_gc_for_test();
        assert!(
            hub.validate_token(&tok).is_ok(),
            "active identity-bound token must survive > 1h"
        );
    }

    /// GC-AC-2: a governor listen token is never age-GC'd.
    #[test]
    fn test_governor_listen_token_survives_indefinitely() {
        let hub = make_hub(Duration::from_secs(30));
        let gov = hub.install_governor(None);
        let (tok, _rx) = hub
            .open_listen(Some(&gov.0), None, None, None, false, false)
            .unwrap();
        {
            let mut inner = hub.inner.lock().unwrap();
            let st = inner.listen_tokens.get_mut(&tok).unwrap();
            assert!(
                st.governor_id.is_some(),
                "governor listen token must record governor_id"
            );
            st.last_active = Instant::now() - Duration::from_secs(100_000);
        }
        let _ = hub.trigger_gc_for_test();
        assert!(
            hub.validate_token(&tok).is_ok(),
            "governor listen token must survive age-GC"
        );
    }

    /// GC-AC-3: a registered-but-never-used token IS reaped once idle past its TTL.
    #[test]
    fn test_abandoned_token_gcd() {
        let hub = make_hub(Duration::from_secs(30));
        let tok = hub.register_participant(); // pending_first_listen=true, no name
        {
            let mut inner = hub.inner.lock().unwrap();
            inner.listen_tokens.get_mut(&tok).unwrap().last_active =
                Instant::now() - Duration::from_secs(100_000);
        }
        let evicted = hub.trigger_gc_for_test();
        assert!(evicted >= 1, "abandoned token must be reaped");
        assert!(
            hub.validate_token(&tok).is_err(),
            "abandoned token must be gone"
        );
    }

    // ── AC-T3: token survives server restart after first grant ─────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_t3_token_persists_after_first_grant_survives_restart() {
        let db = unique_test_db();

        let listen_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let gov = hub.install_governor(None);
            let tok_a = test_mint(&hub).unwrap();

            // Issue token for bob and announce its name.
            let reg_bob = hub.register_participant();
            let (listen_tok, _rx) = hub
                .open_listen(Some(&reg_bob), None, None, None, false, false)
                .unwrap();
            hub.announce(&listen_tok, "bob").unwrap();

            // Register alice so request_grant() can route by name.
            test_bind(&hub, "alice", &tok_a, PresenceScope::GrantScoped).unwrap();

            // Alice has no grant yet — request it explicitly.
            let _ = hub.send(&tok_a, "bob", Payload(b"hello".to_vec()), None, None);
            let request_id = hub
                .request_grant(&tok_a.0, "bob", None, None)
                .expect("request_grant must succeed");

            // Governor approves (PendingGovernor → PendingRecipient; queues grant_request to bob).
            hub.approve_grant_request(&gov.0, &request_id, None)
                .expect("governor approve must succeed");

            // Drain the grant_request event from bob's queue.
            let _ = hub.dequeue(&listen_tok, None).unwrap();

            // Bob approves (PendingRecipient → Established; grant created + token persisted).
            assert!(
                matches!(
                    hub.approve_grant_request(&listen_tok, &request_id, None),
                    Ok(ApproveStatus::Established)
                ),
                "both-approved path must return Established"
            );

            listen_tok
        }; // hub drops; DB write already completed (block_in_place in multi-thread runtime)

        // Simulate restart: rebuild hub from the same DB file.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;

        assert!(
            hub2.validate_token(&listen_tok).is_ok(),
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

        let listen_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let reg_bob = hub.register_participant();
            let (listen_tok, _rx) = hub
                .open_listen(Some(&reg_bob), None, None, None, false, false)
                .unwrap();
            hub.announce(&listen_tok, "bob").unwrap();
            listen_tok
        };

        // Restart: new hub from same DB.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;

        // Create a fresh sender on hub2 (original governor/agent tokens are not persisted).
        let tok_a2 = test_mint(&hub2).unwrap();
        test_bind(&hub2, "alice", &tok_a2, PresenceScope::GrantScoped).unwrap();

        // bob's token was announced before restart; routing entry must have been restored.
        let result = hub2.send(&tok_a2, "bob", Payload(b"ac1-probe".to_vec()), None, None);
        assert!(
            !matches!(result, Err(Error::RecipientUnknown)),
            "AC1: after announce-time persist + restart, send to 'bob' must NOT be RecipientUnknown; got {:?}",
            result
        );

        let _ = std::fs::remove_file(&db);
        drop(listen_tok);
    }

    /// AC2: announce token (no grant) → restart → validate_token returns Ok.
    /// Proves a never-granted token survives restart via the announce-time persist path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_persist_announce_token_valid_after_restart() {
        let db = unique_test_db();

        let listen_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let reg_bob = hub.register_participant();
            let (listen_tok, _rx) = hub
                .open_listen(Some(&reg_bob), None, None, None, false, false)
                .unwrap();
            hub.announce(&listen_tok, "bob").unwrap();
            listen_tok
        };

        // Restart: new hub from same DB.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;

        assert!(
            hub2.validate_token(&listen_tok).is_ok(),
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

        let listen_tok = {
            let hub = make_persisted_hub(&db, Duration::from_secs(30)).await;
            let reg_charlie = hub.register_participant();
            let (listen_tok, _rx) = hub
                .open_listen(Some(&reg_charlie), None, None, None, false, false)
                .unwrap();
            hub.announce(&listen_tok, name).unwrap();
            listen_tok
        };

        // Reload.
        let hub2 = make_persisted_hub(&db, Duration::from_secs(30)).await;

        let inner = hub2.inner.lock().unwrap();
        assert_eq!(
            inner.name_to_token.get(name).map(String::as_str),
            Some(listen_tok.as_str()),
            "AC3: name_to_token[name] must equal the announced token after reload"
        );
        let state = inner
            .listen_tokens
            .get(&listen_tok)
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
        let _gov = hub.install_governor(None);
        let participant_tok = test_mint(&hub).unwrap();

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

        // AC8 (updated for 15-0030): a valid participant (listen) token presented in the
        // governor slot → AuthFailed. The original AC8 expected Forbidden specifically for a
        // TrustChain.agents-backed minted agent token — `verify_governor`'s
        // `self.agents.contains_key(...)` branch distinguished "valid-but-wrong-type" (403)
        // from "unknown" (401) for that now-deleted token category. Listen tokens were never
        // in `TrustChain.agents`, so a listen token here already fell through to AuthFailed
        // pre-15-0030 too — this assertion now matches that reality instead of an
        // unreachable minted-agent path.
        let participant_as_gov = GovernorToken(participant_tok.0.clone());
        assert!(
            matches!(
                hub.list_all_grants_gov(&participant_as_gov, None),
                Err(Error::AuthFailed)
            ),
            "AC8: participant token in governor slot must yield AuthFailed"
        );
    }

    // ── Grant-gated presence tests ────────────────────────────────────────────

    /// AC1: Agent A with no grant to Agent B → presence_for_token returns false (not visible).
    #[test]
    fn test_presence_no_grant_returns_offline() {
        let hub = make_hub(Duration::from_secs(30));
        let _gov = hub.install_governor(None);

        // Register listen tokens for A and B (no grant between them).
        let reg_a = hub.register_participant();
        let reg_b = hub.register_participant();

        // Open listen for both.
        let (listen_a, _rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        let (listen_b, _rx_b) = hub
            .open_listen(Some(&reg_b), None, None, None, false, false)
            .unwrap();

        // Announce both.
        hub.announce(&listen_a, "alice").unwrap();
        hub.announce(&listen_b, "bob").unwrap();

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
        let reg_a = hub.register_participant();
        let reg_b = hub.register_participant();

        // Open listen for both.
        let (listen_a, _rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        let (listen_b, _rx_b) = hub
            .open_listen(Some(&reg_b), None, None, None, false, false)
            .unwrap();

        // Announce both (this updates name_to_token mappings).
        hub.announce(&listen_a, "alice").unwrap();
        hub.announce(&listen_b, "bob").unwrap();

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
        let reg_a = hub.register_participant();
        let reg_b = hub.register_participant();

        // Open listen for both.
        let (listen_a, _rx_a) = hub
            .open_listen(Some(&reg_a), None, None, None, false, false)
            .unwrap();
        let (listen_b, _rx_b) = hub
            .open_listen(Some(&reg_b), None, None, None, false, false)
            .unwrap();

        // Announce both.
        hub.announce(&listen_a, "alice").unwrap();
        hub.announce(&listen_b, "bob").unwrap();

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

    // NOTE: `test_presence_any_token_no_grant_returns_offline` /
    // `test_presence_any_token_with_grant_returns_online` tested `presence_any_token`'s
    // now-deleted minted-agent branch specifically. `presence_any_token` itself was dead
    // (no HTTP route; superseded by `presence_for_token`, which is exercised by
    // `test_presence_no_grant_returns_offline` / `test_presence_with_grant_returns_online`
    // above) — removed along with it rather than converted, since the listen-flow behavior
    // they'd otherwise duplicate is already covered.

    /// AC9: Second open_listen without force=true returns Error::ActiveSubscription.
    #[test]
    fn ac_listen_conflict_returns_active_subscription_error() {
        let hub = make_hub(Duration::from_secs(30));
        let tok = hub.register_participant();

        // Open first listen stream.
        let (token, _rx1) = hub
            .open_listen(Some(&tok), None, None, None, false, false)
            .expect("first open_listen must succeed");

        // Second open_listen without force → should return ActiveSubscription error.
        let result = hub.open_listen(Some(&token), None, None, None, false, false);
        assert!(
            matches!(result, Err(Error::ActiveSubscription)),
            "second open_listen without force must return ActiveSubscription error, got: {:?}",
            result
        );
    }

    /// AC9: Second open_listen with force=true supersedes prior stream and returns same token.
    #[test]
    fn ac_listen_force_takeover_supersedes_prior_stream() {
        let hub = make_hub(Duration::from_secs(30));
        let tok = hub.register_participant();

        // Open first listen stream.
        let (token1, mut rx1) = hub
            .open_listen(Some(&tok), None, None, None, false, false)
            .expect("first open_listen must succeed");

        // Second open_listen with force=true → should succeed and return same token.
        let (token2, _rx2) = hub
            .open_listen(Some(&token1), None, None, None, true, false)
            .expect("force takeover open_listen must succeed");

        assert_eq!(
            token1, token2,
            "force takeover must return same token, got token1={}, token2={}",
            token1, token2
        );

        // The "superseded" event is sent synchronously inside open_listen before it returns,
        // so try_recv() works here without any async runtime — event is already queued.
        // Note: rx1 also holds the welcome event from the first open_listen call; drain
        // until we find "superseded".
        let mut superseded_event = None;
        while let Ok(ev) = rx1.try_recv() {
            if ev.contains("superseded") {
                superseded_event = Some(ev);
                break;
            }
        }
        assert!(
            superseded_event.is_some(),
            "old SSE rx should receive superseded event, got none"
        );
    }

    // ── GC race: register_participant() → GC → open_listen() ─────────────────

    /// AC1+AC2 (sim-gc-race-register-open-listen): a freshly minted listen token must survive
    /// concurrent GC and succeed on the first `open_listen()` call even when the configured
    /// `unlisten_ttl` is shorter than the window between registration and first use,
    /// provided the token is still within the REGISTRATION_GRACE window.
    ///
    /// Procedure:
    ///   1. Create a hub with a very short Branch-1 GC TTL (1 ms) and a 200 ms grace window.
    ///   2. Register a participant token — `pending_first_listen` is set to true.
    ///   3. Sleep 5 ms (past TTL, still within 200 ms grace).
    ///   4. Trigger GC explicitly — the token must NOT be evicted (still in grace).
    ///   5. Call `open_listen()` with the token — must succeed (not AuthFailed/401).
    ///   6. AC3 regression: a second token registered and then used normally still works.
    #[test]
    fn gc_race_registered_token_survives_gc_before_open_listen() {
        let hub = make_hub(Duration::from_secs(30));

        // AC1: set a 1 ms Branch-1 TTL — far shorter than any real deployment, but
        // enough to make a freshly minted token appear "stale" to GC immediately.
        hub.set_gc_unlisten_ttl_for_test(Duration::from_millis(1));
        // Set a 200 ms grace window so the 5 ms sleep below stays inside the grace.
        hub.set_gc_registration_grace_for_test(Duration::from_millis(200));

        // Register the token (sets pending_first_listen = true).
        let token = hub.register_participant();

        // Sleep past the TTL (1 ms) but well within the grace window (200 ms).
        std::thread::sleep(Duration::from_millis(5));

        // AC2: trigger GC — the pending token must survive because it is within grace.
        let evicted = hub.trigger_gc_for_test();
        assert_eq!(
            evicted, 0,
            "GC must not evict a token with pending_first_listen=true that is still within the grace window"
        );

        // Token must still exist: open_listen() must succeed (not AuthFailed).
        let result = hub.open_listen(Some(&token), None, None, None, false, false);
        assert!(
            result.is_ok(),
            "open_listen() must succeed for a registered token that survived GC: {result:?}"
        );

        // AC3 regression: confirm normal lifecycle still works — a second token registered,
        // then immediately listened, must also succeed.
        let token2 = hub.register_participant();
        let result2 = hub.open_listen(Some(&token2), None, None, None, false, false);
        assert!(
            result2.is_ok(),
            "Normal register → open_listen flow must still succeed: {result2:?}"
        );
    }

    /// AC: a token with `pending_first_listen=true` whose registration grace window has
    /// expired must be evicted by GC (prevents unbounded accumulation from `/register` DoS).
    ///
    /// Procedure:
    ///   1. Hub with 1 ms unlisten_ttl and 50 ms grace.
    ///   2. Register a token — `pending_first_listen=true`.
    ///   3. Sleep 60 ms (past both TTL and grace).
    ///   4. Trigger GC — token MUST be evicted (grace expired, never listened).
    ///   5. `open_listen()` must fail with AuthFailed (token gone).
    #[test]
    fn gc_evicts_registered_token_after_grace_expires() {
        let hub = make_hub(Duration::from_secs(30));

        hub.set_gc_unlisten_ttl_for_test(Duration::from_millis(1));
        hub.set_gc_registration_grace_for_test(Duration::from_millis(50));

        let token = hub.register_participant();

        // Sleep past both the TTL (1 ms) and the grace window (50 ms).
        std::thread::sleep(Duration::from_millis(60));

        // GC must evict the stale, never-listened token.
        let evicted = hub.trigger_gc_for_test();
        assert_eq!(
            evicted, 1,
            "GC must evict a pending_first_listen token whose grace window has expired"
        );

        // open_listen() must now fail — the token no longer exists.
        let result = hub.open_listen(Some(&token), None, None, None, false, false);
        assert!(
            matches!(result, Err(Error::AuthFailed)),
            "open_listen() must return AuthFailed for an evicted token, got: {result:?}"
        );
    }
}
