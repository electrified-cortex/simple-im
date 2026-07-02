use std::time::Duration;

/// Upper bound on any future expiry we materialize as an `Instant`/`SystemTime` (~100 years).
/// Clamp `expiry` to this BEFORE computing `now + d`: an absurd `expiry_secs` (e.g. `u64::MAX`)
/// from a request body would otherwise overflow `Instant + Duration` / `SystemTime + Duration`
/// and panic. With the clamp, a too-long expiry simply becomes "very long" — never a crash.
pub const MAX_EXPIRY: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

// §3.1: unique name bound to a token-identity for the lifetime of a registration
pub struct ParticipantName(pub String);

// §4.3: minted by a governor (or bootstrap); authorizes register/send/presence/dequeue for one
// participant identity. Also the SOLE credential a governor holds (15-0040 FR1/FR2): governorship
// is a privilege flag carried by an identity, never a second minted credential.
pub struct ParticipantToken(pub String);

// 15-0040 (FR2, OQ4 — implementer's call): retained as an internal marker type, NOT a second
// wire credential. It wraps the SAME bearer string as `ParticipantToken` — the caller presents
// their one participant token; this newtype only marks "this call site is asserting the governor
// privilege flag must be checked for this bearer" (see `DeliveryHub`/`HubInner::validate_governor_token`,
// which resolves the wrapped token to an identity and checks it against the singleton governor
// pointer). No code path mints a distinct `GovernorToken` value handed to a participant.
pub struct GovernorToken(pub String);

// §5.3: opaque bytes handed to the recipient's live channel; hub never stores or inspects contents
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Payload(pub Vec<u8>);

#[derive(Debug, Clone)]
pub struct QueuedMessage {
    pub payload: Payload,
    pub from_name: String,
    pub reason: Option<String>,
    pub event_type: Option<String>,
    pub thread_id: Option<String>,
}
