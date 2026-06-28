use std::time::Duration;

/// Upper bound on any future expiry we materialize as an `Instant`/`SystemTime` (~100 years).
/// Clamp `expiry` to this BEFORE computing `now + d`: an absurd `expiry_secs` (e.g. `u64::MAX`)
/// from a request body would otherwise overflow `Instant + Duration` / `SystemTime + Duration`
/// and panic. With the clamp, a too-long expiry simply becomes "very long" — never a crash.
pub const MAX_EXPIRY: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

// §3.1: unique name bound to a token-identity for the lifetime of a registration
pub struct ParticipantName(pub String);

// §4.3: minted by a governor; authorizes register/send/presence/dequeue for one agent identity
pub struct ParticipantToken(pub String);

// §4.2: minted by the owner; carries delegated authority valid only while online and unexpired
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
