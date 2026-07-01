//! Trust layer — governor tokens, agent tokens, and pairwise grants.
//! [`TrustChain`] is the in-memory state machine; persistence is handled by the caller.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::Error;
use crate::persistence::{PersistedGrant, PersistedToken};
use crate::types::GovernorToken;

// ── Public enums ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum GrantDirection {
    Symmetric,
    AToB,
    BToA,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GrantMediation {
    Bypass,
    Inspect,
    Notify,
}

// ── Public output / request types ─────────────────────────────────────────────

/// Return type for the GET /grants listing endpoint.
pub struct GrantListItem {
    pub id: String,
    pub counterparty: String,
    pub direction: GrantDirection,
    pub expires: Option<Instant>,
}

/// Return type for the GET /governors/grants listing endpoint (full grant details).
pub struct AllGrantItem {
    pub id: String,
    pub identity_a: String,
    pub identity_b: String,
    pub name_a: Option<String>,
    pub name_b: Option<String>,
    pub direction: GrantDirection,
    pub expires: Option<Instant>,
}

/// A resolved grant reference returned by `check_grant_directed`; carries direction, mediation policy, governor id, and conditions.
pub struct GrantRef {
    pub id: String,
    pub opens_reply_window: bool,
    pub mediation: GrantMediation,
    pub governor_id: String,
    pub conditions: Option<String>,
}

/// Parameters for approving a grant request; carries direction, message limits, reply window flag, mediation policy, conditions, and resolved display names.
#[derive(Clone, Default)]
pub struct ApproveGrantRequest {
    pub direction: Option<GrantDirection>,
    pub max_messages: Option<u64>,
    pub opens_reply_window: Option<bool>,
    pub mediation: Option<GrantMediation>,
    pub conditions: Option<String>,
    /// Stable announced name for identity_a, used to survive identity rotation on reconnect.
    pub name_a: Option<String>,
    /// Stable announced name for identity_b, used to survive identity rotation on reconnect.
    pub name_b: Option<String>,
}

// ── Internal records ──────────────────────────────────────────────────────────

struct GovernorRecord {
    expires: Option<Instant>,
    online: bool,
    revoked: bool,
    /// Listen token linked to this governor's active participant session.
    listen_token: Option<String>,
}

struct PendingTransfer {
    from_gov_token: String,
    to_identity: Option<String>,
}

struct Grant {
    id: String,
    identity_a: String,
    identity_b: String,
    /// Stable announced name for identity_a, if known at grant-creation time.
    /// None for minted-agent grants where identity is already stable.
    name_a: Option<String>,
    /// Stable announced name for identity_b, if known at grant-creation time.
    name_b: Option<String>,
    expires: Option<Instant>,
    direction: GrantDirection,
    max_messages: Option<u64>,
    messages_used: u64,
    opens_reply_window: bool,
    governor_id: String,
    mediation: GrantMediation,
    conditions: Option<String>,
}

/// The in-memory trust state: governors, agent tokens, grants, and pending transfers.
pub struct TrustChain<F = fn() -> Instant>
where
    F: Fn() -> Instant,
{
    governors: HashMap<String, GovernorRecord>,
    grants: Vec<Grant>,
    pending_transfers: HashMap<String, PendingTransfer>,
    counter: u64,
    now: F,
}

impl Default for TrustChain {
    fn default() -> Self {
        Self::new()
    }
}

impl TrustChain {
    /// Creates a new empty `TrustChain` using the system clock.
    pub fn new() -> Self {
        Self::with_clock(Instant::now)
    }
}

impl<F: Fn() -> Instant> TrustChain<F> {
    /// Creates a `TrustChain` with a custom clock function; used in tests.
    pub fn with_clock(now: F) -> Self {
        Self {
            governors: HashMap::new(),
            grants: vec![],
            pending_transfers: HashMap::new(),
            counter: 0,
            now,
        }
    }

    fn verify_governor(&self, token: &GovernorToken) -> Result<(), Error> {
        if let Some(record) = self.governors.get(&token.0) {
            if record.revoked {
                return Err(Error::AuthFailed);
            }
            if !record.online {
                return Err(Error::AuthFailed);
            }
            if let Some(expires) = record.expires
                && (self.now)() >= expires
            {
                return Err(Error::TokenExpired);
            }
            Ok(())
        } else {
            Err(Error::AuthFailed)
        }
    }

    fn next_id(&mut self, prefix: &str) -> String {
        self.counter += 1;
        format!("{}-{}", prefix, self.counter)
    }

    /// True when at least one non-revoked governor record exists (bootstrap gate).
    pub fn has_active_governor(&self) -> bool {
        self.governors.values().any(|r| !r.revoked)
    }

    /// Install a governor unconditionally and return its token. No authorization — the caller
    /// (the hub's claim / election / transfer flow) owns the governance policy that gates this.
    /// Used for auto-grant (first governor on an empty hub), a completed election, or a
    /// completed transfer.
    pub fn install_governor(&mut self, expiry: Option<Duration>) -> GovernorToken {
        let now = (self.now)();
        let expires = expiry.map(|d| now + d.min(crate::types::MAX_EXPIRY));
        let id = self.next_id("gov");
        self.governors.insert(
            id.clone(),
            GovernorRecord {
                expires,
                online: true,
                revoked: false,
                listen_token: None,
            },
        );
        GovernorToken(id)
    }

    /// Token ids of all currently-active (non-revoked) governors.
    pub fn active_governor_tokens(&self) -> Vec<String> {
        self.governors
            .iter()
            .filter(|(_, r)| !r.revoked)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Revoke every active governor (used when a transfer completes: the new governor replaces
    /// the old). Returns the revoked token ids.
    pub fn revoke_all_governors(&mut self) -> Vec<String> {
        let mut revoked = vec![];
        for (id, rec) in self.governors.iter_mut() {
            if !rec.revoked {
                rec.revoked = true;
                revoked.push(id.clone());
            }
        }
        revoked
    }

    /// Marks a governor online or offline. The hub calls this when the governor's session state
    /// changes. An offline governor cannot mint tokens or approve grants (AC-SEC-2).
    pub fn set_governor_online(&mut self, governor: &GovernorToken, online: bool) {
        if let Some(record) = self.governors.get_mut(&governor.0) {
            record.online = online;
        }
    }

    /// Backward-compatible wrapper: Symmetric, no budget, opens_reply_window=true.
    pub fn approve_grant(
        &mut self,
        governor: &GovernorToken,
        identity_a: &str,
        identity_b: &str,
        expiry: Option<Duration>,
    ) -> Result<String, Error> {
        self.approve_grant_req(
            governor,
            identity_a,
            identity_b,
            expiry,
            ApproveGrantRequest::default(),
        )
    }

    /// Governor approves a pending grant request with full parameters; lower-level than `approve_grant`.
    pub fn approve_grant_req(
        &mut self,
        governor: &GovernorToken,
        identity_a: &str,
        identity_b: &str,
        expiry: Option<Duration>,
        req: ApproveGrantRequest,
    ) -> Result<String, Error> {
        self.verify_governor(governor)?;
        let governor_id = governor.0.clone();
        let now = (self.now)();
        let expires = expiry.map(|d| now + d.min(crate::types::MAX_EXPIRY));
        let id = self.next_id("grant");
        self.grants.push(Grant {
            id: id.clone(),
            identity_a: identity_a.to_string(),
            identity_b: identity_b.to_string(),
            name_a: req.name_a.clone(),
            name_b: req.name_b.clone(),
            expires,
            direction: req.direction.unwrap_or(GrantDirection::Symmetric),
            max_messages: req.max_messages,
            messages_used: 0,
            opens_reply_window: req.opens_reply_window.unwrap_or(true),
            governor_id,
            mediation: req.mediation.unwrap_or(GrantMediation::Bypass),
            conditions: req.conditions,
        });
        Ok(id)
    }

    /// Create a grant established by recipient consent alone (governorless mode). No governor
    /// verification — used only when the hub has no active governor, where the recipient's
    /// approval is the sole authority. The synthetic `governor_id` ("recipient-consent") marks
    /// the grant's provenance. With a governor present, this path is never taken.
    pub fn create_consent_grant(
        &mut self,
        identity_a: &str,
        identity_b: &str,
        expiry: Option<Duration>,
        req: ApproveGrantRequest,
    ) -> Result<String, Error> {
        let now = (self.now)();
        let expires = expiry.map(|d| now + d.min(crate::types::MAX_EXPIRY));
        let id = self.next_id("grant");
        self.grants.push(Grant {
            id: id.clone(),
            identity_a: identity_a.to_string(),
            identity_b: identity_b.to_string(),
            name_a: req.name_a.clone(),
            name_b: req.name_b.clone(),
            expires,
            direction: req.direction.unwrap_or(GrantDirection::Symmetric),
            max_messages: req.max_messages,
            messages_used: 0,
            opens_reply_window: req.opens_reply_window.unwrap_or(true),
            governor_id: "recipient-consent".to_string(),
            mediation: req.mediation.unwrap_or(GrantMediation::Bypass),
            conditions: req.conditions,
        });
        Ok(id)
    }

    /// Backward-compat wrapper around `check_grant_directed` (symmetric check only).
    /// Returns `Ok(())` if a valid, unexpired, non-exhausted grant covers the pair.
    pub fn check_grant(&self, identity_a: &str, identity_b: &str) -> Result<(), Error> {
        self.check_grant_directed(identity_a, identity_b)
            .map(|_| ())
    }

    /// Directed grant check. Returns the best matching `GrantRef` or an error.
    ///
    /// Error priority: valid non-exhausted > exhausted > expired > none.
    /// Among valid candidates: directed > symmetric; ties → lowest numeric grant ID.
    pub fn check_grant_directed(&self, from: &str, to: &str) -> Result<GrantRef, Error> {
        self.check_grant_directed_with_names(from, to, None, None)
    }

    /// Directed grant check with optional stable-name fallback (FP1 fix).
    ///
    /// When `from_name` and `to_name` are provided, a grant whose stored `name_a`/`name_b`
    /// matches those names is accepted even when its stored identity fields no longer match
    /// the current (post-reconnect) identity values.  This is the primary fix for the
    /// "grant identity rotation" bug: listen-flow agents re-mint their token on each
    /// /listen reconnect, so grants must be checked by stable name, not rotating token.
    ///
    /// Grants that were created without names (minted-agent grants, legacy persisted grants)
    /// fall back to the existing identity comparison.
    pub fn check_grant_directed_with_names(
        &self,
        from: &str,
        to: &str,
        from_name: Option<&str>,
        to_name: Option<&str>,
    ) -> Result<GrantRef, Error> {
        let now = (self.now)();
        let mut best_valid: Option<&Grant> = None;
        let mut found_exhausted = false;
        let mut found_expired = false;

        for grant in &self.grants {
            if !grant_covers_directed_with_names(from, to, from_name, to_name, grant) {
                continue;
            }
            if let Some(exp) = grant.expires
                && now >= exp
            {
                found_expired = true;
                continue;
            }
            if let Some(max) = grant.max_messages
                && grant.messages_used >= max
            {
                found_exhausted = true;
                continue;
            }
            best_valid = Some(match best_valid {
                None => grant,
                Some(current) => select_better_grant(current, grant),
            });
        }

        if let Some(grant) = best_valid {
            return Ok(GrantRef {
                id: grant.id.clone(),
                opens_reply_window: grant.opens_reply_window,
                mediation: grant.mediation.clone(),
                governor_id: grant.governor_id.clone(),
                conditions: grant.conditions.clone(),
            });
        }
        if found_exhausted {
            return Err(Error::GrantExhausted);
        }
        if found_expired {
            return Err(Error::GrantExpired);
        }
        Err(Error::NoGrant)
    }

    /// Increments `messages_used` for a budgeted grant. No-op for unlimited grants.
    /// Called under the hub lock before channel handoff.
    pub fn consume_grant_message(&mut self, grant_id: &str) {
        if let Some(grant) = self.grants.iter_mut().find(|g| g.id == grant_id)
            && grant.max_messages.is_some()
        {
            grant.messages_used += 1;
        }
    }

    /// Returns `Ok` if the token is a current, non-expired governor token.
    pub fn validate_governor_token(&self, token: &GovernorToken) -> Result<(), Error> {
        self.verify_governor(token)
    }

    /// Returns true if any governor token is currently marked online.
    pub fn has_online_governor(&self) -> bool {
        let now = (self.now)();
        self.governors
            .values()
            .any(|r| !r.revoked && r.online && r.expires.is_none_or(|exp| now < exp))
    }

    /// Returns true if the governor with the given raw token ID is online and not expired.
    pub fn is_governor_id_online(&self, id: &str) -> bool {
        let now = (self.now)();
        self.governors
            .get(id)
            .is_some_and(|r| !r.revoked && r.online && r.expires.is_none_or(|exp| now < exp))
    }

    /// Link a governor token to its active participant listen session.
    /// Called when the governor opens a listen session using their governor token as bearer.
    pub fn link_governor_session(&mut self, gov_token_id: &str, listen_token: &str) {
        if let Some(record) = self.governors.get_mut(gov_token_id) {
            record.listen_token = Some(listen_token.to_string());
        }
    }

    /// Returns all grant IDs where this handle appears as name_a or name_b.
    pub fn grant_ids_for_handle(&self, handle: &str) -> Vec<String> {
        self.grants
            .iter()
            .filter(|g| g.name_a.as_deref() == Some(handle) || g.name_b.as_deref() == Some(handle))
            .map(|g| g.id.clone())
            .collect()
    }

    /// Returns the `governor_id` stored on a grant, or None if the grant doesn't exist.
    pub fn grant_governor_id<'a>(&'a self, grant_id: &str) -> Option<&'a str> {
        self.grants
            .iter()
            .find(|g| g.id == grant_id)
            .map(|g| g.governor_id.as_str())
    }

    /// Returns all non-expired grants where `caller_name` appears as name_a or name_b.
    /// Used by `GET /grants` to list a participant's active grants.
    pub fn list_grants_for_name(&self, caller_name: &str) -> Vec<GrantListItem> {
        let now = (self.now)();
        self.grants
            .iter()
            .filter(|g| {
                // Skip expired grants.
                if let Some(exp) = g.expires
                    && now >= exp
                {
                    return false;
                }
                // Match by stable name on either side.
                g.name_a.as_deref() == Some(caller_name) || g.name_b.as_deref() == Some(caller_name)
            })
            .map(|g| {
                let counterparty = if g.name_a.as_deref() == Some(caller_name) {
                    g.name_b.clone().unwrap_or_default()
                } else {
                    g.name_a.clone().unwrap_or_default()
                };
                GrantListItem {
                    id: g.id.clone(),
                    counterparty,
                    direction: g.direction.clone(),
                    expires: g.expires,
                }
            })
            .collect()
    }

    /// Returns non-expired grants where this agent appears as a party, matched by stable name
    /// (`name_a`/`name_b`) OR by raw identity (`identity_a`/`identity_b`).
    ///
    /// Covers two paths (15-0002F):
    ///   - Name path: grants created via the connection-request flow or with names explicitly set.
    ///   - Identity path: minted-agent grants where the FP1 name lookup failed at creation time
    ///     (because `token_to_name` is keyed by token, not by identity, so the minted agent's
    ///     identity string doesn't resolve to a name via `approve_grant_req`).
    ///
    /// Returns `(counterparty_name, counterparty_identity)` for each unique grant (de-dup by ID).
    /// Direction-agnostic: presence events go to all grant-peers regardless of send direction.
    /// Used by `grant_peer_senders` in `HubInner` to build the presence-event dispatch set.
    pub fn grant_counterparties_for(
        &self,
        name: &str,
        identity: &str,
    ) -> Vec<(Option<String>, String)> {
        let now = (self.now)();
        let mut seen_ids: HashSet<String> = HashSet::new();
        let mut result = Vec::new();

        for grant in &self.grants {
            // Skip expired grants (mirrors list_grants_for_name).
            if let Some(exp) = grant.expires
                && now >= exp
            {
                continue;
            }

            // Match by stable name OR by raw identity to cover both listen-flow and minted-agent grants.
            let is_party_a = grant.name_a.as_deref() == Some(name) || grant.identity_a == identity;
            let is_party_b = grant.name_b.as_deref() == Some(name) || grant.identity_b == identity;

            if !is_party_a && !is_party_b {
                continue;
            }

            // De-duplicate: a grant may match both predicates (e.g. name AND identity both present).
            if !seen_ids.insert(grant.id.clone()) {
                continue;
            }

            // Return the OTHER party as the counterparty.
            // Prefer the A-side match (consistent with list_grants_for_name).
            if is_party_a {
                result.push((grant.name_b.clone(), grant.identity_b.clone()));
            } else {
                result.push((grant.name_a.clone(), grant.identity_a.clone()));
            }
        }

        result
    }

    /// Returns all non-expired grants in the system (governor view).
    /// If `participant_filter` is Some(name), only grants where name_a or name_b matches are returned.
    pub fn list_all_grants(&self, participant_filter: Option<&str>) -> Vec<AllGrantItem> {
        let now = (self.now)();
        self.grants
            .iter()
            .filter(|g| {
                // Skip expired grants.
                if let Some(exp) = g.expires
                    && now >= exp
                {
                    return false;
                }
                // Apply optional participant name filter.
                if let Some(name) = participant_filter {
                    return g.name_a.as_deref() == Some(name) || g.name_b.as_deref() == Some(name);
                }
                true
            })
            .map(|g| AllGrantItem {
                id: g.id.clone(),
                identity_a: g.identity_a.clone(),
                identity_b: g.identity_b.clone(),
                name_a: g.name_a.clone(),
                name_b: g.name_b.clone(),
                direction: g.direction.clone(),
                expires: g.expires,
            })
            .collect()
    }

    /// Returns `opens_reply_window` for a grant, or None if the grant doesn't exist.
    pub fn grant_opens_reply_window(&self, grant_id: &str) -> Option<bool> {
        self.grants
            .iter()
            .find(|g| g.id == grant_id)
            .map(|g| g.opens_reply_window)
    }

    /// Remove a grant by ID. Returns true if the grant was found and removed.
    pub fn remove_grant(&mut self, grant_id: &str) -> bool {
        let before = self.grants.len();
        self.grants.retain(|g| g.id != grant_id);
        self.grants.len() < before
    }

    /// Returns (identity_a, identity_b, name_a, name_b) for a grant, or None if not found.
    pub fn grant_parties(
        &self,
        grant_id: &str,
    ) -> Option<(String, String, Option<String>, Option<String>)> {
        self.grants.iter().find(|g| g.id == grant_id).map(|g| {
            (
                g.identity_a.clone(),
                g.identity_b.clone(),
                g.name_a.clone(),
                g.name_b.clone(),
            )
        })
    }

    /// Rotate a governor token atomically: move the record to a fresh ID, invalidate the old one.
    /// Returns the new token. All record fields (expiry, online, revoked) are unchanged.
    pub fn rotate_governor_token(
        &mut self,
        old_token: &GovernorToken,
    ) -> Result<GovernorToken, Error> {
        self.validate_governor_token(old_token)?;
        let record = self
            .governors
            .remove(&old_token.0)
            .ok_or(Error::AuthFailed)?;
        let new_id = self.next_id("gov");
        self.governors.insert(new_id.clone(), record);
        Ok(GovernorToken(new_id))
    }

    /// Returns the expiry Instant for a governor token, or None if permanent or not found.
    pub fn governor_expiry(&self, token: &GovernorToken) -> Option<Instant> {
        self.governors.get(&token.0).and_then(|r| r.expires)
    }

    /// Reverses a prior `consume_grant_message` call. Called under the hub lock when a channel
    /// handoff fails after a budget decrement, so the slot is not permanently lost.
    pub fn restore_grant_message(&mut self, grant_id: &str) {
        if let Some(grant) = self.grants.iter_mut().find(|g| g.id == grant_id)
            && grant.max_messages.is_some()
            && grant.messages_used > 0
        {
            grant.messages_used -= 1;
        }
    }

    /// Create a pending governor transfer. The current governor designates who may claim authority.
    /// Returns a one-time transfer token the current governor delivers out-of-band to the recipient.
    /// `to_identity` is optional — if set, only that identity may accept; if None, any presenter can.
    pub fn transfer_governor(
        &mut self,
        from: &GovernorToken,
        to_identity: Option<&str>,
    ) -> Result<String, Error> {
        self.verify_governor(from)?;
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 16];
        rng.fill_bytes(&mut bytes);
        let transfer_token: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
        self.pending_transfers.insert(
            transfer_token.clone(),
            PendingTransfer {
                from_gov_token: from.0.clone(),
                to_identity: to_identity.map(|s| s.to_string()),
            },
        );
        Ok(transfer_token)
    }

    /// Clear all pending governor transfers (used by the operator-anchored admin reset, so an
    /// in-flight transfer token cannot bypass the revoke). (15-0029 / completeness-M2)
    pub fn clear_pending_transfers(&mut self) {
        self.pending_transfers.clear();
    }

    /// Accept a pending governor transfer. The caller passes the transfer token and the identity
    /// it resolved from the **verified participant bearer** (never from the request body). On
    /// success: a new governor token is minted, the initiating governor is revoked, and the
    /// pending transfer is consumed. Returns the new governor token. (FG-5 / security-MAJOR-3)
    pub fn accept_governor_transfer(
        &mut self,
        transfer_token: &str,
        verified_bearer_identity: &str,
    ) -> Result<GovernorToken, Error> {
        let pending = self
            .pending_transfers
            .get(transfer_token)
            .ok_or(Error::AuthFailed)?;
        if let Some(ref expected) = pending.to_identity.clone()
            && expected != verified_bearer_identity
        {
            return Err(Error::Forbidden);
        }
        let from_gov = pending.from_gov_token.clone();
        self.pending_transfers.remove(transfer_token);
        // Revoke the initiating governor.
        if let Some(record) = self.governors.get_mut(&from_gov) {
            record.revoked = true;
        }
        // Mint a new governor token for the recipient.
        let new_id = self.next_id("gov");
        self.governors.insert(
            new_id.clone(),
            GovernorRecord {
                expires: None,
                online: true,
                revoked: false,
                listen_token: None,
            },
        );
        Ok(GovernorToken(new_id))
    }
}

// ── Persistence ───────────────────────────────────────────────────────────────

fn parse_counter_val(id: &str) -> u64 {
    id.rsplit('-')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

impl<F: Fn() -> Instant> TrustChain<F> {
    /// Load persisted tokens and grants into the in-memory trust chain on startup.
    /// Expired entries are skipped. Governors are loaded as offline (session state).
    /// The counter is seeded to the max seen ID so new IDs never collide.
    pub fn load_from_store(&mut self, tokens: Vec<PersistedToken>, grants: Vec<PersistedGrant>) {
        let now_instant = (self.now)();
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        for t in tokens {
            let expires = match t.expires_at_secs {
                Some(exp) if exp <= now_unix => continue, // expired
                Some(exp) => Some(
                    now_instant + Duration::from_secs(exp - now_unix).min(crate::types::MAX_EXPIRY),
                ),
                None => None,
            };

            let n = parse_counter_val(&t.token);
            if n > self.counter {
                self.counter = n;
            }

            // Only "governor" rows are loaded here — migration Step 5 purges legacy
            // "agent" rows before this runs (src/persistence.rs), and "participant"
            // (listen) rows are loaded separately by DeliveryHub.
            if t.token_type.as_str() == "governor" {
                // Governors have no session registration flow — they are always
                // considered online once minted (and not revoked/expired).
                self.governors.insert(
                    t.token,
                    GovernorRecord {
                        expires,
                        online: true,
                        revoked: false,
                        listen_token: None,
                    },
                );
            }
        }

        for g in grants {
            let expires = match g.expires_at_secs {
                Some(exp) if exp <= now_unix => continue, // expired
                Some(exp) => Some(
                    now_instant + Duration::from_secs(exp - now_unix).min(crate::types::MAX_EXPIRY),
                ),
                None => None,
            };

            // Counter must be seeded from governor token IDs, agent token IDs, AND grant IDs.
            let n = parse_counter_val(&g.id);
            if n > self.counter {
                self.counter = n;
            }

            let direction = match g.direction.as_str() {
                "a_to_b" => GrantDirection::AToB,
                "b_to_a" => GrantDirection::BToA,
                _ => GrantDirection::Symmetric,
            };
            let mediation = match g.mediation.as_str() {
                "inspect" => GrantMediation::Inspect,
                "notify" => GrantMediation::Notify,
                _ => GrantMediation::Bypass,
            };

            self.grants.push(Grant {
                id: g.id,
                identity_a: g.identity_a,
                identity_b: g.identity_b,
                name_a: g.name_a,
                name_b: g.name_b,
                expires,
                direction,
                max_messages: g.max_messages.map(|n| n as u64),
                messages_used: g.messages_used as u64,
                opens_reply_window: g.opens_reply_window,
                governor_id: g.governor_id,
                mediation,
                conditions: g.conditions,
            });
        }
    }
}

// ── Grant selection helpers ───────────────────────────────────────────────────

/// Covers-directed check with optional stable-name overrides (FP1 fix).
///
/// Matching logic (in priority order):
/// 1. If the grant has both name_a and name_b set, AND the caller provides both from_name and
///    to_name: match by name only (ignores the stored identity fields entirely).  This is the
///    post-reconnect path where identity has rotated but the stable name is constant.
/// 2. Fall back to the existing identity comparison for all other cases (minted-agent grants,
///    legacy grants loaded from DB without name columns, grants issued without names).
fn grant_covers_directed_with_names(
    from: &str,
    to: &str,
    from_name: Option<&str>,
    to_name: Option<&str>,
    grant: &Grant,
) -> bool {
    // Name-based path: only when the grant carries both names AND the caller supplies both names.
    if let (Some(gna), Some(gnb), Some(fn_), Some(tn_)) = (
        grant.name_a.as_deref(),
        grant.name_b.as_deref(),
        from_name,
        to_name,
    ) {
        return match grant.direction {
            GrantDirection::Symmetric => (gna == fn_ && gnb == tn_) || (gna == tn_ && gnb == fn_),
            GrantDirection::AToB => gna == fn_ && gnb == tn_,
            GrantDirection::BToA => gna == tn_ && gnb == fn_,
        };
    }

    // Identity-based fallback (original logic).
    match grant.direction {
        GrantDirection::Symmetric => {
            (grant.identity_a == from && grant.identity_b == to)
                || (grant.identity_a == to && grant.identity_b == from)
        }
        GrantDirection::AToB => grant.identity_a == from && grant.identity_b == to,
        GrantDirection::BToA => grant.identity_a == to && grant.identity_b == from,
    }
}

fn grant_id_num(id: &str) -> u64 {
    id.strip_prefix("grant-")
        .and_then(|s| s.parse().ok())
        .unwrap_or(u64::MAX)
}

/// Directed grant is preferred over Symmetric; ties broken by lowest numeric ID.
fn select_better_grant<'a>(a: &'a Grant, b: &'a Grant) -> &'a Grant {
    let a_directed = !matches!(a.direction, GrantDirection::Symmetric);
    let b_directed = !matches!(b.direction, GrantDirection::Symmetric);
    match (a_directed, b_directed) {
        (true, false) => a,
        (false, true) => b,
        _ => {
            if grant_id_num(&a.id) <= grant_id_num(&b.id) {
                a
            } else {
                b
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::types::GovernorToken;
    use std::cell::Cell;
    use std::rc::Rc;

    fn controlled_chain() -> (TrustChain<impl Fn() -> Instant>, Rc<Cell<Duration>>) {
        let offset = Rc::new(Cell::new(Duration::ZERO));
        let offset2 = Rc::clone(&offset);
        let base = Instant::now();
        let chain = TrustChain::with_clock(move || base + offset2.get());
        (chain, offset)
    }

    /// AC-TOK-1: install_governor creates a governor token; invalid gov token → AuthFailed.
    #[test]
    fn ac_tok_1_install_governor_and_forged_rejected() {
        let (mut chain, _) = controlled_chain();
        let gov = chain.install_governor(None);
        assert!(chain.validate_governor_token(&gov).is_ok());

        let forged = GovernorToken("not-a-real-gov".into());
        assert!(matches!(
            chain.validate_governor_token(&forged),
            Err(Error::AuthFailed)
        ));
    }

    /// AC-TOK-5: check_grant with no covering grant → NoGrant.
    #[test]
    fn ac_tok_5_no_grant_returns_no_grant() {
        let (chain, _) = controlled_chain();
        assert!(matches!(
            chain.check_grant("alice", "bob"),
            Err(Error::NoGrant)
        ));
    }

    /// AC-TOK-6: temporary grant expires → GrantExpired after expiry; permanent grant survives.
    #[test]
    fn ac_tok_6_temporary_grant_expires_permanent_does_not() {
        let (mut chain, offset) = controlled_chain();
        let gov = chain.install_governor(None);

        chain
            .approve_grant(&gov, "alice", "bob", Some(Duration::from_secs(60)))
            .unwrap();
        assert!(chain.check_grant("alice", "bob").is_ok());

        offset.set(Duration::from_secs(61));
        assert!(matches!(
            chain.check_grant("alice", "bob"),
            Err(Error::GrantExpired)
        ));

        chain.approve_grant(&gov, "carol", "dave", None).unwrap();
        offset.set(Duration::from_secs(10_000));
        assert!(chain.check_grant("carol", "dave").is_ok());
    }

    /// AC-TOK-7: revoking governor (via revoke_all_governors) prevents further actions;
    /// grants it issued survive to own expiry.
    #[test]
    fn ac_tok_7_revoke_governor_grants_survive() {
        let (mut chain, _) = controlled_chain();
        let gov = chain.install_governor(None);

        chain.approve_grant(&gov, "alice", "bob", None).unwrap();

        chain.revoke_all_governors();

        assert!(matches!(
            chain.approve_grant(&gov, "alice", "carol", None),
            Err(Error::AuthFailed)
        ));
        assert!(chain.check_grant("alice", "bob").is_ok());
    }

    /// AC-SEC-2: offline governor cannot mint tokens or approve grants even before token expiry.
    #[test]
    fn ac_sec_2_offline_governor_rejected() {
        let (mut chain, _) = controlled_chain();
        let gov = chain.install_governor(None);

        chain.set_governor_online(&gov, false);

        assert!(matches!(
            chain.approve_grant(&gov, "alice", "bob", None),
            Err(Error::AuthFailed)
        ));
    }

    /// Criterion 9 / OQ-G1: grant (A, B) authorizes both A→B and B→A (symmetric).
    #[test]
    fn criterion_9_grants_are_symmetric() {
        let (mut chain, _) = controlled_chain();
        let gov = chain.install_governor(None);

        chain.approve_grant(&gov, "alice", "bob", None).unwrap();

        assert!(chain.check_grant("alice", "bob").is_ok());
        assert!(chain.check_grant("bob", "alice").is_ok());
    }

    /// AToB grant covers alice→bob but blocks bob→alice.
    #[test]
    fn direction_a_to_b_blocks_reverse() {
        let (mut chain, _) = controlled_chain();
        let gov = chain.install_governor(None);

        chain
            .approve_grant_req(
                &gov,
                "alice",
                "bob",
                None,
                ApproveGrantRequest {
                    direction: Some(GrantDirection::AToB),
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(chain.check_grant_directed("alice", "bob").is_ok());
        assert!(matches!(
            chain.check_grant_directed("bob", "alice"),
            Err(Error::NoGrant)
        ));
    }

    /// FP1 regression: grant survives identity rotation (simulates /listen reconnect).
    ///
    /// Scenario: governor approves a grant keyed on (name="alice", name="bob") with initial
    /// identities ("token-alice-1", "token-bob-1").  Alice then reconnects and gets a new
    /// identity "token-alice-2" while her name stays "alice".
    /// check_grant_directed_with_names MUST still pass when called with the new identity
    /// but with the stable names, and MUST fail if names are not supplied (identity only).
    #[test]
    fn fp1_grant_survives_identity_rotation() {
        let (mut chain, _) = controlled_chain();
        let gov = chain.install_governor(None);

        // Approve a name-keyed grant: identity-based fields use the initial tokens.
        chain
            .approve_grant_req(
                &gov,
                "token-alice-1", // identity_a (initial listen token for alice)
                "token-bob-1",   // identity_b (initial listen token for bob)
                None,
                ApproveGrantRequest {
                    name_a: Some("alice".to_string()),
                    name_b: Some("bob".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();

        // Sanity: original identities work via identity path (name path takes precedence
        // when names are supplied).
        assert!(
            chain
                .check_grant_directed_with_names(
                    "token-alice-1",
                    "token-bob-1",
                    Some("alice"),
                    Some("bob"),
                )
                .is_ok(),
            "original identity+name pair should pass"
        );

        // Simulate reconnect: alice re-mints a new listen token → new identity.
        // Name stays "alice".  Bob's identity is unchanged.
        let new_alice_identity = "token-alice-2";

        // NAME-based check MUST PASS: the grant covers (alice, bob) by name.
        assert!(
            chain
                .check_grant_directed_with_names(
                    new_alice_identity,
                    "token-bob-1",
                    Some("alice"),
                    Some("bob"),
                )
                .is_ok(),
            "name-keyed grant must survive alice identity rotation"
        );

        // Symmetric direction: bob→alice with new identity must also pass.
        assert!(
            chain
                .check_grant_directed_with_names(
                    "token-bob-1",
                    new_alice_identity,
                    Some("bob"),
                    Some("alice"),
                )
                .is_ok(),
            "symmetric direction must survive identity rotation"
        );

        // Pure identity check (no names): MUST FAIL because stored identities are stale.
        assert!(
            matches!(
                chain.check_grant_directed(new_alice_identity, "token-bob-1"),
                Err(Error::NoGrant)
            ),
            "identity-only check with rotated identity must return NoGrant"
        );

        // Wrong name: MUST FAIL.
        assert!(
            matches!(
                chain.check_grant_directed_with_names(
                    new_alice_identity,
                    "token-bob-1",
                    Some("wrong-name"),
                    Some("bob"),
                ),
                Err(Error::NoGrant)
            ),
            "wrong name must return NoGrant"
        );
    }

    /// FP1 regression: name-keyed grant respects directed grants after rotation.
    #[test]
    fn fp1_directed_grant_survives_identity_rotation() {
        let (mut chain, _) = controlled_chain();
        let gov = chain.install_governor(None);

        // AToB grant: only alice→bob, not bob→alice.
        chain
            .approve_grant_req(
                &gov,
                "token-alice-1",
                "token-bob-1",
                None,
                ApproveGrantRequest {
                    direction: Some(GrantDirection::AToB),
                    name_a: Some("alice".to_string()),
                    name_b: Some("bob".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();

        let new_alice = "token-alice-99";

        // alice→bob with new identity: PASS.
        assert!(
            chain
                .check_grant_directed_with_names(
                    new_alice,
                    "token-bob-1",
                    Some("alice"),
                    Some("bob"),
                )
                .is_ok(),
            "AToB grant must pass alice→bob after rotation"
        );

        // bob→alice: FAIL (direction blocks it).
        assert!(
            matches!(
                chain.check_grant_directed_with_names(
                    "token-bob-1",
                    new_alice,
                    Some("bob"),
                    Some("alice"),
                ),
                Err(Error::NoGrant)
            ),
            "AToB grant must block bob→alice even after rotation"
        );
    }
}
