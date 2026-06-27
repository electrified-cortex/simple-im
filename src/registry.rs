use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::error::Error;

/// Pre-validated agent identity derived from a token.
/// `None` represents an absent or invalid token; the registry rejects it with `AuthFailed`.
pub struct AgentIdentity(pub(crate) Option<String>);

impl AgentIdentity {
    pub fn valid(id: impl Into<String>) -> Self {
        Self(Some(id.into()))
    }

    pub fn invalid() -> Self {
        Self(None)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum PresenceScope {
    Public,
    #[default]
    GrantScoped,
    Hidden,
}

struct Registration {
    identity: String,
    last_seen: Instant,
    presence_scope: PresenceScope,
}

pub struct Registry<F = fn() -> Instant>
where
    F: Fn() -> Instant,
{
    entries: HashMap<String, Registration>,
    now: F,
    lapse_after: Duration,
}

impl Registry {
    pub fn new(lapse_after: Duration) -> Self {
        Self::with_clock(lapse_after, Instant::now)
    }
}

impl<F: Fn() -> Instant> Registry<F> {
    pub fn with_clock(lapse_after: Duration, now: F) -> Self {
        Self {
            entries: HashMap::new(),
            now,
            lapse_after,
        }
    }

    pub fn register(
        &mut self,
        name: &str,
        identity: AgentIdentity,
        scope: PresenceScope,
    ) -> Result<(), Error> {
        let id = identity.0.ok_or(Error::AuthFailed)?;
        let now = (self.now)();
        let lapse = self.lapse_after;

        if let Some(reg) = self.entries.get_mut(name)
            && now.duration_since(reg.last_seen) < lapse
        {
            return if reg.identity == id {
                reg.last_seen = now;
                reg.presence_scope = scope;
                Ok(())
            } else {
                Err(Error::NameInUse)
            };
        }

        self.entries.insert(
            name.to_string(),
            Registration {
                identity: id,
                last_seen: now,
                presence_scope: scope,
            },
        );
        Ok(())
    }

    pub fn presence_scope(&self, name: &str) -> Option<PresenceScope> {
        let now = (self.now)();
        let lapse = self.lapse_after;
        self.entries.get(name).and_then(|reg| {
            (now.duration_since(reg.last_seen) < lapse).then_some(reg.presence_scope)
        })
    }

    pub fn presence_scope_unconditional(&self, name: &str) -> Option<PresenceScope> {
        self.entries.get(name).map(|reg| reg.presence_scope)
    }

    pub fn set_presence_scope(
        &mut self,
        name: &str,
        identity: &AgentIdentity,
        scope: PresenceScope,
    ) -> Result<(), Error> {
        let id = identity.0.as_deref().ok_or(Error::AuthFailed)?;
        let now = (self.now)();
        let lapse = self.lapse_after;

        if let Some(reg) = self.entries.get_mut(name) {
            if now.duration_since(reg.last_seen) >= lapse {
                return Ok(());
            }
            if reg.identity != id {
                return Err(Error::Forbidden);
            }
            reg.presence_scope = scope;
        }
        Ok(())
    }

    pub fn force_deregister(&mut self, name: &str) {
        self.entries.remove(name);
    }

    pub fn deregister(&mut self, name: &str, identity: AgentIdentity) -> Result<(), Error> {
        let id = identity.0.ok_or(Error::AuthFailed)?;
        let now = (self.now)();
        let lapse = self.lapse_after;

        let verdict = self.entries.get(name).and_then(|reg| {
            (now.duration_since(reg.last_seen) < lapse).then(|| reg.identity == id)
        });

        match verdict {
            Some(true) => {
                self.entries.remove(name);
                Ok(())
            }
            Some(false) => Err(Error::Forbidden),
            None => Ok(()),
        }
    }

    pub fn is_online(&self, name: &str) -> bool {
        let now = (self.now)();
        let lapse = self.lapse_after;
        self.entries
            .get(name)
            .map(|reg| now.duration_since(reg.last_seen) < lapse)
            .unwrap_or(false)
    }

    pub fn reap(&mut self) {
        let now = (self.now)();
        let lapse = self.lapse_after;
        self.entries
            .retain(|_, reg| now.duration_since(reg.last_seen) < lapse);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    fn controlled_registry(ttl: Duration) -> (Registry<impl Fn() -> Instant>, Rc<Cell<Duration>>) {
        let offset = Rc::new(Cell::new(Duration::ZERO));
        let offset2 = Rc::clone(&offset);
        let base = Instant::now();
        let registry = Registry::with_clock(ttl, move || base + offset2.get());
        (registry, offset)
    }

    /// AC-REG-1: register with a valid identity → success; name reports online.
    #[test]
    fn ac_reg_1_register_valid_identity_succeeds() {
        let (mut reg, _) = controlled_registry(Duration::from_secs(30));
        let result = reg.register(
            "alice",
            AgentIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        );
        assert!(result.is_ok());
        assert!(reg.is_online("alice"));
    }

    /// AC-REG-2: register a live name with a different identity → NameInUse; original intact.
    #[test]
    fn ac_reg_2_register_different_identity_while_live_returns_name_in_use() {
        let (mut reg, _) = controlled_registry(Duration::from_secs(30));
        reg.register(
            "alice",
            AgentIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        let result = reg.register(
            "alice",
            AgentIdentity::valid("id-bob"),
            PresenceScope::GrantScoped,
        );
        assert!(matches!(result, Err(Error::NameInUse)));
        assert!(reg.is_online("alice"));
    }

    /// AC-REG-3: re-register a live name with the same identity → success (idempotent reconnect).
    #[test]
    fn ac_reg_3_re_register_same_identity_is_idempotent() {
        let (mut reg, _) = controlled_registry(Duration::from_secs(30));
        reg.register(
            "alice",
            AgentIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        let result = reg.register(
            "alice",
            AgentIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        );
        assert!(result.is_ok());
        assert!(reg.is_online("alice"));
    }

    /// AC-REG-4: register then deregister → name free; re-register by any identity succeeds.
    #[test]
    fn ac_reg_4_deregister_frees_name() {
        let (mut reg, _) = controlled_registry(Duration::from_secs(30));
        reg.register(
            "alice",
            AgentIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        reg.deregister("alice", AgentIdentity::valid("id-alice"))
            .unwrap();
        let result = reg.register(
            "alice",
            AgentIdentity::valid("id-bob"),
            PresenceScope::GrantScoped,
        );
        assert!(result.is_ok());
    }

    /// AC-REG-5: lapsed registration reaped on next reap pass; name offline and free.
    #[test]
    fn ac_reg_5_lapsed_registration_reaped() {
        let ttl = Duration::from_secs(30);
        let (mut reg, offset) = controlled_registry(ttl);
        reg.register(
            "alice",
            AgentIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        offset.set(ttl + Duration::from_secs(1));
        reg.reap();
        assert!(!reg.is_online("alice"));
        assert!(
            reg.register(
                "alice",
                AgentIdentity::valid("id-new"),
                PresenceScope::GrantScoped
            )
            .is_ok()
        );
    }

    /// AC-REG-6: register with an invalid identity → AuthFailed; no registration created.
    #[test]
    fn ac_reg_6_invalid_identity_returns_auth_failed() {
        let (mut reg, _) = controlled_registry(Duration::from_secs(30));
        let result = reg.register(
            "alice",
            AgentIdentity::invalid(),
            PresenceScope::GrantScoped,
        );
        assert!(matches!(result, Err(Error::AuthFailed)));
        assert!(!reg.is_online("alice"));
    }

    /// Criterion 7: deregister by non-owner → Forbidden; original registration intact.
    #[test]
    fn criterion_7_deregister_by_non_owner_returns_forbidden() {
        let (mut reg, _) = controlled_registry(Duration::from_secs(30));
        reg.register(
            "alice",
            AgentIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        let result = reg.deregister("alice", AgentIdentity::valid("id-intruder"));
        assert!(matches!(result, Err(Error::Forbidden)));
        assert!(reg.is_online("alice"));
    }
}
