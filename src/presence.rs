use std::time::{Duration, Instant};

use crate::error::Error;
use crate::registry::{ParticipantIdentity, PresenceScope, Registry};

pub const DEFAULT_LIVENESS_WINDOW: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
pub enum PresenceStatus {
    Online,
    Offline,
}

pub struct PresenceModule<F = fn() -> Instant>
where
    F: Fn() -> Instant,
{
    registry: Registry<F>,
}

impl PresenceModule {
    pub fn new(lapse_after: Duration) -> Self {
        Self {
            registry: Registry::new(lapse_after),
        }
    }
}

impl<F: Fn() -> Instant> PresenceModule<F> {
    pub fn with_clock(lapse_after: Duration, now: F) -> Self {
        Self {
            registry: Registry::with_clock(lapse_after, now),
        }
    }

    pub fn register(
        &mut self,
        name: &str,
        identity: ParticipantIdentity,
        scope: PresenceScope,
    ) -> Result<(), Error> {
        self.registry.register(name, identity, scope)
    }

    pub fn deregister(&mut self, name: &str, identity: ParticipantIdentity) -> Result<(), Error> {
        self.registry.deregister(name, identity)
    }

    /// Returns `Online` if the agent is within its liveness window.
    /// Returns `Offline` and triggers a reap pass on lapse or if the name was never registered
    /// (OQ-P2: never returns an error for an unknown name).
    pub fn query(&mut self, name: &str) -> PresenceStatus {
        if self.registry.is_online(name) {
            PresenceStatus::Online
        } else {
            self.registry.reap();
            PresenceStatus::Offline
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    fn controlled_module(
        ttl: Duration,
    ) -> (PresenceModule<impl Fn() -> Instant>, Rc<Cell<Duration>>) {
        let offset = Rc::new(Cell::new(Duration::ZERO));
        let offset2 = Rc::clone(&offset);
        let base = Instant::now();
        let module = PresenceModule::with_clock(ttl, move || base + offset2.get());
        (module, offset)
    }

    /// Criterion 1 / AC-MSG-4 (online half): agent within liveness window → presence query returns Online.
    #[test]
    fn criterion_1_agent_within_window_is_online() {
        let (mut pm, _) = controlled_module(Duration::from_secs(5));
        pm.register(
            "alice",
            ParticipantIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        assert_eq!(pm.query("alice"), PresenceStatus::Online);
    }

    /// Criterion 2 / AC-MSG-4 (offline half): agent past liveness window → presence query returns Offline.
    #[test]
    fn criterion_2_agent_past_window_is_offline() {
        let ttl = Duration::from_secs(5);
        let (mut pm, offset) = controlled_module(ttl);
        pm.register(
            "alice",
            ParticipantIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        offset.set(ttl + Duration::from_millis(1));
        assert_eq!(pm.query("alice"), PresenceStatus::Offline);
    }

    /// Criterion 3 / OQ-P1: liveness window configurable at construction; default 30s; injectable clock works.
    #[test]
    fn criterion_3_configurable_window_and_injectable_clock() {
        let _pm_default = PresenceModule::new(DEFAULT_LIVENESS_WINDOW);

        let small_ttl = Duration::from_millis(100);
        let (mut pm, offset) = controlled_module(small_ttl);
        pm.register(
            "bob",
            ParticipantIdentity::valid("id-bob"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        assert_eq!(pm.query("bob"), PresenceStatus::Online);
        offset.set(small_ttl + Duration::from_millis(1));
        assert_eq!(pm.query("bob"), PresenceStatus::Offline);
    }

    /// Criterion 4 / §3.1: liveness lapse drives registry reap; lapsed name is free for re-registration.
    #[test]
    fn criterion_4_lapse_drives_reap_name_freed() {
        let ttl = Duration::from_secs(5);
        let (mut pm, offset) = controlled_module(ttl);
        pm.register(
            "alice",
            ParticipantIdentity::valid("id-alice"),
            PresenceScope::GrantScoped,
        )
        .unwrap();
        offset.set(ttl + Duration::from_millis(1));
        assert_eq!(pm.query("alice"), PresenceStatus::Offline);
        assert!(
            pm.register(
                "alice",
                ParticipantIdentity::valid("id-new"),
                PresenceScope::GrantScoped
            )
            .is_ok()
        );
    }

    /// Criterion 5 / OQ-P2: presence query for never-registered name → Offline (not an error, not RecipientUnknown).
    #[test]
    fn criterion_5_never_registered_name_is_offline() {
        let (mut pm, _) = controlled_module(Duration::from_secs(30));
        assert_eq!(pm.query("nonexistent"), PresenceStatus::Offline);
    }
}
