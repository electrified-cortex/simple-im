//! In-memory rooms subsystem — opt-in discovery space for agent co-presence.
//!
//! Rooms allow agents to find peers outside their existing grant relationships.
//! Membership is transient (TTL-based) and rooms do not persist across server restarts.
//!
//! # Rules (from spec §rooms)
//! - `create` returns a UUID; caller is NOT auto-joined.
//! - `join` adds the caller, resets TTL on re-join, returns member list.
//! - TTL defaults to 300 s; expiry removes the agent silently (lazy cleanup on access).
//! - `get` returns member list with online status; 403 if caller not a member.
//! - `leave` removes caller; idempotent if not a member or room not found.
//! - Two agents co-present in a room may submit grant requests to each other.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Default TTL for room membership when the caller does not supply one.
const DEFAULT_TTL_SECS: u64 = 300;

// ── Internal types ────────────────────────────────────────────────────────────

struct RoomMember {
    expires_at: Instant,
}

struct Room {
    /// Maps agent name → member record.
    members: HashMap<String, RoomMember>,
}

impl Room {
    fn new() -> Self {
        Room {
            members: HashMap::new(),
        }
    }

    /// Remove all expired members. Called lazily before any read or write.
    fn prune(&mut self) {
        let now = Instant::now();
        self.members.retain(|_, m| m.expires_at > now);
    }

    /// Live member names after pruning.
    fn active_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.members.keys().cloned().collect();
        names.sort_unstable(); // deterministic ordering for tests
        names
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Error variants for room operations.
#[derive(Debug)]
pub enum RoomError {
    /// The requested room_id does not exist.
    RoomNotFound,
    /// The caller is not a member of the room.
    NotMember,
}

/// Thread-safe in-memory store for all rooms.
pub struct RoomStore {
    inner: Mutex<HashMap<String, Room>>,
}

impl Default for RoomStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RoomStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Create a new room. Returns the server-generated UUID room ID.
    /// The caller is NOT automatically added.
    pub fn create(&self) -> String {
        let id = gen_room_id();
        self.inner.lock().unwrap().insert(id.clone(), Room::new());
        id
    }

    /// Join `name` to room `room_id`.
    ///
    /// - Idempotent: re-joining resets the member's TTL.
    /// - `ttl_secs`: membership lifetime in seconds (default 300).
    /// - Returns the current live member list on success.
    pub fn join(
        &self,
        room_id: &str,
        name: &str,
        ttl_secs: Option<u64>,
    ) -> Result<Vec<String>, RoomError> {
        let mut rooms = self.inner.lock().unwrap();
        let room = rooms.get_mut(room_id).ok_or(RoomError::RoomNotFound)?;
        room.prune();
        let ttl = Duration::from_secs(ttl_secs.unwrap_or(DEFAULT_TTL_SECS));
        room.members.insert(
            name.to_string(),
            RoomMember {
                expires_at: Instant::now() + ttl,
            },
        );
        Ok(room.active_names())
    }

    /// Return the live member list for `room_id`.
    ///
    /// Returns `RoomError::RoomNotFound` if the room does not exist.
    /// Returns `RoomError::NotMember` if `caller_name` is not (or is no longer) a member.
    pub fn members(&self, room_id: &str, caller_name: &str) -> Result<Vec<String>, RoomError> {
        let mut rooms = self.inner.lock().unwrap();
        let room = rooms.get_mut(room_id).ok_or(RoomError::RoomNotFound)?;
        room.prune();
        if !room.members.contains_key(caller_name) {
            return Err(RoomError::NotMember);
        }
        Ok(room.active_names())
    }

    /// Remove `name` from room `room_id`.
    ///
    /// Idempotent: returns `Ok(())` even if the agent was not a member or the room
    /// does not exist.
    pub fn leave(&self, room_id: &str, name: &str) {
        let mut rooms = self.inner.lock().unwrap();
        if let Some(room) = rooms.get_mut(room_id) {
            room.prune();
            room.members.remove(name);
        }
    }

    /// Return `true` if `name_a` and `name_b` are currently co-present in any room.
    ///
    /// Performs lazy TTL pruning on every room it visits.
    pub fn shares_room(&self, name_a: &str, name_b: &str) -> bool {
        let mut rooms = self.inner.lock().unwrap();
        let now = Instant::now();
        rooms.values_mut().any(|room| {
            room.members.retain(|_, m| m.expires_at > now);
            room.members.contains_key(name_a) && room.members.contains_key(name_b)
        })
    }
}

// ── UUID generation ───────────────────────────────────────────────────────────

/// Generate a UUID v4-style room ID using the `rand` crate.
/// Format: `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`
fn gen_room_id() -> String {
    use rand::Rng as _;
    let mut rng = rand::thread_rng();
    // Avoid `gen` which is a reserved keyword in edition 2024.
    let hi: u64 = rng.gen_range(0..=u64::MAX);
    let lo: u64 = rng.gen_range(0..=u64::MAX);

    let p1 = (hi >> 32) as u32;
    let p2 = ((hi >> 16) & 0xffff) as u16;
    let p3 = (((hi) & 0x0fff) | 0x4000) as u16; // version 4
    let p4 = (((lo >> 48) & 0x3fff) | 0x8000) as u16; // variant 1
    let p5 = lo & 0x0000_ffff_ffff_ffff;

    format!("{p1:08x}-{p2:04x}-{p3:04x}-{p4:04x}-{p5:012x}")
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_returns_uuid_format() {
        let store = RoomStore::new();
        let id = store.create();
        let parts: Vec<&str> = id.split('-').collect();
        assert_eq!(
            parts.len(),
            5,
            "UUID must have 5 hyphen-separated groups: {id}"
        );
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
        // Version nibble must be '4'
        assert!(parts[2].starts_with('4'), "version nibble must be 4: {id}");
    }

    #[test]
    fn join_unknown_room_returns_not_found() {
        let store = RoomStore::new();
        assert!(matches!(
            store.join("no-such-room", "alice", None),
            Err(RoomError::RoomNotFound)
        ));
    }

    #[test]
    fn join_and_members_happy_path() {
        let store = RoomStore::new();
        let id = store.create();
        store.join(&id, "alice", None).unwrap();
        let names = store.join(&id, "bob", None).unwrap();
        assert!(names.contains(&"alice".to_string()));
        assert!(names.contains(&"bob".to_string()));
    }

    #[test]
    fn members_requires_caller_membership() {
        let store = RoomStore::new();
        let id = store.create();
        store.join(&id, "alice", None).unwrap();
        assert!(matches!(
            store.members(&id, "bob"),
            Err(RoomError::NotMember)
        ));
        assert!(store.members(&id, "alice").is_ok());
    }

    #[test]
    fn leave_is_idempotent() {
        let store = RoomStore::new();
        let id = store.create();
        store.join(&id, "alice", None).unwrap();
        store.leave(&id, "alice");
        store.leave(&id, "alice"); // second leave is a no-op
        store.leave("phantom-room", "alice"); // missing room is a no-op
    }

    #[test]
    fn shares_room_true_when_both_present() {
        let store = RoomStore::new();
        let id = store.create();
        store.join(&id, "alice", None).unwrap();
        store.join(&id, "bob", None).unwrap();
        assert!(store.shares_room("alice", "bob"));
        assert!(store.shares_room("bob", "alice"));
    }

    #[test]
    fn shares_room_false_after_leave() {
        let store = RoomStore::new();
        let id = store.create();
        store.join(&id, "alice", None).unwrap();
        store.join(&id, "bob", None).unwrap();
        store.leave(&id, "bob");
        assert!(!store.shares_room("alice", "bob"));
    }

    #[test]
    fn ttl_expiry_removes_member() {
        let store = RoomStore::new();
        let id = store.create();
        // 0-second TTL = already expired
        store.join(&id, "alice", Some(0)).unwrap();
        // A tiny sleep to ensure Instant::now() > expires_at
        std::thread::sleep(std::time::Duration::from_millis(1));
        // members() should prune alice
        assert!(matches!(
            store.members(&id, "alice"),
            Err(RoomError::NotMember)
        ));
        assert!(!store.shares_room("alice", "bob"));
    }

    #[test]
    fn rejoin_resets_ttl() {
        let store = RoomStore::new();
        let id = store.create();
        // Join with 0-second TTL (immediately expired)
        store.join(&id, "alice", Some(0)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1));
        // Re-join with normal TTL — should reset
        let names = store.join(&id, "alice", Some(300)).unwrap();
        assert!(
            names.contains(&"alice".to_string()),
            "alice should be back after rejoin"
        );
        assert!(store.members(&id, "alice").is_ok());
    }
}
