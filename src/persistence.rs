use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

// ── Persisted data types ──────────────────────────────────────────────────────

pub struct PersistedToken {
    pub token: String,
    pub identity: String,
    pub token_type: String, // "governor" or "participant" (post-15-0029; "agent"/"listen" purged)
    pub expires_at_secs: Option<u64>,
}

/// A permanent identity record (15-0029 / FG-7). The name survives token GC, revoke, and expiry.
pub struct PersistedIdentity {
    pub name: String,
    pub created_at: String,
}

pub struct PersistedDenialBlock {
    pub from_identity: String,
    pub to_name: String,
    pub reason: String,
    pub expires_at: Option<u64>,
}

/// A file attachment as held server-side. `bytes` is the full blob (stored in the DB).
pub struct StoredAttachment {
    pub from_identity: String,
    pub to_name: String,
    pub filename: String,
    pub mime: String,
    pub bytes: Vec<u8>,
    pub expires_at_secs: u64,
}

pub struct PersistedGrant {
    pub id: String,
    pub identity_a: String,
    pub identity_b: String,
    pub direction: String, // "symmetric", "a_to_b", "b_to_a"
    pub mediation: String, // "bypass", "inspect", "notify"
    pub max_messages: Option<i64>,
    pub messages_used: i64,
    pub conditions: Option<String>,
    pub opens_reply_window: bool,
    pub expires_at_secs: Option<u64>,
    pub governor_id: String,
    /// Stable announced name for identity_a (FP1 fix). None for legacy/minted-agent grants.
    pub name_a: Option<String>,
    /// Stable announced name for identity_b (FP1 fix). None for legacy/minted-agent grants.
    pub name_b: Option<String>,
}

// ── Timestamp helpers ─────────────────────────────────────────────────────────

pub fn system_time_to_secs_str(t: SystemTime) -> String {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        .to_string()
}

pub fn expiry_to_secs_str(base: SystemTime, expiry: Duration) -> String {
    system_time_to_secs_str(base + expiry.min(crate::types::MAX_EXPIRY))
}

fn secs_str_to_u64(s: &str) -> Option<u64> {
    s.parse::<u64>().ok()
}

// ── TokenStore ────────────────────────────────────────────────────────────────

pub struct TokenStore {
    pool: SqlitePool,
}

impl TokenStore {
    /// Open (or create) the SQLite DB at `path`. Runs schema migration on first open.
    pub async fn open(path: &str) -> Result<Self, sqlx::Error> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tokens (
                token       TEXT PRIMARY KEY,
                identity    TEXT NOT NULL,
                token_type  TEXT NOT NULL,
                expires_at  TEXT,
                created_at  TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // Idempotent: migrate pre-listen token_type values to 'listen'.
        sqlx::query("UPDATE tokens SET token_type='listen' WHERE token_type='v2'")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS grants (
                id                  TEXT PRIMARY KEY,
                identity_a          TEXT NOT NULL,
                identity_b          TEXT NOT NULL,
                direction           TEXT NOT NULL,
                mediation           TEXT NOT NULL,
                max_messages        INTEGER,
                messages_used       INTEGER NOT NULL DEFAULT 0,
                conditions          TEXT,
                opens_reply_window  INTEGER NOT NULL DEFAULT 0,
                expires_at          TEXT,
                governor_id         TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // FP1 fix: idempotent migration — add name_a / name_b columns for stable-name keying.
        for col in &["name_a", "name_b"] {
            let res = sqlx::query(&format!("ALTER TABLE grants ADD COLUMN {} TEXT", col))
                .execute(&self.pool)
                .await;
            if let Err(e) = res
                && !e.to_string().contains("duplicate column name")
            {
                return Err(e);
            }
        }

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS denial_blocks (
                from_identity   TEXT NOT NULL,
                to_name         TEXT NOT NULL,
                reason          TEXT NOT NULL,
                expires_at      TEXT,
                created_at      TEXT NOT NULL,
                PRIMARY KEY (from_identity, to_name)
            )",
        )
        .execute(&self.pool)
        .await?;

        // Native file attachments: bytes held server-side as a BLOB, fetched on-demand by
        // id (FR2/FR5 — DB-backed, never written to loose files on disk). Bound to
        // (from_identity, to_name) for access control (NFR2); expires_at drives GC (FR6).
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS attachments (
                id            TEXT PRIMARY KEY,
                from_identity TEXT NOT NULL,
                to_name       TEXT NOT NULL,
                filename      TEXT NOT NULL,
                mime          TEXT NOT NULL,
                size_bytes    INTEGER NOT NULL,
                bytes         BLOB NOT NULL,
                created_at    TEXT NOT NULL,
                expires_at    TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // 15-0029 S1 / FG-7: permanent identities table — a name's record must survive
        // token GC (a token is a replaceable credential; the identity/name is permanent).
        // Step 1 of the zero-debt migration plan. Additive and idempotent.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS identities (
                name        TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // 15-0040 FR2/OQ1: the governor is a privilege flag on a participant identity, not a
        // separate credential — persisted as a single-row singleton pointer (identity name),
        // never a per-identity table (that would smuggle back the two-credential shape this task
        // removes). At most one row (id=0) ever exists.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS governor (
                id        INTEGER PRIMARY KEY CHECK (id = 0),
                identity  TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // ── 15-0029 zero-debt migration Steps 2–7 ──────────────────────────────
        // All idempotent, all mandatory. Run AFTER the identities table exists and AFTER
        // the v2→listen rename above. Provably-safe: a row is preserved by the Step-6 purge
        // iff its identity is a REGISTERED NAME (present in `identities`, populated by 2a/2b).

        // Steps 2a/2b/3 read the retired `name` column to backfill/fix legacy pre-15-0029 rows;
        // Step 7 (15-0031 hard-drop) removes that column once it has been consumed. Gate the
        // whole group on the column's actual presence instead of unconditionally re-adding it
        // (former Step 1) on every single migrate() call: a DB that has already been through the
        // hard-drop (the overwhelmingly common case — every startup after the first) has nothing
        // left to backfill and nothing left to drop, so it now takes none of these DDL paths.
        // This also sidesteps a reproducible SQLite quirk where ADD COLUMN, run against the same
        // connection that previously DROPped a column of the same name, intermittently reports
        // "duplicate column name" while a subsequent statement on that same connection reports
        // "no such column" for that identical column — i.e. add-then-drop-then-add-again of the
        // same name on one live connection is not safe to rely on; never doing the re-add in
        // production avoids the pattern entirely.
        let name_col_present =
            sqlx::query("SELECT 1 FROM pragma_table_info('tokens') WHERE name = 'name' LIMIT 1")
                .persistent(false)
                .fetch_optional(&self.pool)
                .await?
                .is_some();

        let (id_2a, id_2b, name_col_dropped) = if name_col_present {
            // Step 2a: backfill identities from rows where `identity` already holds the name.
            let id_2a = sqlx::query(
                "INSERT OR IGNORE INTO identities (name, created_at) \
                 SELECT identity, created_at FROM tokens \
                 WHERE token_type IN ('listen', 'participant') \
                   AND identity IS NOT NULL AND identity != '' AND identity != token",
            )
            .execute(&self.pool)
            .await?
            .rows_affected();

            // Step 2b: backfill identities for FG-3 defect rows (identity == token) using `name`.
            let id_2b = sqlx::query(
                "INSERT OR IGNORE INTO identities (name, created_at) \
                 SELECT name, created_at FROM tokens \
                 WHERE token_type IN ('listen', 'participant') \
                   AND (identity = token OR identity IS NULL OR identity = '') \
                   AND name IS NOT NULL AND name != ''",
            )
            .execute(&self.pool)
            .await?
            .rows_affected();

            // Step 3: fix identity for FG-3 defect rows (identity == token → identity = name).
            sqlx::query(
                "UPDATE tokens SET identity = name \
                 WHERE token_type IN ('listen', 'participant') \
                   AND name IS NOT NULL AND name != '' \
                   AND (identity = token OR identity IS NULL OR identity = '')",
            )
            .execute(&self.pool)
            .await?;

            // Step 7 (15-0031 hard-drop): `name` has now been fully consumed by 2a/2b/3 above
            // (its only remaining reader) — the BLOCKER-5 startup fallback that used to read it
            // (in DeliveryHub::new_with_persisted_state) has been removed, so no code path reads
            // `name` anymore once this runs. Column presence was just confirmed above on this
            // same connection, so this DROP cannot hit "no such column".
            sqlx::query("ALTER TABLE tokens DROP COLUMN name")
                .persistent(false)
                .execute(&self.pool)
                .await?;

            (id_2a, id_2b, 1)
        } else {
            (0, 0, 0)
        };

        // Step 4: rename listen → participant (token-type collapse 3→2).
        let renamed =
            sqlx::query("UPDATE tokens SET token_type = 'participant' WHERE token_type = 'listen'")
                .execute(&self.pool)
                .await?
                .rows_affected();

        // Step 5: purge legacy agent-N tokens (mandatory breaking change).
        let agent_purged = sqlx::query("DELETE FROM tokens WHERE token_type = 'agent'")
            .execute(&self.pool)
            .await?
            .rows_affected();

        // Step 6: purge participant tokens with no valid identity binding (mandatory).
        // Keep iff identity is a registered name — does NOT rely on the "names are never numeric"
        // invariant. Steps 2a/2b ran first, so every announced name is already in `identities`.
        let orphan_purged = sqlx::query(
            "DELETE FROM tokens \
             WHERE token_type = 'participant' \
               AND (identity IS NULL OR identity = '' OR identity NOT IN (SELECT name FROM identities))",
        )
        .execute(&self.pool)
        .await?
        .rows_affected();

        eprintln!(
            "sim_migrate: identities_created={} listen_renamed={} agent_purged={} \
             orphan_purged={} name_col_dropped={}",
            id_2a + id_2b,
            renamed,
            agent_purged,
            orphan_purged,
            name_col_dropped,
        );

        // ── 15-0040 — operator-authorized full reset (one-time, ever) ──────────────────
        // Collapses the old two-credential (participant + gov-N) model into the single-token +
        // governor-flag model (FR1/FR2). The operator explicitly authorized wiping ALL pre-reset
        // naming/trust state fleet-wide rather than a careful preserve-and-convert migration (see
        // 15-0040 "Backward-compatibility / migration — OPERATOR-AUTHORIZED FULL RESET"): the
        // hub's naming/governor layer was already fully locked out, so a reset cannot make it
        // worse. Every participant re-registers and every grant is re-requested after this runs.
        //
        // Gated by a one-row marker table so this destructive step runs exactly once, ever, no
        // matter how many times the server restarts afterward (AC-13 idempotency).
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS reset_15_0040 (
                id       INTEGER PRIMARY KEY CHECK (id = 0),
                done_at  TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        let already_reset = sqlx::query("SELECT 1 FROM reset_15_0040 WHERE id = 0")
            .fetch_optional(&self.pool)
            .await?
            .is_some();

        if !already_reset {
            let now = system_time_to_secs_str(SystemTime::now());
            let mut tx = self.pool.begin().await?;
            let tokens_cleared = sqlx::query("DELETE FROM tokens")
                .execute(&mut *tx)
                .await?
                .rows_affected();
            let identities_cleared = sqlx::query("DELETE FROM identities")
                .execute(&mut *tx)
                .await?
                .rows_affected();
            let grants_cleared = sqlx::query("DELETE FROM grants")
                .execute(&mut *tx)
                .await?
                .rows_affected();
            let denial_blocks_cleared = sqlx::query("DELETE FROM denial_blocks")
                .execute(&mut *tx)
                .await?
                .rows_affected();
            let governor_cleared = sqlx::query("DELETE FROM governor")
                .execute(&mut *tx)
                .await?
                .rows_affected();
            sqlx::query("INSERT INTO reset_15_0040 (id, done_at) VALUES (0, ?)")
                .bind(&now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            eprintln!(
                "sim_migrate: 15-0040 full reset — tokens_cleared={} identities_cleared={} \
                 grants_cleared={} denial_blocks_cleared={} governor_cleared={}",
                tokens_cleared,
                identities_cleared,
                grants_cleared,
                denial_blocks_cleared,
                governor_cleared,
            );
        }

        Ok(())
    }

    // ── Load ──────────────────────────────────────────────────────────────────

    pub async fn load_tokens(&self) -> Result<Vec<PersistedToken>, sqlx::Error> {
        let rows = sqlx::query("SELECT token, identity, token_type, expires_at FROM tokens")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                use sqlx::Row;
                let exp_str: Option<String> = row.get("expires_at");
                PersistedToken {
                    token: row.get("token"),
                    identity: row.get("identity"),
                    token_type: row.get("token_type"),
                    expires_at_secs: exp_str.as_deref().and_then(secs_str_to_u64),
                }
            })
            .collect())
    }

    pub async fn load_grants(&self) -> Result<Vec<PersistedGrant>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, identity_a, identity_b, direction, mediation, \
             max_messages, messages_used, conditions, opens_reply_window, \
             expires_at, governor_id, name_a, name_b FROM grants",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                use sqlx::Row;
                let exp_str: Option<String> = row.get("expires_at");
                let orw: i64 = row.get("opens_reply_window");
                PersistedGrant {
                    id: row.get("id"),
                    identity_a: row.get("identity_a"),
                    identity_b: row.get("identity_b"),
                    direction: row.get("direction"),
                    mediation: row.get("mediation"),
                    max_messages: row.get("max_messages"),
                    messages_used: row.get("messages_used"),
                    conditions: row.get("conditions"),
                    opens_reply_window: orw != 0,
                    expires_at_secs: exp_str.as_deref().and_then(secs_str_to_u64),
                    governor_id: row.get("governor_id"),
                    name_a: row.get("name_a"),
                    name_b: row.get("name_b"),
                }
            })
            .collect())
    }

    /// Load the singleton governor identity (15-0040 FR2/OQ1), if one is currently set.
    pub async fn load_governor(&self) -> Result<Option<String>, sqlx::Error> {
        use sqlx::Row;
        let row = sqlx::query("SELECT identity FROM governor WHERE id = 0")
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("identity")))
    }

    /// Set (or move) the singleton governor pointer to `identity`. No credential is minted —
    /// this only records which existing participant identity currently holds the flag.
    pub async fn set_governor(&self, identity: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO governor (id, identity) VALUES (0, ?) \
             ON CONFLICT(id) DO UPDATE SET identity = excluded.identity",
        )
        .bind(identity)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Clear the singleton governor pointer (nobody is governor afterward).
    pub async fn clear_governor(&self) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM governor")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Load all permanent identity records (15-0029 / FG-7).
    pub async fn load_identities(&self) -> Result<Vec<PersistedIdentity>, sqlx::Error> {
        use sqlx::Row;
        let rows = sqlx::query("SELECT name, created_at FROM identities")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| PersistedIdentity {
                name: r.get("name"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    // ── Write ─────────────────────────────────────────────────────────────────

    /// Insert a permanent identity record on first announce. `INSERT OR IGNORE` preserves the
    /// original `created_at` across governor rebinds (EPIC-AC-8): a name's first-announce
    /// timestamp is immutable.
    pub async fn upsert_identity(&self, name: &str) -> Result<(), sqlx::Error> {
        let now = system_time_to_secs_str(SystemTime::now());
        sqlx::query("INSERT OR IGNORE INTO identities (name, created_at) VALUES (?, ?)")
            .bind(name)
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_token(
        &self,
        token: &str,
        identity: &str,
        token_type: &str,
        expires_at: Option<SystemTime>,
    ) -> Result<(), sqlx::Error> {
        let exp = expires_at.map(system_time_to_secs_str);
        let now = system_time_to_secs_str(SystemTime::now());
        sqlx::query(
            "INSERT INTO tokens (token, identity, token_type, expires_at, created_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT(token) DO UPDATE SET \
                 identity   = excluded.identity, \
                 token_type = excluded.token_type, \
                 expires_at = excluded.expires_at, \
                 created_at = excluded.created_at",
        )
        .bind(token)
        .bind(identity)
        .bind(token_type)
        .bind(exp)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_token(&self, token: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM tokens WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_grant(
        &self,
        id: &str,
        identity_a: &str,
        identity_b: &str,
        direction: &str,
        mediation: &str,
        max_messages: Option<u64>,
        messages_used: u64,
        conditions: Option<&str>,
        opens_reply_window: bool,
        expires_at: Option<SystemTime>,
        governor_id: &str,
        name_a: Option<&str>,
        name_b: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let exp = expires_at.map(system_time_to_secs_str);
        let orw = opens_reply_window as i32;
        let max_msg = max_messages.map(|n| n as i64);
        sqlx::query(
            "INSERT OR REPLACE INTO grants \
             (id, identity_a, identity_b, direction, mediation, max_messages, \
              messages_used, conditions, opens_reply_window, expires_at, governor_id, \
              name_a, name_b) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(identity_a)
        .bind(identity_b)
        .bind(direction)
        .bind(mediation)
        .bind(max_msg)
        .bind(messages_used as i64)
        .bind(conditions)
        .bind(orw)
        .bind(exp)
        .bind(governor_id)
        .bind(name_a)
        .bind(name_b)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn increment_grant_usage(&self, grant_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE grants SET messages_used = messages_used + 1 WHERE id = ?")
            .bind(grant_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn decrement_grant_usage(&self, grant_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE grants SET messages_used = MAX(messages_used - 1, 0) WHERE id = ?")
            .bind(grant_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_grant(&self, grant_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM grants WHERE id = ?")
            .bind(grant_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ── Denial block persistence ──────────────────────────────────────────────

    pub async fn upsert_denial_block(
        &self,
        from_identity: &str,
        to_name: &str,
        reason: &str,
        expires_at: Option<u64>,
    ) -> Result<(), sqlx::Error> {
        let now = system_time_to_secs_str(SystemTime::now());
        let exp = expires_at.map(|s| s.to_string());
        sqlx::query(
            "INSERT INTO denial_blocks (from_identity, to_name, reason, expires_at, created_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(from_identity, to_name) DO UPDATE SET
                 reason = excluded.reason,
                 expires_at = excluded.expires_at,
                 created_at = excluded.created_at",
        )
        .bind(from_identity)
        .bind(to_name)
        .bind(reason)
        .bind(exp)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_denial_block(
        &self,
        from_identity: &str,
        to_name: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM denial_blocks WHERE from_identity = ? AND to_name = ?")
            .bind(from_identity)
            .bind(to_name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn load_denial_blocks(&self) -> Result<Vec<PersistedDenialBlock>, sqlx::Error> {
        let rows =
            sqlx::query("SELECT from_identity, to_name, reason, expires_at FROM denial_blocks")
                .fetch_all(&self.pool)
                .await?;
        use sqlx::Row;
        Ok(rows
            .into_iter()
            .map(|r| {
                let exp_str: Option<String> = r.get("expires_at");
                PersistedDenialBlock {
                    from_identity: r.get("from_identity"),
                    to_name: r.get("to_name"),
                    reason: r.get("reason"),
                    expires_at: exp_str.as_deref().and_then(|s| s.parse::<u64>().ok()),
                }
            })
            .collect())
    }

    // ── Attachments ─────────────────────────────────────────────────────────────

    /// Store a file attachment blob. `expires_at_secs` is an absolute unix-seconds TTL.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_attachment(
        &self,
        id: &str,
        from_identity: &str,
        to_name: &str,
        filename: &str,
        mime: &str,
        bytes: &[u8],
        expires_at_secs: u64,
    ) -> Result<(), sqlx::Error> {
        let now = system_time_to_secs_str(SystemTime::now());
        sqlx::query(
            "INSERT INTO attachments \
             (id, from_identity, to_name, filename, mime, size_bytes, bytes, created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(from_identity)
        .bind(to_name)
        .bind(filename)
        .bind(mime)
        .bind(bytes.len() as i64)
        .bind(bytes)
        .bind(now)
        .bind(expires_at_secs.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Fetch a stored attachment by id (full blob). `None` if unknown.
    pub async fn get_attachment(&self, id: &str) -> Result<Option<StoredAttachment>, sqlx::Error> {
        use sqlx::Row;
        let row = sqlx::query(
            "SELECT from_identity, to_name, filename, mime, bytes, expires_at FROM attachments WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| {
            let exp: String = r.get("expires_at");
            StoredAttachment {
                from_identity: r.get("from_identity"),
                to_name: r.get("to_name"),
                filename: r.get("filename"),
                mime: r.get("mime"),
                bytes: r.get("bytes"),
                expires_at_secs: exp.parse::<u64>().unwrap_or(0),
            }
        }))
    }

    pub async fn delete_attachment(&self, id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM attachments WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// GC: delete attachments whose expiry is at or before `now_secs`. Returns rows removed.
    pub async fn gc_expired_attachments(&self, now_secs: u64) -> Result<u64, sqlx::Error> {
        let res = sqlx::query("DELETE FROM attachments WHERE CAST(expires_at AS INTEGER) <= ?")
            .bind(now_secs as i64)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }
}

// ── Test-only migration seams (15-0029) ─────────────────────────────────────────
#[cfg(test)]
impl TokenStore {
    /// Re-run the schema migration (idempotency + legacy-row processing in tests).
    pub async fn migrate_for_test(&self) -> Result<(), sqlx::Error> {
        self.migrate().await
    }

    /// Insert a raw token row (including the legacy `name` column) to simulate a pre-migration DB.
    /// `TokenStore::open` already ran `migrate()` once by the time a test gets a `store` handle,
    /// which (15-0031) drops the retired `name` column whenever it finds one present — so this
    /// helper re-adds it itself, idempotently, before writing a legacy row into it. Both
    /// statements run on a single connection (via an explicit transaction) so the INSERT can
    /// never observe a different connection's stale, pre-ADD view of the schema — a real desync
    /// observed under concurrent test load when each statement was independently pulled from the
    /// pool.
    pub async fn seed_raw_token(
        &self,
        token: &str,
        identity: &str,
        token_type: &str,
        name: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        // Only ADD if not already present (checked on this same connection/transaction, matching
        // migrate()'s presence-gated Step 2a/2b/3/7 group) — see the long comment on that group
        // in migrate() for why this helper never blindly re-issues ADD COLUMN after a DROP on the
        // same connection.
        let name_col_present =
            sqlx::query("SELECT 1 FROM pragma_table_info('tokens') WHERE name = 'name' LIMIT 1")
                .persistent(false)
                .fetch_optional(&mut *tx)
                .await?
                .is_some();
        if !name_col_present {
            sqlx::query("ALTER TABLE tokens ADD COLUMN name TEXT")
                .persistent(false)
                .execute(&mut *tx)
                .await?;
        }
        let now = system_time_to_secs_str(SystemTime::now());
        sqlx::query(
            "INSERT OR REPLACE INTO tokens (token, identity, token_type, expires_at, created_at, name) \
             VALUES (?, ?, ?, NULL, ?, ?)",
        )
        .persistent(false)
        .bind(token)
        .bind(identity)
        .bind(token_type)
        .bind(now)
        .bind(name)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Whether the retired `name` column is currently present on the tokens table (15-0031 test
    /// seam for the `name_col_dropped` acceptance criterion, S1-AC-8). Uses the same
    /// `pragma_table_info` presence check as migrate() rather than a `SELECT name ...` probe, so
    /// it can't be confused by the same connection's ADD/DROP history for that column name.
    pub async fn tokens_name_column_exists(&self) -> Result<bool, sqlx::Error> {
        Ok(
            sqlx::query("SELECT 1 FROM pragma_table_info('tokens') WHERE name = 'name' LIMIT 1")
                .persistent(false)
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    /// Count token rows of a given type.
    pub async fn count_tokens_by_type(&self, token_type: &str) -> Result<i64, sqlx::Error> {
        use sqlx::Row;
        let row = sqlx::query("SELECT COUNT(*) AS c FROM tokens WHERE token_type = ?")
            .bind(token_type)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get("c"))
    }

    /// Return the `identity` column for a token, or None if the row is gone.
    pub async fn token_identity(&self, token: &str) -> Result<Option<String>, sqlx::Error> {
        use sqlx::Row;
        let row = sqlx::query("SELECT identity FROM tokens WHERE token = ?")
            .bind(token)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("identity")))
    }

    /// Return the `token_type` column for a token, or None if the row is gone.
    pub async fn token_type_of(&self, token: &str) -> Result<Option<String>, sqlx::Error> {
        use sqlx::Row;
        let row = sqlx::query("SELECT token_type FROM tokens WHERE token = ?")
            .bind(token)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("token_type")))
    }

    /// Return the `created_at` of an identity record, or None if absent.
    pub async fn identity_created_at(&self, name: &str) -> Result<Option<String>, sqlx::Error> {
        use sqlx::Row;
        let row = sqlx::query("SELECT created_at FROM identities WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("created_at")))
    }
}
