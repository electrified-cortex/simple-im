use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

// ── Persisted data types ──────────────────────────────────────────────────────

pub struct PersistedToken {
    pub token: String,
    pub identity: String,
    pub token_type: String, // "governor", "agent", or "listen"
    pub expires_at_secs: Option<u64>,
    pub name: Option<String>, // announce name for listen tokens; None for governor/agent tokens
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

        // Idempotent: add name column introduced for listen-token announce persistence.
        let res = sqlx::query("ALTER TABLE tokens ADD COLUMN name TEXT")
            .execute(&self.pool)
            .await;
        if let Err(e) = res
            && !e.to_string().contains("duplicate column name")
        {
            return Err(e);
        }

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

        Ok(())
    }

    // ── Load ──────────────────────────────────────────────────────────────────

    pub async fn load_tokens(&self) -> Result<Vec<PersistedToken>, sqlx::Error> {
        let rows = sqlx::query("SELECT token, identity, token_type, expires_at, name FROM tokens")
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
                    name: row.get("name"),
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

    // ── Write ─────────────────────────────────────────────────────────────────

    pub async fn upsert_token(
        &self,
        token: &str,
        identity: &str,
        token_type: &str,
        expires_at: Option<SystemTime>,
        name: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let exp = expires_at.map(system_time_to_secs_str);
        let now = system_time_to_secs_str(SystemTime::now());
        // COALESCE(excluded.name, tokens.name): a NULL incoming name never overwrites a stored
        // name, so a grant-time persist (name=None) cannot clobber a prior announce-time persist.
        sqlx::query(
            "INSERT INTO tokens (token, identity, token_type, expires_at, created_at, name) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(token) DO UPDATE SET \
                 identity   = excluded.identity, \
                 token_type = excluded.token_type, \
                 expires_at = excluded.expires_at, \
                 created_at = excluded.created_at, \
                 name       = COALESCE(excluded.name, tokens.name)",
        )
        .bind(token)
        .bind(identity)
        .bind(token_type)
        .bind(exp)
        .bind(now)
        .bind(name)
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
