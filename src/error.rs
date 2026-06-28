#[derive(Debug)]
pub enum Error {
    AuthFailed,
    TokenExpired,
    Forbidden,
    NameInUse,
    NoGrant,
    GrantExpired,
    GrantExhausted,
    RecipientOffline,
    RecipientUnknown,
    BadRequest,
    BriefRequired,
    MediationUnavailable,
    Blocked,
    TokenRejected,
    TokenRevoked,
    RequestPending,
    /// Sender has not called /announce — name required before sending.
    AnnounceRequired,
    /// Grant request blocked by a persistent denial; carries the denial reason.
    GrantBlocked(String),
    /// DCP: handle already exists at introduce time; instructive breadcrumb in message
    HandleExists,
    /// DCP: handle unknown at announce time
    IdentityNotFound,
    /// DCP: connect_probe nonce expired or not found
    ProbeExpired,
    /// DCP: connect_probe nonce mismatch / auth-token mismatch
    ProbeInvalid,
    /// DCP: agent tried to send before reaching CONNECTED
    NotConnected,
    /// File attachment unknown or expired.
    AttachmentNotFound,
    /// Server-side failure (e.g. attachment store unavailable or I/O error).
    Internal,
    /// Token already has an active SSE subscription.
    ActiveSubscription,
}

impl Error {
    pub fn code(&self) -> &'static str {
        match self {
            Error::AuthFailed => "AUTH_FAILED",
            Error::TokenExpired => "TOKEN_EXPIRED",
            Error::Forbidden => "FORBIDDEN",
            Error::NameInUse => "NAME_IN_USE",
            Error::NoGrant => "NO_GRANT",
            Error::GrantExpired => "GRANT_EXPIRED",
            Error::GrantExhausted => "GRANT_EXHAUSTED",
            Error::RecipientOffline => "RECIPIENT_OFFLINE",
            Error::RecipientUnknown => "RECIPIENT_UNKNOWN",
            Error::BadRequest => "BAD_REQUEST",
            Error::BriefRequired => "BRIEF_REQUIRED",
            Error::MediationUnavailable => "MEDIATION_UNAVAILABLE",
            Error::Blocked => "BLOCKED",
            Error::TokenRejected => "TOKEN_REJECTED",
            Error::TokenRevoked => "TOKEN_REVOKED",
            Error::RequestPending => "REQUEST_PENDING",
            Error::AnnounceRequired => "ANNOUNCE_REQUIRED",
            Error::GrantBlocked(_) => "GRANT_BLOCKED",
            Error::HandleExists => "HANDLE_EXISTS",
            Error::IdentityNotFound => "IDENTITY_NOT_FOUND",
            Error::ProbeExpired => "PROBE_EXPIRED",
            Error::ProbeInvalid => "PROBE_INVALID",
            Error::NotConnected => "NOT_CONNECTED",
            Error::AttachmentNotFound => "ATTACHMENT_NOT_FOUND",
            Error::Internal => "INTERNAL",
            Error::ActiveSubscription => "ACTIVE_SUBSCRIPTION",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Error::AuthFailed => "authentication failed",
            Error::TokenExpired => "token has expired",
            Error::Forbidden => "access forbidden",
            Error::NameInUse => "name is currently in use",
            Error::NoGrant => "no grant exists for this sender-recipient pair",
            Error::GrantExpired => "grant has expired",
            Error::GrantExhausted => "grant message limit reached",
            Error::RecipientOffline => "recipient is offline",
            Error::RecipientUnknown => "recipient not found",
            Error::BadRequest => "bad request",
            Error::BriefRequired => "authorization required for this message",
            Error::MediationUnavailable => "mediation is unavailable",
            Error::Blocked => "access blocked",
            Error::TokenRejected => "token not recognized",
            Error::TokenRevoked => "token has been revoked",
            Error::RequestPending => "a grant request is already pending for this target",
            Error::AnnounceRequired => "announce your name before sending",
            Error::GrantBlocked(_) => "sender-recipient pair is blocked",
            Error::HandleExists => "handle already exists",
            Error::IdentityNotFound => "identity not found",
            Error::ProbeExpired => "connect probe has expired",
            Error::ProbeInvalid => "connect probe is invalid",
            Error::NotConnected => "not connected — complete probe handshake first",
            Error::AttachmentNotFound => "attachment not found or expired",
            Error::Internal => "internal server error",
            Error::ActiveSubscription => "token already has an active subscription",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn auth_failed_code() {
        assert_eq!(Error::AuthFailed.code(), "AUTH_FAILED");
    }

    #[test]
    fn token_expired_code() {
        assert_eq!(Error::TokenExpired.code(), "TOKEN_EXPIRED");
    }

    #[test]
    fn forbidden_code() {
        assert_eq!(Error::Forbidden.code(), "FORBIDDEN");
    }

    #[test]
    fn name_in_use_code() {
        assert_eq!(Error::NameInUse.code(), "NAME_IN_USE");
    }

    #[test]
    fn no_grant_code() {
        assert_eq!(Error::NoGrant.code(), "NO_GRANT");
    }

    #[test]
    fn grant_expired_code() {
        assert_eq!(Error::GrantExpired.code(), "GRANT_EXPIRED");
    }

    #[test]
    fn recipient_offline_code() {
        assert_eq!(Error::RecipientOffline.code(), "RECIPIENT_OFFLINE");
    }

    #[test]
    fn recipient_unknown_code() {
        assert_eq!(Error::RecipientUnknown.code(), "RECIPIENT_UNKNOWN");
    }

    #[test]
    fn bad_request_code() {
        assert_eq!(Error::BadRequest.code(), "BAD_REQUEST");
    }

    #[test]
    fn brief_required_code() {
        assert_eq!(Error::BriefRequired.code(), "BRIEF_REQUIRED");
    }

    #[test]
    fn mediation_unavailable_code() {
        assert_eq!(Error::MediationUnavailable.code(), "MEDIATION_UNAVAILABLE");
    }

    #[test]
    fn blocked_code() {
        assert_eq!(Error::Blocked.code(), "BLOCKED");
    }

    #[test]
    fn token_rejected_code() {
        assert_eq!(Error::TokenRejected.code(), "TOKEN_REJECTED");
    }

    #[test]
    fn token_revoked_code() {
        assert_eq!(Error::TokenRevoked.code(), "TOKEN_REVOKED");
    }
}
