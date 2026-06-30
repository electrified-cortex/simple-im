#!/usr/bin/env bash
# S-IM listener — persistent SSE monitor for Simple IM
# Reads service.url, service.handle, service.token from $SCRIPT_DIR
# Emits to STDOUT only on REAL messages (notify). All operational chatter
# (announce/reconnect/sub/breadcrumb/presence/keepalive) -> STDERR (logged,
# not a wake). [2026-06-24: per operator — "S-IM should ONLY notify you of
# real messages and nothing else." Crash/health is covered by a separate watcher.]
#
# 15-0029 (final form): the client NEVER self-registers. A participant credential is
# issued by the governor out-of-band; this script only LISTENS with that credential and
# captures the subscription_id from every welcome. Credential loss (no token / 401 /
# revoked) and name conflicts are terminal exit codes for the governor to act on:
#   exit 2 -> NAME_IN_USE  (governor must rebind the identity to a new credential)
#   exit 3 -> CREDENTIAL_LOST (governor must issue a new token)
# All JSON field extraction uses jq (BT-NFR4).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SIM_URL_FILE="$SCRIPT_DIR/service.url"
HANDLE_FILE="$SCRIPT_DIR/service.handle"
TOKEN_FILE="$SCRIPT_DIR/service.token"

SIM_URL="$(cat "$SIM_URL_FILE" 2>/dev/null | tr -d '[:space:]')"
HANDLE="$(cat "$HANDLE_FILE" 2>/dev/null | tr -d '[:space:]')"

if [[ -z "$SIM_URL" || -z "$HANDLE" ]]; then
    echo "ERROR: service.url or service.handle missing — misconfigured"
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required but not found on PATH"
    exit 1
fi

BACKOFF=2
MAX_BACKOFF=60
STABLE_THRESHOLD=30
FAIL_COUNT=0
FAIL_LIMIT=10   # consecutive fast-failed reconnects → fail-hard: unravel + alert.
               # ~5 min with capped backoff: rides through a routine redeploy/bounce
               # (auto-reconnect), only unravels on a genuine prolonged outage.
FORCE_LISTEN=""  # Set to "--force" when ACTIVE_SUBSCRIPTION flag fires; cleared after each connect()

connect() {
    local listen_extra=""
    # The ONLY force path: a 409 ACTIVE_SUBSCRIPTION from /listen supersedes our own stale
    # subscription on the SAME token. This is a subscription-level reclaim, never a name takeover.
    [[ "${1:-}" == "--force" ]] && listen_extra="?force=true"

    local token
    token="$(cat "$TOKEN_FILE" 2>/dev/null | tr -d '[:space:]')"

    # No credential — the governor must provision one (the client never self-registers).
    if [[ -z "$token" ]]; then
        echo >&2 "[SIM] CREDENTIAL_LOST: governor must issue a new token"
        echo "CREDENTIAL_LOST" > "$SCRIPT_DIR/.sim_credential_lost_flag"
        return 1
    fi

    local connect_start
    connect_start=$(date +%s)

    curl -s -N -X POST "$SIM_URL/listen${listen_extra}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $token" \
        -d '{}' 2>/dev/null | while IFS= read -r line; do
        # Skip empty lines and keepalives
        [[ -z "$line" ]] && continue
        [[ "$line" == :* ]] && continue

        # Non-SSE error response (e.g. 401 AUTH_FAILED on an invalid/stale token). In the final
        # form there is no self-re-registration: a lost credential is terminal (exit 3) so the
        # governor reissues out-of-band.
        if [[ "$line" == *'"AUTH_FAILED"'* || "$line" == *'"TOKEN_REJECTED"'* ]]; then
            echo >&2 "[SIM] CREDENTIAL_LOST: governor must issue a new token"
            echo "CREDENTIAL_LOST" > "$SCRIPT_DIR/.sim_credential_lost_flag"
            break
        fi

        if [[ "$line" == *'"ACTIVE_SUBSCRIPTION"'* ]]; then
            echo >&2 "sim: orphaned subscription detected — will reclaim our own slot on next connect"
            echo "FORCE_RECLAIM" > "$SCRIPT_DIR/.sim_force_reclaim_flag"
            break
        fi

        if [[ "$line" == data:* ]]; then
            data="${line#data: }"
            type=$(printf '%s' "$data" | jq -r '.type // empty' 2>/dev/null)
            event=$(printf '%s' "$data" | jq -r '.event // empty' 2>/dev/null)

            case "$type/$event" in
                service/welcome)
                    # BT-FR1: capture the subscription_id (present on EVERY welcome and equal to
                    # the participant token) and persist it whenever it differs from the stored
                    # value — first connect AND idempotently on each reconnect.
                    sub_id=$(printf '%s' "$data" | jq -r '.subscription_id // empty' 2>/dev/null)
                    if [[ -n "$sub_id" && "$sub_id" != "$token" ]]; then
                        printf '%s' "$sub_id" > "$TOKEN_FILE"
                        token="$sub_id"
                        echo >&2 "sim: subscription_id captured and persisted"
                    fi
                    # Announce our handle to go live. No takeover flag in the body (final form):
                    # a name held by another credential is NAME_IN_USE and requires a governor rebind.
                    announce_result=$(curl -s -w "\n%{http_code}" -X POST "$SIM_URL/announce" \
                        -H "Content-Type: application/json" \
                        -H "Authorization: Bearer $token" \
                        -d "{\"name\":\"$HANDLE\"}" 2>/dev/null)
                    announce_code="${announce_result##*$'\n'}"
                    case "$announce_code" in
                        204)
                            echo >&2 "sim: announce ok"
                            ;;
                        409)
                            echo >&2 "[SIM] NAME_IN_USE: governor rebind required for $HANDLE"
                            echo "NAME_IN_USE" > "$SCRIPT_DIR/.sim_name_in_use_flag"
                            break
                            ;;
                        401)
                            echo >&2 "[SIM] CREDENTIAL_LOST: governor must issue a new token"
                            echo "CREDENTIAL_LOST" > "$SCRIPT_DIR/.sim_credential_lost_flag"
                            break
                            ;;
                        *)
                            echo >&2 "sim: announce HTTP $announce_code — body: ${announce_result%$'\n'*}"
                            ;;
                    esac
                    ;;
                service/superseded|service/cancelled)
                    echo >&2 "sim: stream superseded — reconnecting"
                    break
                    ;;
                service/revoked)
                    echo >&2 "[SIM] CREDENTIAL_LOST: session revoked"
                    echo "CREDENTIAL_LOST" > "$SCRIPT_DIR/.sim_credential_lost_flag"
                    break
                    ;;
                sub/*)
                    echo >&2 "sim: subscription event (sub_id embedded)"
                    ;;
                notify/*)
                    pending=$(printf '%s' "$data" | jq -r '.pending // 0' 2>/dev/null)
                    if [[ "${pending:-0}" -gt 0 ]]; then
                        echo "sim: notify pending=${pending}"
                    else
                        echo >&2 "sim: notify pending=0 (no wake)"
                    fi
                    ;;
                presence/*)
                    participant=$(printf '%s' "$data" | jq -r '.participant // empty' 2>/dev/null)
                    echo >&2 "sim: presence event=${event} participant=${participant}"
                    ;;
                *)
                    echo >&2 "sim: event type=$type event=$event"
                    ;;
            esac
        fi
    done
}

while true; do
    connect_start=$(date +%s)
    connect "$FORCE_LISTEN"
    FORCE_LISTEN=""  # reset after each connect() call
    connect_end=$(date +%s)
    elapsed=$((connect_end - connect_start))

    # Terminal: credential lost (no token / 401 / revoked). The governor must issue a new token
    # out-of-band; the client does not self-recover. Exit 3 so the supervisor surfaces it.
    if [[ -f "$SCRIPT_DIR/.sim_credential_lost_flag" ]]; then
        rm -f "$SCRIPT_DIR/.sim_credential_lost_flag"
        exit 3
    fi

    # Terminal: our name is held by a different credential. Only a governor rebind can reclaim it.
    if [[ -f "$SCRIPT_DIR/.sim_name_in_use_flag" ]]; then
        rm -f "$SCRIPT_DIR/.sim_name_in_use_flag"
        exit 2
    fi

    # Orphaned-subscription recovery (ACTIVE_SUBSCRIPTION 409 from /listen). Reclaim our OWN slot
    # via ?force=true on the NEXT connect() — same-token only, no DELETE, no FAIL_COUNT increment.
    if [[ -f "$SCRIPT_DIR/.sim_force_reclaim_flag" ]]; then
        rm -f "$SCRIPT_DIR/.sim_force_reclaim_flag"
        FORCE_LISTEN="--force"
        BACKOFF=2
        echo >&2 "sim: orphaned subscription — next connect will reclaim our own slot"
        continue
    fi

    if (( elapsed >= STABLE_THRESHOLD )); then
        # connection held → healthy; reset failure tracking
        BACKOFF=2
        FAIL_COUNT=0
    else
        # fast failure → S-IM likely unreachable
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    # Fail-hard: do NOT retry silently forever. After FAIL_LIMIT consecutive
    # fast-failed reconnects, S-IM is treated as hard-down. Emit a clear alert
    # (stdout → wakes this participant) and UNRAVEL (exit) so the failure is
    # visible and acted on, rather than masked by endless reconnects.
    if (( FAIL_COUNT >= FAIL_LIMIT )); then
        echo "SIM-DOWN: S-IM unreachable after ${FAIL_COUNT} consecutive attempts — listener unraveling (fail-hard). S-IM needs attention."
        exit 1
    fi

    echo >&2 "sim: disconnected — reconnecting in ${BACKOFF}s (fail ${FAIL_COUNT}/${FAIL_LIMIT})"
    sleep "$BACKOFF"
    BACKOFF=$(( BACKOFF * 2 > MAX_BACKOFF ? MAX_BACKOFF : BACKOFF * 2 ))
done
