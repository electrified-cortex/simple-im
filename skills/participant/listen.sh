#!/usr/bin/env bash
# S-IM listener — persistent SSE monitor for Simple IM
# Reads service.url, service.handle, service.token from $SCRIPT_DIR
# Emits to STDOUT only on REAL messages (notify). All operational chatter
# (announce/reconnect/sub/breadcrumb/presence/keepalive) -> STDERR (logged,
# not a wake). [2026-06-24: per operator — "S-IM should ONLY notify you of
# real messages and nothing else." Crash/health is covered by a separate watcher.]

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

BACKOFF=2
MAX_BACKOFF=60
STABLE_THRESHOLD=30
FAIL_COUNT=0
FAIL_LIMIT=10   # consecutive fast-failed reconnects → fail-hard: unravel + alert.
               # ~5 min with capped backoff: rides through a routine redeploy/bounce
               # (auto-reconnect), only unravels on a genuine prolonged outage.

connect() {
    local token
    token="$(cat "$TOKEN_FILE" 2>/dev/null | tr -d '[:space:]')"

    # If no token, register first
    if [[ -z "$token" ]]; then
        echo >&2 "sim: no token — registering via /agents/register"
        local reg_result
        reg_result=$(curl -s -X POST "$SIM_URL/agents/register" \
            -H "Content-Type: application/json" 2>/dev/null)
        token=$(echo "$reg_result" | grep -o '"token":"[^"]*"' | sed 's/"token":"//;s/"//')
        if [[ -n "$token" ]]; then
            echo "$token" > "$TOKEN_FILE"
            echo >&2 "sim: registered — token saved"
        else
            echo >&2 "sim: registration failed — ${reg_result}"
            return 1
        fi
    fi

    local auth_header=""
    if [[ -n "$token" ]]; then
        auth_header="Authorization: Bearer $token"
    fi

    local connect_start
    connect_start=$(date +%s)

    curl -s -N -X POST "$SIM_URL/listen" \
        -H "Content-Type: application/json" \
        ${auth_header:+-H "$auth_header"} \
        -d '{}' 2>/dev/null | while IFS= read -r line; do
        # Skip empty lines and keepalives
        [[ -z "$line" ]] && continue
        [[ "$line" == :* ]] && continue

        # Detect non-SSE error response (e.g. 401 AUTH_FAILED on stale/invalid token).
        # S-IM restart clears all tokens — an agent with a pre-restart token in TOKEN_FILE
        # will get AUTH_FAILED on the first POST /listen. Without this guard, FAIL_COUNT
        # would increment 10 times and trigger a false SIM-DOWN. Instead: clear the file
        # and break; the outer loop's next iteration will call /agents/register for a fresh token.
        if [[ "$line" == *'"AUTH_FAILED"'* || "$line" == *'"TOKEN_REJECTED"'* ]]; then
            echo >&2 "sim: token rejected — clearing for re-registration"
            > "$TOKEN_FILE"
            break
        fi

        if [[ "$line" == data:* ]]; then
            data="${line#data: }"
            type=$(echo "$data" | grep -o '"type":"[^"]*"' | head -1 | sed 's/"type":"//;s/"//')
            event=$(echo "$data" | grep -o '"event":"[^"]*"' | head -1 | sed 's/"event":"//;s/"//')

            case "$type/$event" in
                service/welcome)
                    # Welcome no longer echoes the token — use $token (set at top of connect()).
                    # Announce handle to go live.
                    announce_result=$(curl -s -w "\n%{http_code}" -X POST "$SIM_URL/announce" \
                        -H "Content-Type: application/json" \
                        -H "Authorization: Bearer $token" \
                        -d "{\"name\":\"$HANDLE\"}" 2>/dev/null)
                    announce_code="${announce_result##*$'\n'}"
                    echo >&2 "sim: announce HTTP $announce_code"
                    if [[ "$announce_code" != "204" ]]; then
                        echo >&2 "sim: announce failed — body: ${announce_result%$'\n'*}"
                    fi
                    ;;
                service/superseded|service/cancelled)
                    echo >&2 "sim: stream superseded — reconnecting"
                    break
                    ;;
                service/revoked)
                    echo >&2 "sim: token revoked — re-registering"
                    > "$TOKEN_FILE"
                    # Signal outer loop: revoke is intentional governance, not a server failure.
                    # FAIL_COUNT must not be incremented. Bash runs the pipe-RHS in a subshell so
                    # we can't set FAIL_COUNT here directly — use a flag file instead.
                    echo "REVOKED" > "$SCRIPT_DIR/.sim_revoke_flag"
                    break
                    ;;
                sub/*)
                    echo >&2 "sim: subscription event (sub_id embedded)"
                    ;;
                notify/*)
                    pending=$(echo "$data" | grep -o '"pending":[0-9]*' | grep -o '[0-9]*')
                    echo "sim: notify pending=${pending:-?}"
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
    connect
    connect_end=$(date +%s)
    elapsed=$((connect_end - connect_start))

    # Re-registration triggers — two cases, both are intentional, not server failures:
    #   1. Revoke flag: governor explicitly cycled the token (service/revoked handler).
    #   2. Empty/missing TOKEN_FILE: AUTH_FAILED on stale token cleared the file (redeploy recovery).
    # In both cases: reset backoff, skip FAIL_COUNT increment, continue to re-register.
    if [[ -f "$SCRIPT_DIR/.sim_revoke_flag" ]] || [[ ! -s "$TOKEN_FILE" ]]; then
        rm -f "$SCRIPT_DIR/.sim_revoke_flag"
        BACKOFF=2
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
