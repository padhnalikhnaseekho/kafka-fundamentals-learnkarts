#!/usr/bin/env bash
# Manage a local single-node Kafka 4.x (KRaft) broker from ~/kafka.
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
CONFIG="$KAFKA_HOME/config/server.properties"
LOG_DIR="${LOG_DIR:-/tmp/kraft-combined-logs}"
PID_FILE="/tmp/kafka-learnkarts.pid"
SERVER_LOG="/tmp/kafka-learnkarts.log"
BOOTSTRAP_PORT="${BOOTSTRAP_PORT:-9092}"

# ── helpers ────────────────────────────────────────────────────────────────────
die()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo "[kafka.sh] $*"; }
warn() { echo "[kafka.sh] WARN: $*" >&2; }

require() {
    [[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]] \
        || die "Kafka not found at KAFKA_HOME=$KAFKA_HOME"
}

# Returns 0 if the port is accepting connections (something is listening).
port_open() {
    local port=$1
    2>/dev/null </dev/tcp/localhost/"$port" && return 0 || return 1
}

# Returns the PID of whatever process owns the given TCP port, or empty string.
port_pid() {
    ss -tlnp 2>/dev/null \
        | awk -v port=":$1 " '$0 ~ port {match($0,/pid=([0-9]+)/,a); print a[1]; exit}'
}

# True when *our* managed broker is running (PID file exists + process alive).
is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

# ── commands ───────────────────────────────────────────────────────────────────
cmd_start() {
    require

    # Check for our own broker first.
    if is_running; then
        info "Kafka already running (pid=$(cat "$PID_FILE"), port=$BOOTSTRAP_PORT)"
        return
    fi

    # Check whether another process already owns the port.
    if port_open "$BOOTSTRAP_PORT"; then
        local other_pid
        other_pid=$(port_pid "$BOOTSTRAP_PORT")
        warn "Port $BOOTSTRAP_PORT is already in use by pid=${other_pid:-unknown}."
        warn "If that is a different Kafka instance, stop it first, or set:"
        warn "  BOOTSTRAP_PORT=<free-port> ./kafka.sh start"
        warn "  (and update listeners in $CONFIG to match)"
        exit 1
    fi

    # Format storage if never initialised.
    if [[ ! -f "$LOG_DIR/meta.properties" ]]; then
        info "Formatting KRaft storage at $LOG_DIR ..."
        local uuid
        uuid=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
        "$KAFKA_HOME/bin/kafka-storage.sh" format \
            --config "$CONFIG" \
            --cluster-id "$uuid" \
            2>&1 | sed 's/^/  /'
        info "Storage formatted (cluster-id=$uuid)"
    fi

    info "Starting Kafka 4.x broker (KRaft)..."
    "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG" \
        > "$SERVER_LOG" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"

    # Wait up to 20 s for broker to become ready.
    local retries=20
    while (( retries-- > 0 )); do
        sleep 1
        if "$KAFKA_HOME/bin/kafka-topics.sh" \
                --bootstrap-server "localhost:$BOOTSTRAP_PORT" --list \
                > /dev/null 2>&1; then
            info "Kafka is ready  (pid=$pid)"
            info "Bootstrap : localhost:$BOOTSTRAP_PORT"
            info "Log dir   : $LOG_DIR"
            info "Server log: $SERVER_LOG"
            return
        fi
    done

    # If we get here the broker failed to start.
    rm -f "$PID_FILE"
    die "Broker did not become ready. Last lines of $SERVER_LOG:$(tail -5 "$SERVER_LOG" | sed 's/^/\n  /')"
}

cmd_stop() {
    if ! is_running; then
        info "Kafka is not running."
        rm -f "$PID_FILE"
        return
    fi

    local pid
    pid=$(cat "$PID_FILE")
    info "Stopping Kafka (pid=$pid) ..."
    "$KAFKA_HOME/bin/kafka-server-stop.sh" 2>/dev/null || kill "$pid" || true

    local retries=20
    while (( retries-- > 0 )) && kill -0 "$pid" 2>/dev/null; do
        sleep 1
    done

    if kill -0 "$pid" 2>/dev/null; then
        warn "Process still alive after 20 s — sending SIGKILL"
        kill -9 "$pid" 2>/dev/null || true
    fi

    rm -f "$PID_FILE"
    info "Kafka stopped."
}

cmd_status() {
    require
    if is_running; then
        local pid
        pid=$(cat "$PID_FILE")
        info "Kafka is RUNNING (pid=$pid, port=$BOOTSTRAP_PORT)"
        info "Topics:"
        "$KAFKA_HOME/bin/kafka-topics.sh" \
            --bootstrap-server "localhost:$BOOTSTRAP_PORT" --list 2>/dev/null \
            | sed 's/^/  /' \
            || warn "  Could not list topics."
    else
        info "Kafka is NOT running."
        if port_open "$BOOTSTRAP_PORT"; then
            local other_pid
            other_pid=$(port_pid "$BOOTSTRAP_PORT")
            warn "Port $BOOTSTRAP_PORT is in use by another process (pid=${other_pid:-?})."
        fi
    fi
}

cmd_restart() {
    cmd_stop
    sleep 1
    cmd_start
}

cmd_logs() {
    [[ -f "$SERVER_LOG" ]] || die "No server log at $SERVER_LOG"
    tail -f "$SERVER_LOG"
}

# ── usage ──────────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $0 {start|stop|restart|status|logs}

  start    Format KRaft storage if needed, then start the broker.
  stop     Gracefully stop the broker.
  restart  stop + start.
  status   Show whether Kafka is running and list topics.
  logs     Tail the server log (Ctrl-C to exit).

Environment variables:
  KAFKA_HOME       Path to Kafka installation  (default: ~/kafka)
  LOG_DIR          KRaft data directory         (default: /tmp/kraft-combined-logs)
  BOOTSTRAP_PORT   Broker listener port         (default: 9092)

If port 9092 is taken by another Kafka instance, override:
  BOOTSTRAP_PORT=9094 LOG_DIR=/tmp/kraft-alt ./kafka.sh start
  (update config/server.properties listeners accordingly)
EOF
    exit 1
}

case "${1:-}" in
    start)   cmd_start   ;;
    stop)    cmd_stop    ;;
    restart) cmd_restart ;;
    status)  cmd_status  ;;
    logs)    cmd_logs    ;;
    *)       usage       ;;
esac
