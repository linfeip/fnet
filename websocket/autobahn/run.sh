#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=== Building websocket Autobahn echo test server ==="
go build -o autobahn_server server.go

echo "=== Starting echo server (:9001) ==="
./autobahn_server -addr :9001 &
SERVER_PID=$!

cleanup() {
    echo "=== Stopping test server (PID: $SERVER_PID) ==="
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    rm -f autobahn_server "$started"
}
trap cleanup EXIT INT TERM

sleep 1
if ! kill -0 "$SERVER_PID" 2>/dev/null; then # it could not listen (say :9001 is in use): the run would test another server
    echo "Error: the echo server did not start, see its output above."
    exit 1
fi

mkdir -p reports
started=$(mktemp) # the report must be newer than this, or the run wrote none

if command -v docker >/dev/null 2>&1; then
    echo "=== Running Autobahn Testsuite via Docker ==="
    docker run --rm \
        --net=host \
        -v "$DIR/fuzzingclient.json:/fuzzingclient.json:ro" \
        -v "$DIR/reports:/reports" \
        crossbario/autobahn-testsuite \
        wstest -m fuzzingclient -s /fuzzingclient.json
elif command -v wstest >/dev/null 2>&1; then
    echo "=== Running Autobahn Testsuite via local wstest ==="
    wstest -m fuzzingclient -s fuzzingclient.json
else
    echo "Error: docker or wstest not found on host, so nothing was tested."
    echo "To run the complete official Autobahn fuzzing client against websocket:"
    echo "  1) Install Docker or wstest"
    echo "  2) Run ./run.sh again"
    exit 1
fi

# Autobahn counts OK, NON-STRICT and INFORMATIONAL as passing; any other result of a case, or of its close, fails the run.
results="$DIR/reports/servers/index.json"
if [ ! "$results" -nt "$started" ]; then
    echo "Error: the run wrote no report to $results."
    exit 1
fi
failed=$(grep -oE '"behavior(Close)?": "[^"]*"' "$results" | grep -cvE '"(OK|NON-STRICT|INFORMATIONAL)"$' || true)
if [ "$failed" -ne 0 ]; then
    echo "=== Autobahn found $failed failing results, see $DIR/reports/servers/index.html ==="
    exit 1
fi
echo "=== Autobahn test passed! Reports generated in $DIR/reports/servers/index.html ==="
