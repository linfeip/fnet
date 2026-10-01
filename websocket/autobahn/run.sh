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
    rm -f autobahn_server
}
trap cleanup EXIT INT TERM

sleep 1

mkdir -p reports

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
    echo "Notice: docker or wstest not found on host."
    echo "To run the complete official Autobahn fuzzing client against websocket:"
    echo "  1) Install Docker or wstest"
    echo "  2) Run ./run.sh again"
    exit 0
fi

echo "=== Autobahn test complete! Reports generated in $DIR/reports/servers/index.html ==="
