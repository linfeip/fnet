#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=== Building fnet Autobahn echo test server ==="
go build -o autobahn_server server.go

echo "=== Starting echo servers (event-driven :9001, goroutine :9002) ==="
./autobahn_server -event-port 9001 -goroutine-port 9002 &
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
    echo "To run the complete official Autobahn fuzzing client against fnet:"
    echo "  1) Install Docker or wstest"
    echo "  2) Run ./run.sh again"
    echo ""
    echo "Tip: You can always run the built-in Go test suite directly without external tools:"
    echo "  go test -v ./websocket -run TestAutobahn"
    exit 0
fi

echo "=== Autobahn test complete! Reports generated in $DIR/reports/servers/index.html ==="
