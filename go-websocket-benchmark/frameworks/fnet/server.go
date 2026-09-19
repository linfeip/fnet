package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"go-websocket-benchmark/config"
	"go-websocket-benchmark/frameworks"
	"go-websocket-benchmark/logging"

	"fnet"
	"fnet/websocket"
)

var (
	nodelay = flag.Bool("nodelay", true, `tcp nodelay`)
	_       = flag.Int("b", 1024, `read buffer size`)
	_       = flag.Int("mrb", 4096, `max read buffer size`)
	_       = flag.Int64("m", 1024*1024*1024*2, `memory limit`)
	_       = flag.Int("mb", 10000, `max blocking online num, e.g. 10000`)
	_       = flag.Bool("tpn", true, `benchmark: whether enable TPN caculation`)

	upgrader = &websocket.Upgrader{}
)

func main() {
	flag.Parse()

	addrs, err := config.GetFrameworkServerAddrs(config.Fnet)
	if err != nil {
		logging.Fatalf("GetFrameworkBenchmarkAddrs(%v) failed: %v", config.Fnet, err)
	}
	servers := startServers(addrs)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	for _, s := range servers {
		_ = s.Close()
	}
}

func startServers(addrs []string) []*fnet.Server {
	servers := make([]*fnet.Server, 0, len(addrs))
	for _, addr := range addrs {
		mux := &http.ServeMux{}
		mux.HandleFunc("/ws", onWebsocket)
		frameworks.HandleCommon(mux)

		s := &fnet.Server{
			Addr:    addr,
			Handler: mux,
		}
		servers = append(servers, s)
		go func(srv *fnet.Server) {
			logging.Printf("server exit: %v", srv.ListenAndServe())
		}(s)
	}
	return servers
}

func onWebsocket(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r)
	if err != nil {
		log.Printf("upgrade failed: %v", err)
		return
	}
	// Must block until the WebSocket session ends: fnet closes the
	// hijacked connection when the HTTP handler returns.
	defer c.Close()
	frameworks.SetNoDelay(c.NetConn(), *nodelay)
	_ = c.SetReadDeadline(time.Time{})

	_ = c.Handle(func(op websocket.OpCode, msg []byte) error {
		return c.WriteMessage(op, msg)
	})
}
