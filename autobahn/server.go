package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/linfeip/fnet"
	"github.com/linfeip/fnet/websocket"
)

func main() {
	eventPort := flag.Int("event-port", 9001, "Port for event-driven WebSocket echo server")
	goroutinePort := flag.Int("goroutine-port", 9002, "Port for blocking goroutine WebSocket echo server")
	enableCompression := flag.Bool("compress", true, "Enable permessage-deflate compression")
	flag.Parse()

	// 1. Event-Driven Echo Server (fnet Reactor Poller)
	eventMux := http.NewServeMux()
	eventUpgrader := &websocket.Upgrader{
		EnableCompression: *enableCompression,
		OnMessage: func(conn *websocket.Conn, op websocket.OpCode, msg []byte) {
			_ = conn.WriteMessage(op, msg)
		},
	}
	eventMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = eventUpgrader.Upgrade(w, r)
	})
	eventSrv := &fnet.Server{
		Addr:    fmt.Sprintf(":%d", *eventPort),
		Handler: eventMux,
	}

	go func() {
		log.Printf("[Event-Driven] WebSocket echo server listening on ws://0.0.0.0:%d", *eventPort)
		if err := eventSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("event server error: %v", err)
		}
	}()

	// 2. Goroutine Echo Server (Traditional blocking read loop)
	goroutineMux := http.NewServeMux()
	goroutineUpgrader := &websocket.Upgrader{
		EnableCompression: *enableCompression,
	}
	goroutineMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := goroutineUpgrader.Upgrade(w, r)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.Handle(func(op websocket.OpCode, msg []byte) error {
			return conn.WriteMessage(op, msg)
		})
	})
	goroutineSrv := &fnet.Server{
		Addr:    fmt.Sprintf(":%d", *goroutinePort),
		Handler: goroutineMux,
	}

	go func() {
		log.Printf("[Goroutine]    WebSocket echo server listening on ws://0.0.0.0:%d", *goroutinePort)
		if err := goroutineSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("goroutine server error: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down echo servers...")
	_ = eventSrv.Close()
	_ = goroutineSrv.Close()
	log.Println("Servers stopped.")
}
