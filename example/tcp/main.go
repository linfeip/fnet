// Command tcp is a small chat-room game server on fnet.Server: length-prefixed
// packets, login, heartbeats, a room goroutine that broadcasts, dropping
// players who cannot keep up, and a graceful shutdown that warns everyone
// before it stops.
//
// Packet: | len uint32 BE | cmd uint16 BE | body |, where len counts cmd+body.
//
//	go run ./example/tcp            # server on 127.0.0.1:7001
//	go run ./example/tcp -bots 3    # the server plus three chatting bots
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/linfeip/fnet"
)

const (
	cmdHeartbeat uint16 = 1
	cmdLogin     uint16 = 2 // body: the player's name
	cmdChat      uint16 = 3 // body: text; the room gets "name: text"
	cmdNotice    uint16 = 4 // server to client: a notice
)

func packet(cmd uint16, body []byte) []byte {
	b := make([]byte, 6+len(body))
	binary.BigEndian.PutUint32(b, uint32(2+len(body)))
	binary.BigEndian.PutUint16(b[4:], cmd)
	copy(b[6:], body)
	return b
}

// session is one player's state, kept in the connection's Context. Only the
// connection's own callbacks touch name: they never run concurrently.
type session struct {
	conn *fnet.Conn
	name string // empty until login
}

// room owns its players; only its goroutine touches them, so it needs no lock.
type room struct {
	events chan func(players map[*session]bool)
}

func newRoom() *room {
	r := &room{events: make(chan func(map[*session]bool), 1024)}
	go func() {
		players := map[*session]bool{}
		for ev := range r.events {
			ev(players)
		}
	}()
	return r
}

func (r *room) join(s *session)  { r.events <- func(p map[*session]bool) { p[s] = true } }
func (r *room) leave(s *session) { r.events <- func(p map[*session]bool) { delete(p, s) } }

// broadcast encodes a packet once and writes the same bytes to every player:
// Write never blocks and copies what it cannot send at once. A player whose
// output queue is full cannot keep up and is dropped.
func (r *room) broadcast(cmd uint16, body []byte) {
	pkt := packet(cmd, body)
	r.events <- func(players map[*session]bool) {
		for s := range players {
			if _, err := s.conn.Write(pkt); errors.Is(err, fnet.ErrWriteBufferFull) {
				delete(players, s)
				_ = s.conn.Close()
			}
		}
	}
}

// flush waits until the events sent so far have run.
func (r *room) flush() {
	done := make(chan struct{})
	r.events <- func(map[*session]bool) { close(done) }
	<-done
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7001", "listen address")
	bots := flag.Int("bots", 0, "chatting bots to start")
	flag.Parse()

	lobby := newRoom()
	srv := &fnet.Server{
		Addr:             *addr,
		Split:            fnet.LengthField{Size: 4, Strip: 4}.Split, // OnMessage gets cmd+body
		MaxMessageSize:   64 << 10,
		MaxOutboundBytes: 256 << 10,        // a player this far behind is dropped
		IdleTimeout:      30 * time.Second, // clients send a heartbeat every 10s

		OnOpen: func(c *fnet.Conn) { c.SetContext(&session{conn: c}) },
		OnMessage: func(c *fnet.Conn, msg []byte) {
			s := c.Context().(*session)
			if len(msg) < 2 {
				_ = c.Close()
				return
			}
			cmd, body := binary.BigEndian.Uint16(msg), msg[2:]
			switch {
			case cmd == cmdHeartbeat:
				_, _ = c.Write(packet(cmdHeartbeat, nil))
			case cmd == cmdLogin && s.name == "":
				s.name = string(body) // string() copies: msg is only valid during the call
				lobby.join(s)
				lobby.broadcast(cmdNotice, []byte(s.name+" joined"))
			case cmd == cmdChat && s.name != "":
				lobby.broadcast(cmdChat, []byte(s.name+": "+string(body)))
			default:
				_, _ = c.Write(packet(cmdNotice, []byte("bad request")))
				_ = c.Close() // the notice is flushed before the connection closes
			}
		},
		OnClose: func(c *fnet.Conn, err error) {
			s := c.Context().(*session)
			if s.name != "" {
				lobby.leave(s)
				lobby.broadcast(cmdNotice, []byte(s.name+" left"))
			}
			log.Printf("%s closed: %v", c.RemoteAddr(), err)
		},
	}

	stopped := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		lobby.broadcast(cmdNotice, []byte("server going down for maintenance"))
		lobby.flush() // queued for every player before the connections close
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
		close(stopped)
	}()

	for i := 1; i <= *bots; i++ {
		go bot(*addr, fmt.Sprintf("bot%d", i), time.Duration(1+i)*time.Second)
	}
	log.Printf("listening on %s", *addr)
	if err := srv.ListenAndServe(); err != fnet.ErrServerClosed {
		log.Fatal(err)
	}
	<-stopped // Shutdown returns once every OnClose has run
}

// bot logs in, keeps its connection alive with heartbeats, chats every
// interval, and prints what the room says.
func bot(addr, name string, every time.Duration) {
	time.Sleep(200 * time.Millisecond) // let the server start
	c, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("%s: %v", name, err)
		return
	}
	defer c.Close()
	go func() {
		var hdr [6]byte
		for {
			if _, err := io.ReadFull(c, hdr[:]); err != nil {
				return
			}
			body := make([]byte, binary.BigEndian.Uint32(hdr[:])-2)
			if _, err := io.ReadFull(c, body); err != nil {
				return
			}
			if binary.BigEndian.Uint16(hdr[4:]) != cmdHeartbeat {
				log.Printf("[%s] %s", name, body)
			}
		}
	}()
	if _, err := c.Write(packet(cmdLogin, []byte(name))); err != nil {
		return
	}
	heartbeat := time.NewTicker(10 * time.Second)
	chat := time.NewTicker(every)
	defer heartbeat.Stop()
	defer chat.Stop()
	for n := 1; ; {
		var err error
		select {
		case <-heartbeat.C:
			_, err = c.Write(packet(cmdHeartbeat, nil))
		case <-chat.C:
			_, err = c.Write(packet(cmdChat, []byte(fmt.Sprintf("hello #%d", n))))
			n++
		}
		if err != nil {
			return
		}
	}
}
