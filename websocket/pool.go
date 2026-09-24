package websocket

import "github.com/linfeip/fnet"

// The worker pool is shared with package fnet; these aliases spare WebSocket
// users a second import.

// WorkerPool is fnet.WorkerPool.
type WorkerPool = fnet.WorkerPool

// WorkerPoolConfig is fnet.WorkerPoolConfig.
type WorkerPoolConfig = fnet.WorkerPoolConfig

// NewWorkerPool is fnet.NewWorkerPool.
var NewWorkerPool = fnet.NewWorkerPool

// AdaptPool is fnet.AdaptPool.
var AdaptPool = fnet.AdaptPool

// DefaultWorkerPool is the pool fnet.DefaultWorkerPool pointed to at start-up.
// Change it with SetDefaultWorkerPool so both packages agree.
var DefaultWorkerPool = fnet.DefaultWorkerPool

// SetDefaultWorkerPool replaces the default pool of both fnet and websocket.
func SetDefaultWorkerPool(p *WorkerPool) {
	if p != nil {
		fnet.SetDefaultWorkerPool(p)
		DefaultWorkerPool = p
	}
}
