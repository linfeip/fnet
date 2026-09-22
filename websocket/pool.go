package websocket

import (
	"github.com/linfeip/fnet"
)

// WorkerPool is a type alias to fnet.WorkerPool.
type WorkerPool = fnet.WorkerPool

// WorkerPoolConfig is a type alias to fnet.WorkerPoolConfig.
type WorkerPoolConfig = fnet.WorkerPoolConfig

// NewWorkerPool creates a new high-concurrency sharded worker pool.
var NewWorkerPool = fnet.NewWorkerPool

// DefaultWorkerPool points to fnet.DefaultWorkerPool for unified worker pool scheduling
// across HTTP and WebSocket.
var DefaultWorkerPool = fnet.DefaultWorkerPool

// SetDefaultWorkerPool updates the global default WorkerPool across both HTTP and WebSocket.
func SetDefaultWorkerPool(p *WorkerPool) {
	fnet.SetDefaultWorkerPool(p)
	DefaultWorkerPool = p
}

// AdaptPool adapts a simple task submission function to a connection-aware WorkerPool function.
var AdaptPool = fnet.AdaptPool

