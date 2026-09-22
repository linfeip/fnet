package fnet

import (
	"sync/atomic"
	"time"
)

// HTTP dates carry one-second resolution (RFC 9110 5.6.7), so re-formatting the
// Date header on every response is wasted work. The formatted string is
// refreshed at most once per second and shared across responses.
var (
	cachedDate    atomic.Value // string
	cachedDateSec atomic.Int64
)

func httpDateNow() string {
	now := time.Now()
	sec := now.Unix()
	if cachedDateSec.Load() == sec {
		if s, ok := cachedDate.Load().(string); ok {
			return s
		}
	}
	s := now.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	cachedDate.Store(s)
	cachedDateSec.Store(sec)
	return s
}
