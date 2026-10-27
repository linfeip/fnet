// Package httpx holds the small helpers that package fhttp and package websocket share.
package httpx

import (
	"strings"
	"time"
)

// HasToken reports whether the comma-separated header values contain token (case-insensitive).
func HasToken(values []string, token string) bool {
	for _, v := range values {
		for v != "" {
			var t string
			t, v, _ = strings.Cut(v, ",")
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// DeadlineAfter returns the instant timeout from now; timeout<=0 means no limit and the zero value is returned.
func DeadlineAfter(timeout time.Duration) time.Time {
	if timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(timeout)
}
