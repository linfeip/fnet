package fhttp

import (
	"slices"
	"strings"
	"testing"

	"github.com/linfeip/fnet/internal/units"
)

// frameAll simulates the engine: data arrives step bytes at a time. It returns the messages that were
// split out and the error it ran into.
func frameAll(in string, step int) ([]string, error) {
	f := framer{maxHeaderBytes: units.KB}
	var msgs []string
	var buf []byte
	for i := 0; i < len(in); i += step {
		buf = append(buf, in[i:min(i+step, len(in))]...)
		for {
			n, err := f.next(buf)
			if err != nil {
				return msgs, err
			}
			if n == 0 {
				break
			}
			msgs = append(msgs, string(buf[:n]))
			buf = buf[n:]
		}
	}
	return msgs, nil
}

func TestFramer(t *testing.T) {
	const (
		get  = "GET / HTTP/1.1\r\nHost: a\r\n\r\n"
		post = "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\n"
	)
	tests := []struct {
		name string
		in   string
		want []string
		err  error
	}{
		{"单个请求", get, []string{get}, nil},
		{"pipelining", get + get + get, []string{get, get, get}, nil},
		{"只切到头部结束符，不含请求体", post + "hello", []string{post}, nil},
		{"头部过大且未结束", "GET / HTTP/1.1\r\nX: " + strings.Repeat("a", 1100), nil, errHeaderTooLarge},
		{"头部过大", "GET / HTTP/1.1\r\nX: " + strings.Repeat("a", 1100) + "\r\n\r\n", nil, errHeaderTooLarge},
	}
	for _, tt := range tests {
		for _, step := range []int{len(tt.in), 1, 7} {
			got, err := frameAll(tt.in, step)
			if err != tt.err || !slices.Equal(got, tt.want) {
				t.Errorf("%s(step=%d): got %q, err=%v; want %q, err=%v", tt.name, step, got, err, tt.want, tt.err)
			}
		}
	}
}
