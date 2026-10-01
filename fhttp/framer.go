package fhttp

import (
	"bytes"
	"errors"
)

// errHeaderTooLarge means the request line and headers exceed MaxHeaderBytes.
var errHeaderTooLarge = errors.New("fhttp: request header too large")

// headerTerminator is the terminator of the request headers.
var headerTerminator = []byte("\r\n\r\n")

// framer splits request messages inside fnet's callbacks: it only looks for the header terminator and hands the
// request line and headers as a whole to the standard library's http.ReadRequest. It does not recognize request
// bodies; requests with a body are rejected by the worker (see conn.handle).
type framer struct {
	maxHeaderBytes int
	scanned        int // bytes of this message known to lack the terminator; scanning resumes here, so slow clients cause no rescans
}

// next looks for the header terminator in data (which starts at the beginning of a message): n>0 means data[:n] is
// a complete request header, n==0 means there is not enough data and next must be called again with the same
// message start once more data arrives.
func (f *framer) next(data []byte) (n int, err error) {
	// Back up by len-1 bytes, in case the terminator is split across two arrivals of data.
	from := max(f.scanned-len(headerTerminator)+1, 0)
	i := bytes.Index(data[from:], headerTerminator)
	if i < 0 {
		if len(data) > f.maxHeaderBytes {
			return 0, errHeaderTooLarge
		}
		f.scanned = len(data)
		return 0, nil
	}
	n = from + i + len(headerTerminator)
	if n > f.maxHeaderBytes {
		return 0, errHeaderTooLarge
	}
	f.scanned = 0
	return n, nil
}
