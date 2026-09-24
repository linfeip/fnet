//go:build linux

package netpoll

import "golang.org/x/sys/unix"

// Writev writes iovs with a single writev(2) and may return a short count.
func Writev(fd int, iovs [][]byte) (int, error) {
	for {
		n, err := unix.Writev(fd, iovs)
		if err != unix.EINTR {
			if n < 0 {
				n = 0
			}
			return n, err
		}
	}
}
