//go:build unix && !linux && !darwin

package netpoll

// Writev falls back to sequential writes; it stops at the first short write.
func Writev(fd int, iovs [][]byte) (int, error) {
	total := 0
	for _, b := range iovs {
		if len(b) == 0 {
			continue
		}
		n, err := Write(fd, b)
		total += n
		if err != nil || n < len(b) {
			return total, err
		}
	}
	return total, nil
}
