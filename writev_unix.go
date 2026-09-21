//go:build unix && !linux && !darwin

package fnet

func writevFD(fd int, iovs [][]byte) (int, error) {
	total := 0
	for _, b := range iovs {
		if len(b) == 0 {
			continue
		}
		n, err := writeFD(fd, b)
		if n > 0 {
			total += n
		}
		if err != nil {
			return total, err
		}
		if n < len(b) {
			return total, nil
		}
	}
	return total, nil
}
