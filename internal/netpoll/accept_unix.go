//go:build unix && !linux

package netpoll

import "golang.org/x/sys/unix"

func sysAccept(fd int) (int, unix.Sockaddr, error) {
	nfd, sa, err := unix.Accept(fd)
	if err != nil {
		return -1, nil, err
	}
	unix.CloseOnExec(nfd)
	if err := unix.SetNonblock(nfd, true); err != nil {
		_ = unix.Close(nfd)
		return -1, nil, err
	}
	return nfd, sa, nil
}
