//go:build unix && !linux

package netpoll

import (
	"net/netip"

	"golang.org/x/sys/unix"
)

func sysAccept(fd int) (int, netip.AddrPort, error) {
	nfd, sa, err := unix.Accept(fd)
	if err != nil {
		return -1, netip.AddrPort{}, err
	}
	unix.CloseOnExec(nfd)
	if err := unix.SetNonblock(nfd, true); err != nil {
		_ = unix.Close(nfd)
		return -1, netip.AddrPort{}, err
	}
	return nfd, addrPort(sa), nil
}
