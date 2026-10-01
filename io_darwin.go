//go:build darwin

package fnet

import "golang.org/x/sys/unix"

// darwin's system calls go through libc, so the unix wrappers are kept.

func sysRead(fd int, b []byte) (int, error)      { return unix.Read(fd, b) }
func sysWrite(fd int, b []byte) (int, error)     { return unix.Write(fd, b) }
func sysWritev(fd int, bs [][]byte) (int, error) { return unix.Writev(fd, bs) }
