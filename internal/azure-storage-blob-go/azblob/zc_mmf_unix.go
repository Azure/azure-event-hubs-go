//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly
// +build linux darwin freebsd openbsd netbsd dragonfly

package azblob

import (
	"os"
	"syscall"
)

type mmf []byte

func newMMF(file *os.File, writable bool, offset int64, length int) (mmf, error) {
	prot, flags := syscall.PROT_READ, syscall.MAP_SHARED // Assume read-only
	if writable {
		prot, flags = syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED
	}
	addr, err := syscall.Mmap(int(file.Fd()), offset, length, prot, flags)
	return mmf(addr), err
}

func (m *mmf) unmap() {
	err := syscall.Munmap(*m)
	*m = nil
	if err != nil {
		panic("if we are unable to unmap the memory-mapped file, there is serious concern for memory corruption")
	}
}
