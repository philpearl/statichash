// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package statichash

import "syscall"

func mapMemory(fd, size uintptr) (uintptr, error) {
	data, _, errno := syscall.Syscall6(
		syscall.SYS_MMAP,
		0, // address
		size,
		syscall.PROT_READ,
		syscall.MAP_FILE|syscall.MAP_PRIVATE,
		uintptr(fd), // No file descriptor
		0,           // offset
	)
	if errno != 0 {
		// zero errno is not nil!
		return 0, errno
	}

	_, _, errno = syscall.Syscall(syscall.SYS_MLOCK, data, size, 0)
	if errno != 0 {
		// zero errno is not nil!
		return 0, errno
	}

	return data, nil
}

func unmap(data, length uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, data, length, 0)
	if errno != 0 {
		// zero errno is not nil!
		return errno
	}
	return nil
}
