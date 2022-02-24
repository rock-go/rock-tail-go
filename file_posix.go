//go:build linux || darwin || freebsd || netbsd || openbsd
// +build linux darwin freebsd netbsd openbsd

package tail

import "os"

func openFile(filename string) (*os.File, error) {
	return os.Open(filename)
}
