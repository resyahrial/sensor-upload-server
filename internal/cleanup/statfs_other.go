//go:build !unix

package cleanup

import "errors"

// Statfs is not implemented on this platform.
func Statfs(string) (DiskUsage, error) {
	return DiskUsage{}, errors.New("statfs is not supported on this platform")
}
