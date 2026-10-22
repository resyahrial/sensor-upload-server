//go:build unix

package cleanup

import "syscall"

// Statfs reports the usage of the filesystem holding path, computed as
// df does: used = blocks - free, total = used + available to
// unprivileged users.
func Statfs(path string) (DiskUsage, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return DiskUsage{}, err
	}
	bs := uint64(st.Bsize) // int64 on Linux, uint32 on Darwin
	used := (st.Blocks - st.Bfree) * bs
	return DiskUsage{Total: used + st.Bavail*bs, Used: used}, nil
}
