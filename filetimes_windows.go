package main

import (
	"os"
	"syscall"
	"time"
)

func getCreationTime(f os.FileInfo) time.Time {
	stat := f.Sys().(*syscall.Win32FileAttributeData)
	seconds := stat.CreationTime.Nanoseconds() / 1000000000
	nanoseconds := stat.CreationTime.Nanoseconds() - seconds*1000000000
	return time.Unix(seconds, nanoseconds)
}
