package main

func getCreationTime(f os.FileInfo) time.Time {
	var stat = fileinfo.Sys().(*syscall.Stat_t)
	time.Unix(stat.Btim.Sec, stat.Btim.Nsec)
}
