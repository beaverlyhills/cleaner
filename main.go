package main

import (
	"flag"

	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("cleaner")

func main() {
	var dbFile string
	var compactDB bool
	var folderToScanForDuplicates string
	var folderToScanForMasters string
	var verbose bool
	var silent bool
	var moveDuplicatesTo string
	var searchForDuplicates bool
	flag.StringVar(&dbFile, "db", "cache.txt", "Database file path, default value is cache.txt")
	flag.BoolVar(&compactDB, "compact", false, "Compact database (remove deleted and changed records)")
	flag.StringVar(&folderToScanForDuplicates, "dup", "", "Show duplicates in specified folder from database, implies -dups")
	flag.StringVar(&folderToScanForMasters, "master", "", "Show duplicates with masters in specified folder from database, implies -dups")
	flag.StringVar(&moveDuplicatesTo, "move", "", "Move duplicates into specified folder preserving their relative paths, implies -dups")
	flag.BoolVar(&searchForDuplicates, "dups", false, "Scan for duplicates")
	flag.BoolVar(&silent, "silent", false, "Supress non-error logging")
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging")
	flag.Parse()
	if silent {
		logging.SetLevel(logging.WARNING, "cleaner")
	} else if verbose {
		logging.SetLevel(logging.DEBUG, "cleaner")
	} else {
		logging.SetLevel(logging.INFO, "cleaner")
	}
	fh, err := ReadDB(dbFile, compactDB)
	if err != nil {
		log.Fatal(err)
	}
	if len(flag.Args()) > 0 {
		if err := ScanFolders(flag.Args(), fh); err != nil {
			log.Fatal(err)
		}
	}
	if searchForDuplicates || len(folderToScanForDuplicates) > 0 || len(folderToScanForMasters) > 0 || len(moveDuplicatesTo) > 0 {
		dups, err := FindDuplicates(folderToScanForDuplicates, folderToScanForMasters, fh)
		if err != nil {
			log.Fatal(err)
		}
		if len(moveDuplicatesTo) > 0 && len(dups) > 0 {
			err := moveDuplicates(moveDuplicatesTo, dups)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
