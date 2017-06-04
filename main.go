package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	logging "github.com/op/go-logging"
)

var log = logging.MustGetLogger("cleaner")

func readDB(dbPath string, compact bool) (*fileHashes, error) {
	log.Infof("Reading database from %s\n", dbPath)
	fh := &fileHashes{files: make(map[string]*FileMetadata), hashes: make(map[string][]*FileMetadata)}
	file, err := os.Open(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fh, nil
		}
		return nil, err
	}
	defer file.Close()
	needsCompacting := false
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		record := &FileMetadata{}
		err := json.Unmarshal(scanner.Bytes(), record)
		if err != nil {
			return nil, err
		}
		if len(record.Path) > 0 {
			if f, err := os.Stat(record.Path); os.IsNotExist(err) {
				log.Warningf("File not found %s\n", record.Path)
			} else if err != nil {
				return nil, err
			} else {
				if record.Created.IsZero() {
					log.Debugf("Backfilling creation time %s\n", record.Path)
					record.Created = getCreationTime(f)
				}
				if checkRecord(f, record) {
					if fh.files[record.Path] != nil {
						log.Debugf("Overwriting older record for %s\n", record.Path)
						removeRecord(fh, fh.files[record.Path])
						needsCompacting = true
					}
					log.Debugf("Restored metadata for %s\n", record.Path)
					addRecord(fh, record)
				} else {
					log.Debugf("Refreshing changed file %s\n", record.Path)
					record, err = getFileRecord(record.Path, f, record)
					if err != nil {
						return nil, err
					}
					log.Debugf("Adding refreshed %s\n", record.Path)
					addRecord(fh, record)
					addFileToDB(dbPath, record)
					needsCompacting = true
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if compact && needsCompacting {
		if err := compactDB(dbPath, fh); err != nil {
			return nil, err
		}
	}
	return fh, nil
}

// Pick oldest files, unless it's an image with larger size
func pickMaster(dups map[*FileMetadata]bool, duplicatePrefix string, masterPrefix string) *FileMetadata {
	var master *FileMetadata
	for d := range dups {
		if master == nil {
			master = d
		} else if strings.HasPrefix(d.Path, masterPrefix) != strings.HasPrefix(master.Path, masterPrefix) {
			// Pick master inside masters folder
			if !strings.HasPrefix(master.Path, masterPrefix) {
				master = d
			}
		} else if strings.HasPrefix(d.Path, duplicatePrefix) != strings.HasPrefix(master.Path, duplicatePrefix) {
			// Pick master outside of searched folder
			if strings.HasPrefix(master.Path, duplicatePrefix) {
				master = d
			}
		} else if d.Size > master.Size {
			// Picking larger files since they most likely have more metadata with same image data
			master = d
		} else if !d.DateShot.IsZero() && (master.DateShot.IsZero() || d.DateShot.Unix() < master.DateShot.Unix()) {
			// Pick earliest shooting date
			master = d
		} else if d.Modified.Unix() < master.Modified.Unix() {
			// For copied files modification date would be more accurate than creation date
			master = d
		} else if d.Created.Unix() < master.Created.Unix() {
			master = d
		}
	}
	return master
}

func getDupsForFile(r *FileMetadata, visited map[string]*FileMetadata, prefix string, files []*FileMetadata, dups map[*FileMetadata]bool) {
	if len(prefix) > 0 {
		log.Debugf("Looking for duplicates of %s in %s\n", r.Path, prefix)
	} else {
		log.Debugf("Looking for duplicates of %s\n", r.Path)
	}
	found := false
	if len(files) > 1 {
		for _, d := range files {
			if !strings.HasPrefix(d.Path, prefix) {
				continue
			}
			if visited[d.Path] != nil {
				continue
			}
			if d == r {
				continue
			}
			if _, ok := dups[d]; ok {
				continue
			}
			if r.FileHash == d.FileHash {
				log.Debugf("Found exact duplicate %s\n", d.Path)
			} else {
				log.Debugf("Found image duplicate %s\n", d.Path)
			}
			dups[d] = true
			found = true
		}
	}
	if found {
		dups[r] = true
	}
}

func findDuplicates(folderToScanForDuplicates string, folderToScanForMasters string, fh *fileHashes) map[*FileMetadata][]*FileMetadata {
	result := make(map[*FileMetadata][]*FileMetadata)
	visited := make(map[string]*FileMetadata)
	duplicatePrefix := ""
	if len(folderToScanForDuplicates) > 0 {
		duplicatePrefix = fmt.Sprintf("%s%c", folderToScanForDuplicates, filepath.Separator)
	}
	masterPrefix := ""
	if len(folderToScanForMasters) > 0 {
		masterPrefix = fmt.Sprintf("%s%c", folderToScanForMasters, filepath.Separator)
	}
	if len(folderToScanForDuplicates) > 0 && len(folderToScanForMasters) > 0 {
		log.Infof("Searching for duplicates in %s with masters in %s\n", folderToScanForDuplicates, folderToScanForMasters)
	} else if len(folderToScanForDuplicates) > 0 {
		log.Infof("Searching for duplicates in %s\n", folderToScanForDuplicates)
	} else if len(folderToScanForMasters) > 0 {
		log.Infof("Searching for duplicates across all db with masters in %s\n", folderToScanForMasters)
	} else {
		log.Infof("Searching for duplicates across all db\n")
	}
	for p, r := range fh.files {
		if visited[p] != nil {
			continue
		}
		if len(masterPrefix) > 0 {
			if !strings.HasPrefix(p, masterPrefix) {
				continue
			}
		} else if len(duplicatePrefix) > 0 && !strings.HasPrefix(p, duplicatePrefix) {
			continue
		}
		dups := make(map[*FileMetadata]bool)
		if len(masterPrefix) > 0 {
			// With masters we only care about finding duplicates in duplicates directory
			getDupsForFile(r, visited, duplicatePrefix, fh.hashes[r.FileHash], dups)
			if len(r.ImageHash) > 0 {
				getDupsForFile(r, visited, duplicatePrefix, fh.hashes[r.ImageHash], dups)
			}
		} else {
			// Find masters anywhere given file in duplicates directory
			getDupsForFile(r, visited, "", fh.hashes[r.FileHash], dups)
			if len(r.ImageHash) > 0 {
				getDupsForFile(r, visited, "", fh.hashes[r.ImageHash], dups)
			}
		}
		if len(dups) > 0 {
			var master *FileMetadata
			master = pickMaster(dups, duplicatePrefix, masterPrefix)
			log.Debugf("Picked master: %s (Shot: %s, Created: %s, Modified: %s)\n", master.Path, master.DateShot, master.Created, master.Modified)
			fmt.Printf("* Duplicates for: %s\n", master.Path)
			resultDups := make([]*FileMetadata, 0)
			visited[master.Path] = master
			for p := range dups {
				if p == master {
					continue
				}
				strict := master.FileHash == p.FileHash
				matchType := "Strict Match"
				if !strict {
					matchType = "Image Match"
				}
				log.Debugf("Duplicate File: %s (%s, Shot: %s, Created: %s, Modified: %s)\n", p.Path, matchType, p.DateShot, p.Created, p.Modified)
				if len(masterPrefix) > 0 && strings.HasPrefix(p.Path, masterPrefix) {
					fmt.Printf("!   Duplicate is in master directory: %s\n", p.Path)
				} else if len(duplicatePrefix) > 0 && !strings.HasPrefix(p.Path, duplicatePrefix) {
					fmt.Printf("!   Duplicate outside duplicates directory: %s\n", p.Path)
				} else if !strict {
					fmt.Printf("?   Image duplicate: %s\n", p.Path)
				} else {
					fmt.Printf("    %s\n", p.Path)
					resultDups = append(resultDups, p)
				}
			}
			if len(resultDups) > 0 {
				result[master] = resultDups
			}
		}
	}
	log.Infof("Done looking for duplicates in %s\n", folderToScanForDuplicates)
	return result
}

func scanFolders(dbPath string, folders []string, fh *fileHashes) error {
	log.Infof("Scanning paths\n")
	for _, path := range folders {
		log.Infof("Scanning %s\n", path)
		err := filepath.Walk(path, visitFunc(dbPath, fh))
		if err != nil {
			return err
		}
		log.Infof("Finished scanning %s\n", path)
	}
	log.Infof("Finished scanning all paths\n")
	return nil
}

func moveDuplicates(moveDuplicatesTo string, dups map[*FileMetadata][]*FileMetadata) error {
	for _, list := range dups {
		for _, p := range list {
			var relPath string
			var err error
			relPath, err = filepath.Rel(fmt.Sprintf("%s%c", filepath.VolumeName(p.Path), filepath.Separator), p.Path)
			if err != nil {
				return err
			}
			relDir := filepath.Dir(relPath)
			newDir := moveDuplicatesTo
			if relDir != "." {
				newDir = fmt.Sprintf("%s%c%s", filepath.Clean(moveDuplicatesTo), filepath.Separator, relDir)
			}
			log.Debugf("Destination folder: %s\n", newDir)
			newPath := fmt.Sprintf("%s%c%s", filepath.Clean(moveDuplicatesTo), filepath.Separator, relPath)
			log.Debugf("Destination path: %s\n", newPath)
			fmt.Printf("Moving %s to %s\n", p.Path, newPath)
			// os.MkdirAll(newPath, 0777)
			// os.Rename(p.Path, newPath)
		}
	}
	return nil
}

func main() {
	var dbFile string
	var compactDB bool
	var folderToScanForDuplicates string
	var folderToScanForMasters string
	var verbose bool
	var silent bool
	var moveDuplicatesTo string
	flag.StringVar(&dbFile, "db", "cache.txt", "Database file path, default value is cache.txt")
	flag.BoolVar(&compactDB, "compact", false, "Compact database (remove deleted and changed records)")
	flag.StringVar(&folderToScanForDuplicates, "dup", "", "Show duplicates in specified folder from database")
	flag.StringVar(&folderToScanForMasters, "master", "", "Show duplicates with masters in specified folder from database")
	flag.StringVar(&moveDuplicatesTo, "move", "", "Move duplicates into specified folder preserving their relative paths")
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
	fh, err := readDB(dbFile, compactDB)
	if err != nil {
		log.Fatal(err)
	}
	if len(flag.Args()) > 0 {
		if err := scanFolders(dbFile, flag.Args(), fh); err != nil {
			log.Fatal(err)
		}
	}
	if len(folderToScanForDuplicates) > 0 || len(folderToScanForMasters) > 0 || len(moveDuplicatesTo) > 0 {
		dups := findDuplicates(folderToScanForDuplicates, folderToScanForMasters, fh)
		if len(moveDuplicatesTo) > 0 && len(dups) > 0 {
			err := moveDuplicates(moveDuplicatesTo, dups)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
