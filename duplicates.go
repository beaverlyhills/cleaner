package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Pick oldest files, unless it's an image with larger size
func pickMaster(candidates map[*FileMetadata]bool, duplicatePrefix string, masterPrefix string) *FileMetadata {
	var selected *FileMetadata
	for candidate := range candidates {
		log.Debugf("Master candidate %s\n", candidate.Path)
		if selected == nil {
			selected = candidate
		} else if strings.HasPrefix(candidate.Path, masterPrefix) != strings.HasPrefix(selected.Path, masterPrefix) {
			// Pick master inside masters folder
			if !strings.HasPrefix(selected.Path, masterPrefix) {
				selected = candidate
			}
		} else if strings.HasPrefix(candidate.Path, duplicatePrefix) != strings.HasPrefix(selected.Path, duplicatePrefix) {
			// Pick master outside of searched folder
			if strings.HasPrefix(selected.Path, duplicatePrefix) {
				selected = candidate
			}
		} else if candidate.Size != selected.Size {
			// Picking larger files since they most likely have more metadata with same image data
			if candidate.Size > selected.Size {
				selected = candidate
			}
		} else if !candidate.DateShot.IsZero() && (selected.DateShot.IsZero() || candidate.DateShot.Unix() != selected.DateShot.Unix()) {
			// Pick earliest shooting date
			if selected.DateShot.IsZero() || candidate.DateShot.Unix() < selected.DateShot.Unix() {
				selected = candidate
			}
		} else if candidate.Modified.Unix() != selected.Modified.Unix() {
			// For copied files modification date would be more accurate than creation date
			// Pick file that was modified earlier
			if candidate.Modified.Unix() < selected.Modified.Unix() {
				selected = candidate
			}
		} else if candidate.Created.Unix() != selected.Created.Unix() {
			// Pick file that is older
			if candidate.Created.Unix() < selected.Created.Unix() {
				selected = candidate
			}
		}
	}
	return selected
}

func getDupsForFile(record *FileMetadata, visited map[string]*FileMetadata, prefix string, filesWithSameHash []*FileMetadata, foundDups map[*FileMetadata]bool) {
	found := false
	if len(filesWithSameHash) > 1 {
		for _, dupPath := range filesWithSameHash {
			if !strings.HasPrefix(dupPath.Path, prefix) {
				continue
			}
			if visited[dupPath.Path] != nil {
				continue
			}
			if dupPath == record {
				continue
			}
			if _, ok := foundDups[dupPath]; ok {
				continue
			}
			if record.FileHash == dupPath.FileHash {
				log.Debugf("Found exact duplicate %s\n", dupPath.Path)
			} else {
				log.Debugf("Found image duplicate %s\n", dupPath.Path)
			}
			foundDups[dupPath] = true
			found = true
		}
	}
	if found {
		foundDups[record] = true
	}
}

// FindDuplicates tries to find duplicate files in database
// If folderToScanForMasters is specified, only duplicates of files present in that folder will be returned
// If folderToScanForDuplicates is specified, only duplicate files from that directory will be returned
func FindDuplicates(folderToScanForDuplicates string, folderToScanForMasters string, fh *FileHashes) (map[*FileMetadata][]*FileMetadata, error) {
	result := make(map[*FileMetadata][]*FileMetadata)
	visited := make(map[string]*FileMetadata)
	duplicatePrefix := ""
	if len(folderToScanForDuplicates) > 0 {
		folderToScanForDuplicates, err := filepath.Abs(folderToScanForDuplicates)
		if err != nil {
			return nil, err
		}
		duplicatePrefix = fmt.Sprintf("%s%c", folderToScanForDuplicates, filepath.Separator)
	}
	masterPrefix := ""
	if len(folderToScanForMasters) > 0 {
		folderToScanForMasters, err := filepath.Abs(folderToScanForMasters)
		if err != nil {
			return nil, err
		}
		masterPrefix = fmt.Sprintf("%s%c", folderToScanForMasters, filepath.Separator)
	}
	if len(duplicatePrefix) > 0 && len(masterPrefix) > 0 {
		log.Infof("Searching for duplicates in %s with masters in %s\n", duplicatePrefix, masterPrefix)
	} else if len(duplicatePrefix) > 0 {
		log.Infof("Searching for duplicates in %s\n", duplicatePrefix)
	} else if len(masterPrefix) > 0 {
		log.Infof("Searching for duplicates across all db with masters in %s\n", masterPrefix)
	} else {
		log.Infof("Searching for duplicates across all db\n")
	}
	for path, record := range fh.files {
		if visited[path] != nil {
			continue
		}
		if len(masterPrefix) > 0 {
			if !strings.HasPrefix(path, masterPrefix) {
				continue
			}
		} else if len(duplicatePrefix) > 0 && !strings.HasPrefix(path, duplicatePrefix) {
			continue
		}
		prefix := ""
		dups := make(map[*FileMetadata]bool)
		if len(masterPrefix) > 0 {
			// With masters we only care about finding duplicates in duplicates directory
			prefix = duplicatePrefix
		} else {
			// Find masters anywhere given file in duplicates directory
		}
		if len(prefix) > 0 {
			log.Debugf("Looking for duplicates of %s in %s\n", record.Path, prefix)
		} else {
			log.Debugf("Looking for duplicates of %s\n", record.Path)
		}
		getDupsForFile(record, visited, prefix, fh.hashes[record.FileHash], dups)
		if len(record.ImageHash) > 0 {
			getDupsForFile(record, visited, prefix, fh.hashes[record.ImageHash], dups)
		}
		if len(dups) > 0 {
			var master *FileMetadata
			// Add record itself to dup candidates
			dups[record] = true
			master = pickMaster(dups, duplicatePrefix, masterPrefix)
			log.Debugf("Picked master: %s (Shot: %s, Created: %s, Modified: %s)\n", master.Path, master.DateShot, master.Created, master.Modified)
			fmt.Printf("* Duplicates for: %s\n", master.Path)
			resultDups := make([]*FileMetadata, 0)
			visited[master.Path] = master
			for dup := range dups {
				if dup == master {
					continue
				}
				isStrictMatch := master.FileHash == dup.FileHash
				matchType := "Strict Match"
				if !isStrictMatch {
					matchType = "Image Match"
				}
				log.Debugf("Duplicate File: %s (%s, Shot: %s, Created: %s, Modified: %s)\n", dup.Path, matchType, dup.DateShot, dup.Created, dup.Modified)
				if len(masterPrefix) > 0 && strings.HasPrefix(dup.Path, masterPrefix) && masterPrefix != duplicatePrefix {
					fmt.Printf("!   Duplicate is in master directory: %s\n", dup.Path)
				} else if len(duplicatePrefix) > 0 && !strings.HasPrefix(dup.Path, duplicatePrefix) {
					fmt.Printf("!   Duplicate outside duplicates directory: %s\n", dup.Path)
				} else if len(masterPrefix) > 0 && !strings.HasPrefix(master.Path, masterPrefix) {
					fmt.Printf("!   Master is outside of master directory: %s\n", dup.Path)
				} else {
					if !isStrictMatch {
						fmt.Printf("?   Image duplicate: %s\n", dup.Path)
					} else {
						fmt.Printf("    %s\n", dup.Path)
						visited[dup.Path] = dup
					}
					resultDups = append(resultDups, dup)
				}
			}
			if len(resultDups) > 0 {
				result[master] = resultDups
			}
		}
	}
	log.Infof("Done looking for duplicates\n")
	return result, nil
}

// MoveDuplicates moves found duplicates to destination folder with preserving relative path
func MoveDuplicates(moveDuplicatesTo string, removePrefix string, dups map[*FileMetadata][]*FileMetadata, fh *FileHashes, applyMove bool) (bool, error) {
	moveDuplicatesTo, err := filepath.Abs(moveDuplicatesTo)
	if err != nil {
		return false, err
	}
	moved := false
	for _, list := range dups {
		for _, p := range list {
			var relPath string
			var err error
			if len(removePrefix) > 0 {
				removePrefix, err := filepath.Abs(removePrefix)
				if err != nil {
					return moved, err
				}
				relPath, err = filepath.Rel(removePrefix, p.Path)
			} else {
				relPath, err = filepath.Rel(fmt.Sprintf("%s%c", filepath.VolumeName(p.Path), filepath.Separator), p.Path)
			}
			if err != nil {
				return moved, err
			}
			relDir := filepath.Dir(relPath)
			newDir := moveDuplicatesTo
			if relDir != "." {
				newDir = fmt.Sprintf("%s%c%s", filepath.Clean(moveDuplicatesTo), filepath.Separator, relDir)
			}
			log.Debugf("Destination folder: %s\n", newDir)
			newPath := fmt.Sprintf("%s%c%s", filepath.Clean(moveDuplicatesTo), filepath.Separator, relPath)
			log.Debugf("Destination path: %s\n", newPath)
			fmt.Printf("%011d Moving %s to %s\n", p.Size, p.Path, newPath)
			if _, err := os.Stat(p.Path); os.IsNotExist(err) {
				// Most likely we already moved this duplicate
				log.Warningf("File does not exist %s\n", p.Path)
				continue
			} else if err != nil {
				return moved, err
			}
			if _, err := os.Stat(newPath); err == nil || !os.IsNotExist(err) {
				return moved, errors.New("Destination file already exists")
			}
			if !applyMove {
				continue
			}
			err = os.MkdirAll(newDir, 0777)
			if err != nil && !os.IsExist(err) {
				return moved, err
			}
			err = os.Rename(p.Path, newPath)
			if err != nil {
				return moved, err
			}
			removeRecord(fh, p)
			moved = true
		}
	}
	return moved, nil
}
