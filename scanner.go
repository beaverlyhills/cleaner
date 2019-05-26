package main

import (
	"os"
	"path/filepath"
)

func makeWalkFunc(fh *FileHashes) filepath.WalkFunc {
	return func(path string, f os.FileInfo, err error) error {
		if f == nil || f.IsDir() {
			return nil
		}
		record := fh.files[path]
		if record != nil {
			if checkFileDidNotChange(f, record) {
				return nil
			}
			log.Warningf("Metadata changed for %s\n", path)
			removeRecord(fh, record)
		}
		record, err = parseFileMetadata(path, f, record)
		if err != nil {
			return err
		}
		log.Debugf("Adding %s\n", record.Path)
		addRecord(fh, record)
		addFileToDB(fh, record)
		return nil
	}
}

func parseFileMetadata(path string, f os.FileInfo, existingRecord *FileMetadata) (*FileMetadata, error) {
	log.Infof("Processing %s\n", path)
	fileHash, err := getFileHash(path)
	if err != nil {
		return nil, err
	}
	imageHash, err := getImageHash(path)
	if err != nil {
		log.Debugf("Not an image %s\n", path)
	}
	dateShot, err := getMediaDate(path)
	if err != nil {
		log.Debugf("Not a supported media file %s\n", path)
	}
	creationTime := getCreationTime(f)
	if existingRecord != nil && (fileHash != existingRecord.FileHash || imageHash != existingRecord.ImageHash || dateShot != existingRecord.DateShot) {
		log.Warningf("Contents changed for %s\n", path)
	}
	return &FileMetadata{Path: path, Created: creationTime, Modified: f.ModTime(), Size: f.Size(), FileHash: fileHash, ImageHash: imageHash, DateShot: dateShot}, nil
}

// checkFileDidNotChange checks that file on record wasn't changed
func checkFileDidNotChange(f os.FileInfo, record *FileMetadata) bool {
	return !f.IsDir() && f.Size() == record.Size && getCreationTime(f) == record.Created && f.ModTime() == record.Modified && len(record.FileHash) > 0
}

// ReadDB reads cache database, checks and refreshes outdated file records
func ReadDB(dbPath string, compact bool) (*FileHashes, error) {
	return readDB(dbPath, compact, readDBRecord, updateToAbsolutePath)
}

// ScanFolders scans specified paths and adds them to database
func ScanFolders(folders []string, fh *FileHashes) error {
	log.Infof("Scanning paths\n")
	walkFunc := makeWalkFunc(fh)
	for _, path := range folders {
		path, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		log.Infof("Scanning %s\n", path)
		err = filepath.Walk(path, walkFunc)
		if err != nil {
			return err
		}
		log.Infof("Finished scanning %s\n", path)
	}
	log.Infof("Finished scanning all paths\n")
	return nil
}

func readDBRecord(fh *FileHashes, record *FileMetadata) (bool, error) {
	f, err := os.Stat(record.Path)
	if os.IsNotExist(err) {
		log.Warningf("File not found %s\n", record.Path)
		return true, nil
	} else if err != nil {
		return false, err
	}
	if fh.files[record.Path] != nil && checkFileDidNotChange(f, fh.files[record.Path]) {
		log.Debugf("Already have accurate record for %s\n", record.Path)
		return true, nil
	}
	if checkFileDidNotChange(f, record) {
		// Record is in sync with file, load it in memory as is
		log.Debugf("Restoring metadata for %s\n", record.Path)
		return replaceLatestRecord(fh, record)
	}
	log.Debugf("Refreshing changed file %s\n", record.Path)
	record, err = parseFileMetadata(record.Path, f, record)
	if err != nil {
		return false, err
	}
	log.Debugf("Adding refreshed %s\n", record.Path)
	replaceLatestRecord(fh, record)
	addFileToDB(fh, record)
	return true, nil
}
