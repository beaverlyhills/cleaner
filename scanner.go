package main

import (
	"os"
	"path/filepath"
)

func visitFunc(dbFile string, fh *fileHashes) filepath.WalkFunc {
	return func(path string, f os.FileInfo, err error) error {
		if f == nil || f.IsDir() {
			return nil
		}
		record := fh.files[path]
		if record != nil {
			if checkRecord(f, record) {
				return nil
			}
			log.Warningf("Metadata changed for %s\n", path)
			removeRecord(fh, record)
		}
		record, err = getFileRecord(path, f, record)
		if err != nil {
			return err
		}
		log.Debugf("Adding %s\n", record.Path)
		addRecord(fh, record)
		if len(dbFile) > 0 {
			addFileToDB(dbFile, record)
		}
		return nil
	}
}

func getFileRecord(path string, f os.FileInfo, record *FileMetadata) (*FileMetadata, error) {
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
	if record != nil && (fileHash != record.FileHash || imageHash != record.ImageHash || dateShot != record.DateShot) {
		log.Warningf("Contents changed for %s\n", path)
	}
	return &FileMetadata{Path: path, Created: creationTime, Modified: f.ModTime(), Size: f.Size(), FileHash: fileHash, ImageHash: imageHash, DateShot: dateShot}, nil
}

func checkRecord(f os.FileInfo, record *FileMetadata) bool {
	return !f.IsDir() && f.Size() == record.Size && getCreationTime(f) == record.Created && f.ModTime() == record.Modified && len(record.FileHash) > 0
}
