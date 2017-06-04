package main

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"time"
)

// FileMetadata contains cached file metadata
type FileMetadata struct {
	Path      string
	Size      int64
	FileHash  string
	ImageHash string
	Created   time.Time
	Modified  time.Time
	DateShot  time.Time
}

type fileHashes struct {
	files  map[string]*FileMetadata
	hashes map[string][]*FileMetadata
}

func addFileToDB(dbFile string, record *FileMetadata) error {
	if dbFile == record.Path {
		return errors.New("Tried to write db data to destination file")
	}
	file, err := os.OpenFile(dbFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	return writeRecordToFile(file, record)
}

func writeRecordToFile(file *os.File, record *FileMetadata) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		return err
	}
	if _, err = file.WriteString("\n"); err != nil {
		return err
	}
	log.Debugf("Saved metadata for %s\n", record.Path)
	return nil
}

func writeAllRecordsToDB(dbFile string, fh *fileHashes) error {
	file, err := os.OpenFile(dbFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, v := range fh.files {
		if err := writeRecordToFile(file, v); err != nil {
			return err
		}
	}
	return nil
}

func compactDB(dbPath string, fh *fileHashes) error {
	backup := dbPath + "." + strconv.FormatInt(time.Now().Unix(), 16)
	log.Infof("Compacting db file %s with backup in %s\n", dbPath, backup)
	os.Rename(dbPath, backup)
	if err := writeAllRecordsToDB(dbPath, fh); err != nil {
		return err
	}
	return nil
}

func addRecord(fh *fileHashes, record *FileMetadata) {
	fh.files[record.Path] = record
	if record.Size > 0 {
		fh.hashes[record.FileHash] = append(fh.hashes[record.FileHash], record)
		if len(record.ImageHash) > 0 {
			fh.hashes[record.ImageHash] = append(fh.hashes[record.ImageHash], record)
		}
	}
}

func removeRecord(fh *fileHashes, record *FileMetadata) {
	delete(fh.files, record.Path)
	fh.hashes[record.FileHash] = deleteRecord(fh.hashes[record.FileHash], record)
	if len(record.ImageHash) > 0 {
		fh.hashes[record.ImageHash] = deleteRecord(fh.hashes[record.ImageHash], record)
	}
}

func deleteRecord(records []*FileMetadata, record *FileMetadata) []*FileMetadata {
	for i, r := range records {
		if r.Path == record.Path {
			return append(records[:i], records[i+1:]...)
		}
	}
	return records
}
