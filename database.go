package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

// FileHashes holds database records
type FileHashes struct {
	dbPath string
	files  map[string]*FileMetadata
	hashes map[string][]*FileMetadata
}

func addFileToDB(fh *FileHashes, record *FileMetadata) error {
	if fh.dbPath == record.Path {
		return errors.New("Tried to write db data to destination file")
	}
	file, err := os.OpenFile(fh.dbPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
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

func writeAllRecordsToDB(fh *FileHashes) error {
	file, err := os.OpenFile(fh.dbPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
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

func compactDB(fh *FileHashes) error {
	backup := fh.dbPath + "." + strconv.FormatInt(time.Now().Unix(), 16)
	log.Infof("Compacting db file %s with backup in %s\n", fh.dbPath, backup)
	os.Rename(fh.dbPath, backup)
	if err := writeAllRecordsToDB(fh); err != nil {
		return err
	}
	return nil
}

func addRecord(fh *FileHashes, record *FileMetadata) {
	fh.files[record.Path] = record
	if record.Size > 0 {
		fh.hashes[record.FileHash] = append(fh.hashes[record.FileHash], record)
		if len(record.ImageHash) > 0 {
			fh.hashes[record.ImageHash] = append(fh.hashes[record.ImageHash], record)
		}
	}
}

func removeRecord(fh *FileHashes, record *FileMetadata) {
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

type addFn func(fh *FileHashes, record *FileMetadata) (bool, error)

type updatePathFn func(record *FileMetadata) (bool, error)

func defaultPath(record *FileMetadata) (bool, error) {
	newPath, err := filepath.Abs(record.Path)
	if err != nil {
		return false, err
	}
	updated := newPath != record.Path
	if updated {
		log.Debugf("Updating path %s to %s\n", record.Path, newPath)
		record.Path = newPath
	}
	return updated, nil
}

func defaultAdd(fh *FileHashes, record *FileMetadata) (bool, error) {
	needsCompacting := false
	if fh.files[record.Path] != nil {
		log.Debugf("Overwriting older record for %s\n", record.Path)
		removeRecord(fh, fh.files[record.Path])
		needsCompacting = true
	}
	log.Debugf("Restored metadata for %s\n", record.Path)
	addRecord(fh, record)
	return needsCompacting, nil
}

func readDB(dbPath string, compact bool, addRec addFn, updatePath updatePathFn) (*FileHashes, error) {
	dbPath, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, err
	}
	log.Infof("Reading database from %s\n", dbPath)
	fh := &FileHashes{dbPath: dbPath, files: make(map[string]*FileMetadata), hashes: make(map[string][]*FileMetadata)}
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
			updated, err := updatePath(record)
			if err != nil {
				return nil, err
			}
			refreshed, err := addRec(fh, record)
			if err != nil {
				return nil, err
			}
			needsCompacting = needsCompacting || refreshed || updated
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if compact && needsCompacting {
		if err := compactDB(fh); err != nil {
			return nil, err
		}
	}
	return fh, nil
}
