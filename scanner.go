package main

import (
	"os"
	"path/filepath"
	"sync"
)

type scanInfo struct {
	path           string
	f              os.FileInfo
	existingRecord *FileMetadata
}

func makeParserWorker(wg *sync.WaitGroup, jobs <-chan *scanInfo, results chan<- *FileMetadata) {
	for j := range jobs {
		record, err := parseFileMetadata(j.path, j.f, j.existingRecord)
		if err == nil {
			results <- record
		} else {
			wg.Done()
		}
	}
}

func makeAdderWorker(results <-chan *FileMetadata, fh *FileHashes) {
	for record := range results {
		addParsedFileRecord(fh, record)
		fh.wg.Done()
	}
}

func addParsedFileRecord(fh *FileHashes, record *FileMetadata) {
	fh.lock.Lock()
	defer fh.lock.Unlock()
	log.Debugf("Adding %s\n", record.Path)
	addRecord(fh, record)
	addFileToDB(fh, record)
}

func makeWalkFunc(jobs chan<- *scanInfo, fh *FileHashes) filepath.WalkFunc {
	return func(path string, f os.FileInfo, err error) error {
		if f == nil || f.IsDir() {
			return nil
		}
		fh.lock.Lock()
		record := fh.files[path]
		if record != nil {
			if checkFileDidNotChange(f, record) {
				fh.lock.Unlock()
				return nil
			}
			log.Warningf("Metadata changed for %s\n", path)
			removeRecord(fh, record)
		}
		fh.lock.Unlock()
		jobs <- &scanInfo{path: path, f: f, existingRecord: record}
		fh.wg.Add(1)
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
func ScanFolders(folders []string, fh *FileHashes, concurrency int) error {
	log.Infof("Scanning paths\n")
	jobs := make(chan *scanInfo, concurrency*4)
	results := make(chan *FileMetadata, concurrency*4)
	for w := 0; w < concurrency; w++ {
		go makeParserWorker(&fh.wg, jobs, results)
	}
	go makeAdderWorker(results, fh)
	walkFunc := makeWalkFunc(jobs, fh)
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
	log.Debugf("Waiting for parsers to complete\n")
	close(jobs)
	fh.wg.Wait()
	close(results)
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
