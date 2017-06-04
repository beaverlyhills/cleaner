package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"s.mcquay.me/sm/mov"

	logging "github.com/op/go-logging"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/tiff"

	"golang.org/x/image/bmp"
)

var log = logging.MustGetLogger("cleaner")

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

func visitFunc(dbFile string, files map[string]*FileMetadata, hashes map[string][]*FileMetadata) filepath.WalkFunc {
	return func(path string, f os.FileInfo, err error) error {
		if f == nil || f.IsDir() {
			return nil
		}
		record := files[path]
		if record != nil {
			if checkRecord(f, record) {
				return nil
			}
			log.Warningf("Metadata changed for %s\n", path)
			removeRecord(files, hashes, record)
		}
		record, err = getFileRecord(path, f, record)
		if err != nil {
			return err
		}
		log.Debugf("Adding %s\n", record.Path)
		addRecord(files, hashes, record)
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

func getMediaDate(path string) (time.Time, error) {
	dateShot, err := getImageDate(path)
	if err != nil && strings.HasSuffix(strings.ToLower(path), ".mov") {
		log.Debugf("No exif %s\n", path)
		dateShot, err = getMovieDate(path)
		if err != nil {
			log.Debugf("No moov %s\n", path)
		}
	}
	return dateShot, err
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

func writeAllRecordsToDB(dbFile string, files map[string]*FileMetadata) error {
	file, err := os.OpenFile(dbFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, v := range files {
		if err := writeRecordToFile(file, v); err != nil {
			return err
		}
	}
	return nil
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

func getFileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	log.Debugf("Hashing file %s\n", path)
	hasher := sha1.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func getImageHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	log.Debugf("Reading image %s\n", path)
	image, err := jpeg.Decode(f)
	if err != nil {
		return "", err
	}
	log.Debugf("Hashing image %s\n", path)
	hasher := sha1.New()
	if err := writeImage(hasher, image); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func getImageDate(path string) (time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()
	log.Debugf("Reading exif %s\n", path)
	x, err := exif.Decode(f)
	if err != nil {
		return time.Time{}, err
	}
	datetime, err := getOriginalDateTime(x)
	if err == nil {
		return datetime, nil
	}
	return time.Time{}, err
}

func getOriginalDateTime(x *exif.Exif) (time.Time, error) {
	dt, err := getDateTimeFromTag(exif.DateTimeOriginal, x)
	if err != nil {
		dt, err = getDateTimeFromTag(exif.DateTimeDigitized, x)
		if err != nil {
			dt, err = getDateTimeFromTag(exif.DateTime, x)
		}
	}
	return dt, err
}

func getDateTimeFromTag(name exif.FieldName, x *exif.Exif) (time.Time, error) {
	tag, err := x.Get(name)
	if err != nil {
		return time.Time{}, err
	}
	if tag.Format() != tiff.StringVal {
		return time.Time{}, errors.New("DateTime[Original] not in string format")
	}
	exifTimeLayout := "2006:01:02 15:04:05"
	dateStr := strings.TrimRight(string(tag.Val), "\x00")
	// TODO(bradfitz,mpl): look for timezone offset, GPS time, etc.
	// For now, just always return the time.Local timezone.
	return time.ParseInLocation(exifTimeLayout, dateStr, time.Local)
}

func getMovieDate(path string) (time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()
	log.Debugf("Reading moov %s\n", path)
	datetime, err := mov.Created(f)
	if err == nil {
		return datetime, nil
	}
	return time.Time{}, err
}

func writeImage(writer io.Writer, image image.Image) error {
	return bmp.Encode(writer, image)
}

func compactDB(dbPath string, files map[string]*FileMetadata) error {
	backup := dbPath + "." + strconv.FormatInt(time.Now().Unix(), 16)
	log.Infof("Compacting db file %s with backup in %s\n", dbPath, backup)
	os.Rename(dbPath, backup)
	if err := writeAllRecordsToDB(dbPath, files); err != nil {
		return err
	}
	return nil
}

func readDB(dbPath string, compact bool) (map[string]*FileMetadata, map[string][]*FileMetadata, error) {
	log.Infof("Reading database from %s\n", dbPath)
	files := make(map[string]*FileMetadata)
	hashes := make(map[string][]*FileMetadata)
	file, err := os.Open(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return files, hashes, nil
		}
		return nil, nil, err
	}
	defer file.Close()
	needsCompacting := false
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		record := &FileMetadata{}
		err := json.Unmarshal(scanner.Bytes(), record)
		if err != nil {
			return nil, nil, err
		}
		if len(record.Path) > 0 {
			if f, err := os.Stat(record.Path); os.IsNotExist(err) {
				log.Warningf("File not found %s\n", record.Path)
			} else if err != nil {
				return nil, nil, err
			} else {
				if record.Created.IsZero() {
					log.Debugf("Backfilling creation time %s\n", record.Path)
					record.Created = getCreationTime(f)
				}
				if checkRecord(f, record) {
					if files[record.Path] != nil {
						log.Debugf("Overwriting older record for %s\n", record.Path)
						removeRecord(files, hashes, files[record.Path])
						needsCompacting = true
					}
					log.Debugf("Restored metadata for %s\n", record.Path)
					addRecord(files, hashes, record)
				} else {
					log.Debugf("Refreshing changed file %s\n", record.Path)
					record, err = getFileRecord(record.Path, f, record)
					if err != nil {
						return nil, nil, err
					}
					log.Debugf("Adding refreshed %s\n", record.Path)
					addRecord(files, hashes, record)
					addFileToDB(dbPath, record)
					needsCompacting = true
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	if compact && needsCompacting {
		if err := compactDB(dbPath, files); err != nil {
			return nil, nil, err
		}
	}
	return files, hashes, nil
}

func checkRecord(f os.FileInfo, record *FileMetadata) bool {
	return !f.IsDir() && f.Size() == record.Size && getCreationTime(f) == record.Created && f.ModTime() == record.Modified && len(record.FileHash) > 0
}

func addRecord(files map[string]*FileMetadata, hashes map[string][]*FileMetadata, record *FileMetadata) {
	files[record.Path] = record
	if record.Size > 0 {
		hashes[record.FileHash] = append(hashes[record.FileHash], record)
		if len(record.ImageHash) > 0 {
			hashes[record.ImageHash] = append(hashes[record.ImageHash], record)
		}
	}
}

func removeRecord(files map[string]*FileMetadata, hashes map[string][]*FileMetadata, record *FileMetadata) {
	delete(files, record.Path)
	hashes[record.FileHash] = deleteRecord(hashes[record.FileHash], record)
	if len(record.ImageHash) > 0 {
		hashes[record.ImageHash] = deleteRecord(hashes[record.ImageHash], record)
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

func getDupsForFile(r *FileMetadata, hashes map[string][]*FileMetadata, visited map[string]*FileMetadata, prefix string) map[*FileMetadata]bool {
	if len(prefix) > 0 {
		log.Debugf("Looking for duplicates of %s in %s\n", r.Path, prefix)
	} else {
		log.Debugf("Looking for duplicates of %s\n", r.Path)
	}
	dups := make(map[*FileMetadata]bool)
	dups[r] = true
	visited[r.Path] = r
	strictMatches := hashes[r.FileHash]
	if len(strictMatches) > 1 {
		for _, d := range strictMatches {
			if !strings.HasPrefix(d.Path, prefix) {
				continue
			}
			if _, ok := dups[d]; ok {
				continue
			}
			log.Debugf("Found exact duplicate %s\n", d.Path)
			dups[d] = true
			visited[d.Path] = d
		}
	}
	if len(r.ImageHash) > 0 {
		bitmapMatches := hashes[r.ImageHash]
		if len(bitmapMatches) > 1 {
			for _, d := range bitmapMatches {
				if !strings.HasPrefix(d.Path, prefix) {
					continue
				}
				if _, ok := dups[d]; ok {
					continue
				}
				log.Debugf("Found image duplicate %s\n", d.Path)
				dups[d] = true
				visited[d.Path] = d
			}
		}
	}
	if len(dups) < 2 {
		return nil
	}
	return dups
}

func findDuplicates(folderToScanForDuplicates string, folderToScanForMasters string, files map[string]*FileMetadata, hashes map[string][]*FileMetadata) map[*FileMetadata][]*FileMetadata {
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
	for p, r := range files {
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
		var dups map[*FileMetadata]bool
		if len(masterPrefix) > 0 {
			// With masters we only care about finding duplicates in duplicates directory
			dups = getDupsForFile(r, hashes, visited, duplicatePrefix)
		} else {
			// Find masters anywhere given file in duplicates directory
			dups = getDupsForFile(r, hashes, visited, "")
		}
		if len(dups) > 0 {
			var master *FileMetadata
			master = pickMaster(dups, duplicatePrefix, masterPrefix)
			log.Debugf("Picked master: %s (Shot: %s, Created: %s, Modified: %s)\n", master.Path, master.DateShot, master.Created, master.Modified)
			fmt.Printf("* Duplicates for: %s\n", master.Path)
			resultDups := make([]*FileMetadata, 0)
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

func scanFolders(dbFile string, folders []string, files map[string]*FileMetadata, hashes map[string][]*FileMetadata) error {
	log.Infof("Scanning paths\n")
	for _, path := range folders {
		log.Infof("Scanning %s\n", path)
		err := filepath.Walk(path, visitFunc(dbFile, files, hashes))
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
	files, hashes, err := readDB(dbFile, compactDB)
	if err != nil {
		log.Fatal(err)
	}
	if len(flag.Args()) > 0 {
		if err := scanFolders(dbFile, flag.Args(), files, hashes); err != nil {
			log.Fatal(err)
		}
	}
	if len(folderToScanForDuplicates) > 0 || len(folderToScanForMasters) > 0 || len(moveDuplicatesTo) > 0 {
		dups := findDuplicates(folderToScanForDuplicates, folderToScanForMasters, files, hashes)
		if len(moveDuplicatesTo) > 0 && len(dups) > 0 {
			err := moveDuplicates(moveDuplicatesTo, dups)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
