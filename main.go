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
	"strings"
	"time"

	"s.mcquay.me/sm/mov"

	logging "github.com/op/go-logging"
	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/tiff"

	"strconv"

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
		log.Infof("Adding %s\n", record.Path)
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
		log.Infof("Not an image %s\n", path)
	}
	dateShot, err := getMediaDate(path)
	if err != nil {
		log.Infof("Not a supported media file %s\n", path)
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
		log.Infof("No exif %s\n", path)
		dateShot, err = getMovieDate(path)
		if err != nil {
			log.Infof("No moov %s\n", path)
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
	log.Infof("Saved metadata for %s\n", record.Path)
	return nil
}

func getFileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	log.Infof("Hashing file %s\n", path)
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
	log.Infof("Reading image %s\n", path)
	image, err := jpeg.Decode(f)
	if err != nil {
		return "", err
	}
	log.Infof("Hashing image %s\n", path)
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
	log.Infof("Reading exif %s\n", path)
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
	log.Infof("Reading moov %s\n", path)
	datetime, err := mov.Created(f)
	if err == nil {
		return datetime, nil
	}
	return time.Time{}, err
}

func writeImage(writer io.Writer, image image.Image) error {
	return bmp.Encode(writer, image)
}

func readDB(dbPath string) (map[string]*FileMetadata, map[string][]*FileMetadata, bool, error) {
	log.Infof("Reading database from %s\n", dbPath)
	files := make(map[string]*FileMetadata)
	hashes := make(map[string][]*FileMetadata)
	file, err := os.Open(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return files, hashes, false, nil
		}
		return nil, nil, false, err
	}
	defer file.Close()
	needsCompacting := false
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		record := &FileMetadata{}
		err := json.Unmarshal(scanner.Bytes(), record)
		if err != nil {
			return nil, nil, false, err
		}
		if len(record.Path) > 0 {
			if f, err := os.Stat(record.Path); os.IsNotExist(err) {
				log.Infof("File not found %s\n", record.Path)
			} else if err != nil {
				return nil, nil, false, err
			} else {
				if record.Created.IsZero() {
					log.Infof("Backfilling creation time %s\n", record.Path)
					record.Created = getCreationTime(f)
				}
				if checkRecord(f, record) {
					if files[record.Path] != nil {
						log.Infof("Overwriting older record for %s\n", record.Path)
						removeRecord(files, hashes, files[record.Path])
						needsCompacting = true
					}
					log.Infof("Restored metadata for %s\n", record.Path)
					addRecord(files, hashes, record)
				} else {
					log.Infof("Refreshing changed file %s\n", record.Path)
					record, err = getFileRecord(record.Path, f, record)
					if err != nil {
						return nil, nil, false, err
					}
					log.Infof("Adding refreshed %s\n", record.Path)
					addRecord(files, hashes, record)
					addFileToDB(dbPath, record)
					needsCompacting = true
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, false, err
	}
	return files, hashes, needsCompacting, nil
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
func pickMaster(dups map[*FileMetadata]bool) *FileMetadata {
	var master *FileMetadata
	for d := range dups {
		if master == nil {
			master = d
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

func showDuplicates(folderToScanForDuplicates string, files map[string]*FileMetadata, hashes map[string][]*FileMetadata) {
	log.Infof("Looking for duplicates in %s\n", folderToScanForDuplicates)
	visited := make(map[string]*FileMetadata)
	prefix := fmt.Sprintf("%s%c", folderToScanForDuplicates, filepath.Separator)
	for p, r := range files {
		if visited[p] != nil {
			continue
		}
		if strings.HasPrefix(p, prefix) {
			dups := make(map[*FileMetadata]bool)
			allDupsInside := true
			strictMatches := hashes[r.FileHash]
			if len(strictMatches) > 1 {
				for _, d := range strictMatches {
					if strings.HasPrefix(p, prefix) {
						dups[d] = true
						visited[d.Path] = d
					} else {
						allDupsInside = false
					}
				}
			}
			if len(r.ImageHash) > 0 {
				bitmapMatches := hashes[r.ImageHash]
				if len(bitmapMatches) > 1 {
					for _, d := range bitmapMatches {
						if _, ok := dups[d]; ok {
							continue
						}
						if strings.HasPrefix(p, prefix) {
							dups[d] = false
							visited[d.Path] = d
						} else {
							allDupsInside = false
						}
					}
				}
			}
			if len(dups) > 0 {
				var master *FileMetadata
				if allDupsInside {
					master = pickMaster(dups)
					log.Infof("Picked master: %s (Shot: %s, Created: %s, Modified: %s)\n", master.Path, master.DateShot, master.Created, master.Modified)
				}
				for p, strict := range dups {
					if p == master {
						continue
					}
					matchType := "Strict Match"
					if !strict {
						matchType = "Bitmap Match"
					}
					log.Infof("Duplicate File: %s (%s, Shot: %s, Created: %s, Modified: %s)\n", p.Path, matchType, p.DateShot, p.Created, p.Modified)
					fmt.Printf("%s\n", p.Path)
				}
			}
		}
	}
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

func main() {
	var dbFile string
	var compactDB bool
	var folderToScanForDuplicates string
	flag.StringVar(&dbFile, "db", "cache.txt", "Database file path, default value is cache.txt")
	flag.BoolVar(&compactDB, "compact", false, "Compact database (remove deleted and changed records)")
	flag.StringVar(&folderToScanForDuplicates, "dup", "", "Show duplicates in specified folder from database")
	flag.Parse()
	files, hashes, needsCompacting, err := readDB(dbFile)
	if err != nil {
		log.Fatal(err)
	}
	if compactDB && needsCompacting {
		backup := dbFile + "." + strconv.FormatInt(time.Now().Unix(), 16)
		log.Infof("Compacting db file %s with backup in %s\n", dbFile, backup)
		os.Rename(dbFile, backup)
		if err := writeAllRecordsToDB(dbFile, files); err != nil {
			log.Fatal(err)
		}
	}
	if len(flag.Args()) > 0 {
		if err := scanFolders(dbFile, flag.Args(), files, hashes); err != nil {
			log.Fatal(err)
		}
	}
	if len(folderToScanForDuplicates) > 0 {
		showDuplicates(folderToScanForDuplicates, files, hashes)
	}
}
