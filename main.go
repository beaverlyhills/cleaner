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
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"s.mcquay.me/sm/mov"

	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/tiff"

	"golang.org/x/image/bmp"
)

// FileMetadata contains cached file metadata
type FileMetadata struct {
	Path      string
	Size      int64
	FileHash  string
	ImageHash string
	Modified  time.Time
	DateShot  time.Time
}

func visitFunc(dbFile string, files map[string]*FileMetadata, hashes map[string][]*FileMetadata) filepath.WalkFunc {
	return func(path string, f os.FileInfo, err error) error {
		if f.IsDir() {
			return nil
		}
		record := files[path]
		if record != nil {
			if f.Size() == record.Size && f.ModTime() == record.Modified && len(record.FileHash) > 0 {
				return nil
			}
			fmt.Printf("Metadata changed for %s\n", path)
			removeRecord(files, hashes, record)
		}
		fileHash, err := getFileHash(path)
		if err != nil {
			return err
		}
		imageHash, err := getImageHash(path)
		if err != nil {
			fmt.Printf("Not an image %s\n", path)
		}
		dateShot, err := getMediaDate(path)
		record = &FileMetadata{Path: path, Modified: f.ModTime(), Size: f.Size(), FileHash: fileHash, ImageHash: imageHash, DateShot: dateShot}
		addRecord(files, hashes, record)
		addFileToDB(dbFile, record)
		fmt.Printf("Added %s\n", path)
		return nil
	}
}

func getMediaDate(path string) (time.Time, error) {
	dateShot, err := getImageDate(path)
	if err != nil {
		fmt.Printf("No exif %s\n", path)
		dateShot, err = getMovieDate(path)
		if err != nil {
			fmt.Printf("No moov %s\n", path)
		}
	}
	return dateShot, err
}

func addFileToDB(dbFile string, record *FileMetadata) error {
	file, err := os.OpenFile(dbFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
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
	fmt.Printf("Saved metadata for %s\n", record.Path)
	return nil
}

func getFileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fmt.Printf("Hashing file %s\n", path)
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
	fmt.Printf("Reading image %s\n", path)
	image, err := jpeg.Decode(f)
	if err != nil {
		return "", err
	}
	fmt.Printf("Hashing image %s\n", path)
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
	fmt.Printf("Reading exif %s\n", path)
	x, err := exif.Decode(f)
	if err != nil {
		return time.Time{}, err
	}
	datetime, err := getOriginalDateTime(x) // DateTime, DateTimeOriginal
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
	fmt.Printf("Reading moov %s\n", path)
	datetime, err := mov.Created(f)
	if err == nil {
		return datetime, nil
	}
	return time.Time{}, err
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func writeImage(writer io.Writer, image image.Image) error {
	return bmp.Encode(writer, image)
}

func readDB(path string) (map[string]*FileMetadata, map[string][]*FileMetadata, error) {
	fmt.Printf("Reading database from %s\n", path)
	files := make(map[string]*FileMetadata)
	hashes := make(map[string][]*FileMetadata)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return files, hashes, nil
		}
		return nil, nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record FileMetadata
		err := json.Unmarshal(scanner.Bytes(), &record)
		if err != nil {
			return nil, nil, err
		}
		if len(record.Path) > 0 {
			fmt.Printf("Restored metadata for %s\n", record.Path)
			addRecord(files, hashes, &record)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	return files, hashes, nil
}

func addRecord(files map[string]*FileMetadata, hashes map[string][]*FileMetadata, record *FileMetadata) {
	files[record.Path] = record
	hashes[record.FileHash] = append(hashes[record.FileHash], record)
	if len(record.ImageHash) > 0 {
		hashes[record.ImageHash] = append(hashes[record.ImageHash], record)
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

func main() {
	var dbFile string
	flag.StringVar(&dbFile, "db", "cache.txt", "Database file path, default value is cache.txt")
	flag.Parse()
	files, hashes, err := readDB(dbFile)
	if err != nil {
		log.Fatal(err)
	}
	for _, path := range flag.Args() {
		fmt.Printf("Scanning %s\n", path)
		err := filepath.Walk(path, visitFunc(dbFile, files, hashes))
		if err != nil {
			fmt.Printf("filepath.Walk() returned %v\n", err)
		}
		fmt.Printf("Finished scanning\n")
	}
}
