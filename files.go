package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"image"
	"image/jpeg"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rwcarlsen/goexif/exif"
	"github.com/rwcarlsen/goexif/tiff"
	"golang.org/x/image/bmp"
	"s.mcquay.me/sm/mov"
)

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

func writeImage(writer io.Writer, image image.Image) error {
	return bmp.Encode(writer, image)
}
