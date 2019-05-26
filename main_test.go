package main

import (
	"path/filepath"
	"testing"

	logging "github.com/op/go-logging"
)

func BenchmarkWalkFile(b *testing.B) {
	logging.SetLevel(logging.WARNING, "cleaner")
	for n := 0; n < b.N; n++ {
		fh := &FileHashes{dbPath: "", files: make(map[string]*FileMetadata), hashes: make(map[string][]*FileMetadata)}
		filepath.Walk("samples/sample.jpg", makeWalkFunc(fh))
	}
}
