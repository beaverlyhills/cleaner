package main

import (
	"path/filepath"
	"testing"

	logging "github.com/op/go-logging"
)

func BenchmarkWalkFile(b *testing.B) {
	logging.SetLevel(logging.WARNING, "cleaner")
	for n := 0; n < b.N; n++ {
		filepath.Walk("samples/sample.jpg", visitFunc("", make(map[string]*FileMetadata), make(map[string][]*FileMetadata)))
	}
}
