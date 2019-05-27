package main

import (
	"os"
	"testing"

	logging "github.com/op/go-logging"
)

func BenchmarkParseFileMetadata(b *testing.B) {
	logging.SetLevel(logging.WARNING, "cleaner")
	path := "samples/sample.jpg"
	f, _ := os.Stat(path)
	for n := 0; n < b.N; n++ {
		parseFileMetadata(path, f, nil)
	}
}
