package main

import (
	"path/filepath"
	"testing"
)

func BenchmarkWalkFile(b *testing.B) {
	for n := 0; n < b.N; n++ {
		filepath.Walk("samples/sample.jpg", visitFunc("", make(map[string]*FileMetadata), make(map[string][]*FileMetadata)))
	}
}
