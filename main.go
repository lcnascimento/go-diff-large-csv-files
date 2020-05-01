package main

import (
	"context"
	"flag"
	"log"

	_ "github.com/mattn/go-sqlite3"

	"github.com/lcnascimento/go-diff-large-csv-files/diff"
)

func main() {
	oldPath := flag.String("old", "", "old file path")
	newPath := flag.String("new", "", "new file path")
	key := flag.String("key", "", "key column to use")

	flag.Parse()

	if oldPath == nil || *oldPath == "" {
		log.Fatal("missing '-old' parameter")
	}

	if newPath == nil || *newPath == "" {
		log.Fatal("missing '-new' parameter")
	}

	if key == nil || *key == "" {
		log.Fatal("missing '-key' parameter")
	}

	diff := diff.NewDiff(*oldPath, *newPath, *key)
	diff.Do(context.Background())
}
