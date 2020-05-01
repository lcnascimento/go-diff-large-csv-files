package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"golang.org/x/sync/errgroup"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	ctx := context.Background()

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

	db, err := sql.Open("sqlite3", "./poc.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	eg, _ := errgroup.WithContext(ctx)

	var oldTblName, newTblName string
	eg.Go(func() (err error) {
		oldTblName, err = createTable(db, *oldPath, *key)
		return err
	})
	eg.Go(func() (err error) {
		newTblName, err = createTable(db, *newPath, *key)
		return err
	})

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	inCh, outCh, err := diff(oldTblName, newTblName)
	if err != nil {
		log.Fatal(err)
	}

	eg, _ = errgroup.WithContext(ctx)

	eg.Go(func() error { return proccessDiff("+", inCh) })
	eg.Go(func() error { return proccessDiff("-", outCh) })

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	if err := os.Remove("./poc.db"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Done")
}

func createTable(db *sql.DB, filepath, key string) (string, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return "", err
	}

	tblName := strings.Replace(stat.Name(), ".csv", "", 1)
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id text not null primary key, row text);", tblName)

	_, err = db.Exec(sql)
	if err != nil {
		return "", err
	}

	tx, err := db.Begin()
	if err != nil {
		return "", err
	}

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s(id, row) VALUES(?, ?)", tblName))
	if err != nil {
		return "", err
	}
	defer stmt.Close()

	reader := csv.NewReader(f)

	header, err := reader.Read()
	if err != nil {
		return "", err
	}

	keyIdx, err := keyIndex(key, header)
	if err != nil {
		return "", err
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return "", err
		}

		_, err = stmt.Exec(row[keyIdx], strings.Join(row, ";"))
		if err != nil {
			return "", err
		}
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}

	return tblName, nil
}

func keyIndex(key string, header []string) (int, error) {
	for i, h := range header {
		if h == key {
			return i, nil
		}
	}

	return 0, errors.New("key not present in file header")
}

func diff(oldTblName, newTblName string) (chan []string, chan []string, error) {
	inCh := make(chan []string)
	outCh := make(chan []string)

	close(inCh)
	close(outCh)

	return inCh, outCh, nil
}

func proccessDiff(diffType string, rowsCh chan []string) error {
	for row := range rowsCh {
		fmt.Printf("[%s] %s\n", diffType, row)
	}

	return nil
}
