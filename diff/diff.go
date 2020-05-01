package diff

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"golang.org/x/sync/errgroup"
)

// DiffType ...
type DiffType string

var (
	// InDiffType ...
	InDiffType DiffType = "+"
	// OutDiffType ...
	OutDiffType DiffType = "-"
)

// Diff ...
type Diff struct {
	oldFilepath string
	newFilepath string
	key         string
}

// NewDiff ...
func NewDiff(old, new, key string) *Diff {
	return &Diff{
		oldFilepath: old,
		newFilepath: new,
		key:         key,
	}
}

// Do ...
func (d Diff) Do(ctx context.Context) {
	db, err := sql.Open("sqlite3", "./poc.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	eg, _ := errgroup.WithContext(ctx)

	var oldTblName, newTblName string
	eg.Go(func() (err error) {
		oldTblName, err = createTable(db, d.oldFilepath, d.key)
		return err
	})
	eg.Go(func() (err error) {
		newTblName, err = createTable(db, d.newFilepath, d.key)
		return err
	})

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	inCh, outCh, err := diff(db, oldTblName, newTblName)
	if err != nil {
		log.Fatal(err)
	}

	eg, _ = errgroup.WithContext(ctx)

	eg.Go(func() error { return proccessDiff(InDiffType, inCh) })
	eg.Go(func() error { return proccessDiff(OutDiffType, outCh) })

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	if err := os.Remove("./poc.db"); err != nil {
		log.Fatal(err)
	}
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

func diff(db *sql.DB, oldTblName, newTblName string) (chan string, chan string, error) {
	inCh, err := handleDiffType(db, newTblName, oldTblName)
	if err != nil {
		return nil, nil, err
	}

	outCh, err := handleDiffType(db, oldTblName, newTblName)
	if err != nil {
		return nil, nil, err
	}

	return inCh, outCh, nil
}

func handleDiffType(db *sql.DB, baseTable, outerTable string) (chan string, error) {
	ch := make(chan string)

	tmpl := `
		SELECT $$BASE$$.id, $$BASE$$.row
		FROM $$BASE$$
		LEFT JOIN $$OUTER$$ on $$BASE$$.id = $$OUTER$$.id
		WHERE $$OUTER$$.id IS NULL
	`
	tmpl = strings.ReplaceAll(tmpl, "$$BASE$$", baseTable)
	tmpl = strings.ReplaceAll(tmpl, "$$OUTER$$", outerTable)

	rows, err := db.Query(tmpl)
	if err != nil {
		return nil, err
	}

	go func() {
		for rows.Next() {
			var id *string
			var row *string

			if err := rows.Scan(&id, &row); err != nil {
				log.Fatal(err)
			}

			if row != nil {
				ch <- *row
			}
		}

		close(ch)
		rows.Close()
	}()

	return ch, nil
}

func proccessDiff(diffType DiffType, rowsCh chan string) error {
	for row := range rowsCh {
		fmt.Printf("[%s] %s\n", diffType, row)
	}

	return nil
}
