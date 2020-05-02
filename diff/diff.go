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
	"time"

	"github.com/Pallinder/go-randomdata"
	"golang.org/x/sync/errgroup"
)

// Type ...
type Type string

// RowRecord ...
type RowRecord struct {
	ID  string
	Row string
}

var (
	// InType ...
	InType Type = "+"
	// OutType ...
	OutType Type = "-"
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
func (d Diff) Do(ctx context.Context) error {
	execID := randomdata.Alphanumeric(5)
	dbFilename := fmt.Sprintf("./%s.db", execID)

	fmt.Println("Starting diff with ID ", execID)

	start := time.Now()

	db, err := sql.Open("sqlite3", dbFilename)
	if err != nil {
		return err
	}
	defer db.Close()

	eg, _ := errgroup.WithContext(ctx)

	var oldTblName, newTblName string
	eg.Go(func() (err error) {
		oldTblName, err = createTable(db, execID, d.oldFilepath, d.key)
		return err
	})
	eg.Go(func() (err error) {
		newTblName, err = createTable(db, execID, d.newFilepath, d.key)
		return err
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	fmt.Printf("[%s] Load Duration: %s\n", execID, time.Since(start).String())
	endLoad := time.Now()

	inCh, outCh, err := diff(db, oldTblName, newTblName)
	if err != nil {
		return err
	}

	eg, _ = errgroup.WithContext(ctx)

	eg.Go(func() error { return proccessDiff(execID, InType, inCh) })
	eg.Go(func() error { return proccessDiff(execID, OutType, outCh) })

	if err := eg.Wait(); err != nil {
		return err
	}

	if err := os.Remove(dbFilename); err != nil {
		return err
	}

	fmt.Printf("[%s] Diff Duration: %s\n", execID, time.Since(endLoad).String())

	return nil
}

func createTable(db *sql.DB, execID, filepath, key string) (string, error) {
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

	sql := `
		CREATE TABLE IF NOT EXISTS $$TABLE_NAME$$ (id INTEGER PRIMARY KEY AUTOINCREMENT, key text, row text);
		CREATE INDEX idx_$$TABLE_NAME$$_key ON $$TABLE_NAME$$ (key)
	`
	sql = strings.ReplaceAll(sql, "$$TABLE_NAME$$", tblName)

	_, err = db.Exec(sql)
	if err != nil {
		return "", err
	}

	reader := csv.NewReader(f)

	header, err := reader.Read()
	if err != nil {
		return "", err
	}

	keyIdx, err := keyIndex(key, header)
	if err != nil {
		return "", err
	}

	buffer := []*RowRecord{}
	count := 0
	for ; ; count++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return "", err
		}

		buffer = append(buffer, &RowRecord{
			ID:  row[keyIdx],
			Row: strings.Join(row, ","),
		})
		if len(buffer)%100000 == 0 {
			if err := bulkInsert(db, tblName, buffer, count); err != nil {
				return "", err
			}
			buffer = []*RowRecord{}
		}
	}

	if err := bulkInsert(db, tblName, buffer, count); err != nil {
		return "", err
	}

	return tblName, nil
}

func bulkInsert(db *sql.DB, tblName string, bulk []*RowRecord, count int) error {
	if len(bulk) == 0 {
		return nil
	}

	fmt.Printf("[%s] Progress: %d\n", tblName, count+1)

	definitions := []string{}
	for _, record := range bulk {
		definitions = append(definitions, fmt.Sprintf("('%s', '%s')", record.ID, record.Row))
	}

	query := fmt.Sprintf("INSERT INTO %s(key, row) VALUES %s", tblName, strings.Join(definitions, ","))

	_, err := db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func keyIndex(key string, header []string) (int, error) {
	for i, h := range header {
		if h == key {
			return i, nil
		}
	}

	return 0, errors.New("key not present in file header")
}

func diff(db *sql.DB, oldTblName, newTblName string) (chan []string, chan []string, error) {
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

func handleDiffType(db *sql.DB, baseTable, outerTable string) (chan []string, error) {
	ch := make(chan []string)

	tmpl := `
		SELECT $$BASE$$.key, $$BASE$$.row
		FROM $$BASE$$
		LEFT JOIN $$OUTER$$ on $$BASE$$.key = $$OUTER$$.key
		WHERE $$OUTER$$.key IS NULL
		GROUP BY $$BASE$$.key, $$BASE$$.row
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
				ch <- strings.Split(*row, ",")
			}
		}

		close(ch)
		rows.Close()
	}()

	return ch, nil
}

func proccessDiff(id string, diffType Type, rowsCh chan []string) error {
	filename := "in.csv"
	if diffType == OutType {
		filename = "out.csv"
	}

	folderPath := fmt.Sprintf("./output/%s", id)
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		os.MkdirAll(folderPath, 0700)
	}

	f, err := os.Create(fmt.Sprintf("./output/%s/%s", id, filename))
	if err != nil {
		return err
	}

	writer := csv.NewWriter(f)

	for row := range rowsCh {
		writer.Write(row)
		writer.Flush()
	}

	return nil
}
