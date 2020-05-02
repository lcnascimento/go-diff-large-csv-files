package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/Pallinder/go-randomdata"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.Background()

	start := time.Now()

	size := flag.Int("size", 10e6, "number of rows in large_files")

	flag.Parse()

	eg, _ := errgroup.WithContext(ctx)

	eg.Go(func() error { return newLargeFile("old", *size) })
	eg.Go(func() error { return newLargeFile("new", *size) })

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Duration: %s\n", time.Since(start).String())
}

func newLargeFile(name string, size int) error {
	filename := fmt.Sprintf("./files/large_%s.csv", name)

	os.Remove(filename)

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)

	header := []string{
		"reference",
		"name",
		"email",
		"phone",
		"city",
		"country",
	}
	if err := writer.Write(header); err != nil {
		return err
	}
	writer.Flush()

	offset := 0
	if name == "new" {
		offset = int(math.Abs(float64(size / 2)))
	}

	for i := 0; i < size; i++ {
		values := []string{
			strconv.FormatInt(int64(i+offset), 10),
			randomdata.FullName(randomdata.RandomGender),
			randomdata.Email(),
			randomdata.PhoneNumber(),
			randomdata.City(),
			randomdata.Country(randomdata.FullCountry),
		}

		if err := writer.Write(values); err != nil {
			return err
		}

		if i%500 == 0 {
			writer.Flush()
		}
	}
	writer.Flush()

	return nil
}
