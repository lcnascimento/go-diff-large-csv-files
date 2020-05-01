package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
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

	buffer := [][]string{}
	for i := 0; i < size; i++ {
		values := []string{
			strconv.FormatInt(int64(randomdata.Number(size)), 10),
			randomdata.FullName(randomdata.RandomGender),
			randomdata.Email(),
			randomdata.PhoneNumber(),
			randomdata.City(),
			randomdata.Country(randomdata.FullCountry),
		}

		buffer = append(buffer, values)

		if len(buffer)%500 == 0 {
			if err := writer.WriteAll(buffer); err != nil {
				return err
			}

			buffer = [][]string{}
		}
	}

	if len(buffer) > 0 {
		if err := writer.WriteAll(buffer); err != nil {
			return err
		}
	}

	return nil
}
