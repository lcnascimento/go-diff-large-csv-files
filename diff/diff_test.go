package diff_test

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/lcnascimento/go-diff-large-csv-files/diff"
)

func BenchmarkDo(b *testing.B) {
	b.Run("tiny files", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			diff := diff.NewDiff("../files/old.csv", "../files/new.csv", "reference")
			diff.Do(context.Background())
		}
	})

	b.Run("gigantic files", func(b *testing.B) {
		b.ReportAllocs()
	})
}
