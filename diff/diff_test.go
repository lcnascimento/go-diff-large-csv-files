package diff_test

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/lcnascimento/go-diff-large-csv-files/diff"
)

func BenchmarkDo(b *testing.B) {
	// b.Run("tiny files", func(b *testing.B) {
	// 	b.ReportAllocs()

	// 	for i := 0; i < b.N; i++ {
	// 		diff := diff.NewDiff("../files/tiny_old.csv", "../files/tiny_new.csv", "reference")
	// 		if err := diff.Do(context.Background()); err != nil {
	// 			b.Fatal(err)
	// 		}
	// 	}
	// })

	b.Run("gigantic files", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			diff := diff.NewDiff("../files/large_old.csv", "../files/large_new.csv", "reference")
			if err := diff.Do(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})
}
