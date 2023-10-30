package main

import (
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "At least 1 argument is required.")
		os.Exit(1)
	}

	logger := log.NewLogfmtLogger(os.Stdout)

	db, err := tsdb.Open(os.Args[1], logger, nil, &tsdb.Options{
		AllowOverlappingCompaction: true,
		MaxBlockDuration:           (6 * time.Hour).Milliseconds(),
	}, nil)
	if err != nil {
		fmt.Printf("error occurred: %s\n", err)
		os.Exit(1)
	}
	defer db.Close()

	err = db.Compact()
	if err != nil {
		fmt.Printf("error occurred while compacting: %s\n", err)
		os.Exit(1)
	}

	for _, b := range db.Blocks() {
		min := time.UnixMilli(b.Meta().MinTime)
		max := time.UnixMilli(b.Meta().MaxTime)
		dur := max.Sub(min)
		fmt.Printf("block %s; min: %s, max: %s, duration %s\n", b.Meta().ULID.String(), min, max, dur.String())
	}

	fmt.Println("done.")
}
