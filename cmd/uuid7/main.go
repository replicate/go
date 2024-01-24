package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/replicate/go/uuid"
)

func main() {
	count := flag.Int("count", 1, "number of uuids to create (default: 1)")
	timestamps := flag.Bool("timestamps", false, "include timestamp in column (default: false)")

	flag.Parse()

	if *count < 0 {
		fmt.Println("count cannot be less than 0")
		os.Exit(1)
	}

	for i := 1; i <= *count; i++ {
		u, err := uuid.NewV7()
		if err != nil {
			fmt.Println("error creating uuid: %w", err)
			os.Exit(1)
		}

		ts, err := uuid.TimeFromV7(u)
		if err != nil {
			fmt.Println("error extracting timestamp: %w", err)
			os.Exit(1)
		}

		if *timestamps {
			fmt.Println(u, ts.Format(time.RFC3339Nano))
		} else {
			fmt.Println(u)
		}
	}
}
