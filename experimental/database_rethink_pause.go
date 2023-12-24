package main

import (
	"fmt"

	"github.com/voidshard/igor/pkg/database"
)

func main() {
	db, err := database.NewRethinkImpl(&database.Options{
		Address: "localhost:28015",
	})
	if err != nil {
		panic(err)
	}

	delta, err := db.SetJobsPaused(
		15, "e15",
		[]*database.IDTag{
			&database.IDTag{ID: "1", ETag: "e1"},
			&database.IDTag{ID: "2", ETag: "e2"},
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("delta: %d\n", delta)
}
