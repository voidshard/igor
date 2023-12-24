package main

import (
	"encoding/json"
	"fmt"

	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/structs"
)

func main() {
	db, err := database.NewRethinkImpl(&database.Options{
		URL: "localhost:28015",
	})
	if err != nil {
		panic(err)
	}

	/*
		jobs, err := db.Jobs(&structs.Query{
			JobIDs: []string{"1"},
		})
		if err != nil {
			panic(err)
		}

		for _, job := range jobs {
			fmt.Printf("job: %s\n", job)
		}
	*/

	objects, err := db.Tasks(&structs.Query{
		LayerIDs: []string{"0", "1", "2", "3", "4", "6", "7", "8", "9"},                               // no 5
		TaskIDs:  []string{"0", "1", "2", "3", "4", "5", "6", "7", "9", "10", "11", "12", "13", "14"}, // no 8 or > 14
	})
	if err != nil {
		panic(err)
	}

	for _, o := range objects {
		data, err := json.Marshal(o)
		if err != nil {
			fmt.Printf("%v err: %s\n", o, err)
		}
		fmt.Printf("%s\n", string(data))
	}

}
