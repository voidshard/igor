package main

import (
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/structs"
)

func main() {
	db, err := database.NewRethinkImpl(&database.Options{
		Address: "localhost:28015",
	})
	if err != nil {
		panic(err)
	}

	err = db.InsertJobs([]*structs.Job{
		&structs.Job{ID: "1", ETag: "e1"},
		&structs.Job{ID: "2", ETag: "e2"},
		&structs.Job{ID: "3", ETag: "e3"},
		&structs.Job{ID: "4", ETag: "e4"},
		&structs.Job{ID: "5", ETag: "e5"},
		&structs.Job{ID: "6", ETag: "e6"},
		&structs.Job{ID: "7", ETag: "e7"},
	})
	if err != nil {
		panic(err)
	}

	err = db.InsertLayers([]*structs.Layer{
		&structs.Layer{LayerSpec: structs.LayerSpec{Name: "test"}, ID: "0", ETag: "e0", JobID: "1"},
		&structs.Layer{ID: "1", ETag: "e1", JobID: "1"},
		&structs.Layer{ID: "2", ETag: "e2", JobID: "2"},
		&structs.Layer{ID: "3", ETag: "e3", JobID: "3"},
		&structs.Layer{ID: "4", ETag: "e4", JobID: "3"},
		&structs.Layer{ID: "5", ETag: "e5", JobID: "3"},
		&structs.Layer{ID: "6", ETag: "e6", JobID: "4"},
		&structs.Layer{ID: "7", ETag: "e7", JobID: "5"},
		&structs.Layer{ID: "8", ETag: "e8", JobID: "6"},
		&structs.Layer{ID: "9", ETag: "e9", JobID: "7"},
	})
	if err != nil {
		panic(err)
	}

	err = db.InsertTasks([]*structs.Task{
		&structs.Task{ID: "0", ETag: "e0", JobID: "1", LayerID: "0"},
		&structs.Task{ID: "1", ETag: "e1", JobID: "1", LayerID: "0"},
		&structs.Task{ID: "2", ETag: "e2", JobID: "2", LayerID: "2"},
		&structs.Task{ID: "3", ETag: "e3", JobID: "3", LayerID: "3"},
		&structs.Task{ID: "4", ETag: "e4", JobID: "3", LayerID: "3"},
		&structs.Task{ID: "5", ETag: "e5", JobID: "3", LayerID: "3"},
		&structs.Task{ID: "6", ETag: "e6", JobID: "3", LayerID: "4"},
		&structs.Task{ID: "7", ETag: "e7", JobID: "3", LayerID: "4"},
		&structs.Task{ID: "9", ETag: "e8", JobID: "3", LayerID: "5"},
		&structs.Task{ID: "10", ETag: "e9", JobID: "4", LayerID: "6"},
		&structs.Task{ID: "11", ETag: "e0", JobID: "4", LayerID: "6"},
		&structs.Task{ID: "12", ETag: "e1", JobID: "4", LayerID: "6"},
		&structs.Task{ID: "13", ETag: "e2", JobID: "4", LayerID: "6"},
		&structs.Task{ID: "14", ETag: "e3", JobID: "4", LayerID: "6"},
		&structs.Task{ID: "15", ETag: "e4", JobID: "6", LayerID: "8"},
		&structs.Task{ID: "16", ETag: "e5", JobID: "6", LayerID: "8"},
		&structs.Task{ID: "17", ETag: "e6", JobID: "7", LayerID: "9"},
		&structs.Task{ID: "18", ETag: "e7", JobID: "7", LayerID: "9"},
		&structs.Task{ID: "19", ETag: "e8", JobID: "7", LayerID: "9"},
	})
	if err != nil {
		panic(err)
	}
}
