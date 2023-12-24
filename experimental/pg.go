package main

import (
	"fmt"
	"time"

	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/structs"
)

func main() {
	db, err := database.NewPostgres(&database.Options{URL: "postgres://postgres:test@localhost:5432/igor?sslmode=disable&search_path=igor"})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	go func() {
		changes, err := db.Changes()
		if err != nil {
			panic(err)
		}
		defer changes.Close()

		for {
			fmt.Println("[listener] waiting for changes...")
			ch, err := changes.Next()
			if err != nil {
				fmt.Println("[listener] error", err)
				panic(err)
			}
			fmt.Println("[listener] got change", ch)
		}
	}()

	fmt.Println("inserting job")
	err = db.InsertJob(
		&structs.Job{
			JobSpec: structs.JobSpec{
				Name: "job1",
			},
			ID:        "job1",
			Status:    structs.PENDING,
			ETag:      "etag1",
			CreatedAt: time.Now().Unix(),
			UpdatedAt: time.Now().Unix(),
		},
		[]*structs.Layer{
			&structs.Layer{
				LayerSpec: structs.LayerSpec{
					Name:     "layer1",
					Priority: 0,
				},
				ID:        "layer1",
				Status:    structs.PENDING,
				ETag:      "etag1",
				JobID:     "job1",
				CreatedAt: time.Now().Unix(),
				UpdatedAt: time.Now().Unix(),
			},
			&structs.Layer{
				LayerSpec: structs.LayerSpec{
					Name:     "layer2",
					Priority: 10,
				},
				ID:        "layer2",
				Status:    structs.PENDING,
				ETag:      "etag1",
				JobID:     "job1",
				CreatedAt: time.Now().Unix(),
				UpdatedAt: time.Now().Unix(),
			},
		},
		[]*structs.Task{
			&structs.Task{
				TaskSpec: structs.TaskSpec{
					Type: "task1",
					Args: []byte("args1"),
					Name: "task1",
				},
				ID:        "task1",
				Status:    structs.PENDING,
				ETag:      "etag1",
				JobID:     "job1",
				LayerID:   "layer1",
				CreatedAt: time.Now().Unix(),
				UpdatedAt: time.Now().Unix(),
			},
			&structs.Task{
				TaskSpec: structs.TaskSpec{
					Type: "task2",
					Args: []byte("args2"),
					Name: "task2",
				},
				ID:        "task2",
				Status:    structs.PENDING,
				ETag:      "etag1",
				JobID:     "job1",
				LayerID:   "layer2",
				CreatedAt: time.Now().Unix(),
				UpdatedAt: time.Now().Unix(),
			},
		},
	)
	fmt.Println("inserted job", err)

	time.Sleep(5 * time.Second)

}
