package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	worker "github.com/ioj/gophile-worker"
	"github.com/ioj/gophile-worker/middleware"
)

// HelloPayload defines a payload for the "hello" task.
type HelloPayload struct {
	Name string `json:"name"`
}

func main() {
	w := worker.New(nil)

	w.Use(middleware.Logger)

	w.Handle("hello", func(ctx context.Context, job *worker.Job) error {
		p := HelloPayload{Name: "stranger"}
		if err := job.UnmarshalPayload(&p); err != nil {
			return err
		}

		fmt.Printf("Hello, %v!\n", p.Name)
		return nil
	})

	w.Handle("jobcount", func(ctx context.Context, job *worker.Job) error {
		w := ctx.Value(worker.CtxWorker).(*worker.Worker)

		db := w.DB()
		if db == nil {
			return errors.New("db handle is nil")
		}

		var count int
		stmt := fmt.Sprintf(`SELECT count(*) FROM %v.jobs`, w.Schema())
		if err := db.QueryRowContext(ctx, stmt).Scan(&count); err != nil {
			return err
		}

		fmt.Printf("total jobs in queue: %v\n", count)
		return nil
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		<-c
		w.Shutdown()
	}()

	conninfo := "dbname=worker_test sslmode=disable"
	if err := w.ListenAndServe(conninfo); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Graceful shutdown.")
}
