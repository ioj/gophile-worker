package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	worker "github.com/ioj/gophile-worker"
	"github.com/ioj/gophile-worker/middleware"
	"github.com/jackc/pgx/v4/pgxpool"
)

// HelloPayload defines a payload for the "hello" task.
type HelloPayload struct {
	Name string `json:"name"`
}

func main() {
	conninfo := "dbname=worker_test sslmode=disable"
	pool, err := pgxpool.Connect(context.Background(), conninfo)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	w := worker.New(pool, nil)

	w.Use(middleware.Logger)

	w.Handle("hello", func(ctx context.Context, job *worker.Job) error {
		p := HelloPayload{Name: "stranger"}
		if err := job.UnmarshalPayload(&p); err != nil {
			return err
		}

		fmt.Printf("Hello, %v!\n", p.Name)
		return nil
	})

	w.Handle("longtask", func(ctx context.Context, job *worker.Job) error {
		fmt.Printf("doing long task...")
		time.Sleep(5 * time.Second)
		fmt.Printf("long task done...")
		return nil
	})

	w.Handle("jobcount", func(ctx context.Context, job *worker.Job) error {
		w := ctx.Value(worker.CtxWorker).(*worker.Worker)

		db := w.Pool()
		if db == nil {
			return errors.New("db handle is nil")
		}

		var count int
		stmt := fmt.Sprintf(`SELECT count(*) FROM %v.jobs`, w.Schema().Sanitize())
		if err := db.QueryRow(ctx, stmt).Scan(&count); err != nil {
			return err
		}

		fmt.Printf("total jobs in queue: %v\n", count)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		<-c
		cancel()
	}()

	if err := w.ListenAndServe(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Println("graceful shutdown")
}
