package main

import (
	"context"
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
