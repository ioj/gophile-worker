package middleware

import (
	"context"
	"log"
	"time"

	worker "github.com/ioj/gophile-worker"
)

// Logger returns a middleware that logs task execution.
func Logger(next worker.HandlerFunc) worker.HandlerFunc {
	return func(ctx context.Context, job *worker.Job) error {
		log.Printf("Executing job\tid=%v\ttask=%v\n", job.ID, job.TaskID)

		now := time.Now()
		err := next(ctx, job)

		log.Printf("Job done\tid=%v\tdur=%vms\terr=%v\n", job.ID, time.Since(now).Milliseconds(), err)
		return err
	}
}
