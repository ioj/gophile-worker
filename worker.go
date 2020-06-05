/*
Package worker implements a task server compatible with graphile-worker.

Example:

  package main

  import (
  	"context"
  	"fmt"
  	"log"

  	worker "github.com/ioj/gophile-worker"
  	"github.com/ioj/gophile-worker/middleware"
  )

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

  	log.Fatal(w.ListenAndServe("dbname=worker_test sslmode=disable"))
	}
*/
package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v4/pgxpool"
)

// CtxKey is a key type for context values passed to handlers.
type CtxKey string

// CtxWorker is a context key which holds the worker instance.
const CtxWorker CtxKey = "worker"

// HandlerFunc defines a function to serve tasks.
type HandlerFunc func(ctx context.Context, job *Job) error

// MiddlewareFunc defines a function to process middleware.
type MiddlewareFunc func(HandlerFunc) HandlerFunc

// Used by Worker.stmts map
const (
	getJobStmt = iota
	failJobStmt
	completeJobStmt
)

// Worker is the root of your worker application. You should instantiate one
// of these and call ListenAndServe() to start accepting jobs.
//
// Always use the New() method to create a new Worker.
type Worker struct {
	opts *Opts

	pool        *pgxpool.Pool
	schemaident pgx.Identifier

	middlewares []MiddlewareFunc

	handlerfuncs map[string]HandlerFunc
	supportedIDs []string
	runningtasks sync.WaitGroup

	// Prepared statements.
	stmts map[int]string

	listening bool
}

// New initialises a new Worker. Configuration options are provided
// via an instance of Opts. Calling this function in your code will
// probably look something like this if you want to use defaults:
//
//     worker  := worker.New(nil)
//
// or:
//
//     opts    := &worker.Opts{Schema: "worker"}
//     worker  := worker.New(opts)
//
func New(pool *pgxpool.Pool, opts *Opts) *Worker {
	o := optsWithDefaults(opts)
	w := &Worker{
		pool:        pool,
		opts:        o,
		stmts:       make(map[int]string),
		schemaident: pgx.Identifier{o.Schema},
	}

	w.stmts[getJobStmt] = fmt.Sprintf(`
		SELECT id, queue_name, task_identifier, payload
		FROM %v.get_job($1, $2)`, w.schemaident.Sanitize())

	w.stmts[completeJobStmt] = fmt.Sprintf(`
		SELECT FROM %v.complete_job($1, $2)`, w.schemaident.Sanitize())

	w.stmts[failJobStmt] = fmt.Sprintf(`
		SELECT FROM %v.fail_job($1, $2, $3)`, w.schemaident.Sanitize())

	return w
}

// ID returns the worker ID.
func (w *Worker) ID() string {
	return w.opts.ID
}

// Pool returns the pointer to the underlying *pgxpool.DB.
// It might be null if worker wasn't started yet.
func (w *Worker) Pool() *pgxpool.Pool {
	return w.pool
}

// Schema returns a quoted schema name used by the worker,
// as configured in Opts.
func (w *Worker) Schema() pgx.Identifier {
	return w.schemaident
}

// Handle registers the handler for the given task ID.
// If a handler already exists for the ID, Handle panics.
func (w *Worker) Handle(taskID string, task HandlerFunc) {
	if w.listening {
		panic("registering handlers when listening is forbidden")
	}

	if taskID == "" {
		panic("invalid task id")
	}

	if _, ok := w.handlerfuncs[taskID]; ok {
		panic("handler already registered")
	}

	if w.handlerfuncs == nil {
		w.handlerfuncs = make(map[string]HandlerFunc)
	}

	w.handlerfuncs[taskID] = task
	w.supportedIDs = append(w.supportedIDs, taskID)
}

func (w *Worker) waitForNotification(ctx context.Context, noti chan bool) error {
	poolconn, err := w.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer poolconn.Release()

	conn := poolconn.Conn()
	_, err = conn.Exec(ctx, "listen \"jobs:insert\"")
	if err != nil {
		return err
	}

	for {
		if _, err = conn.WaitForNotification(ctx); err != nil {
			// This is a workaround for a pgx bug where it doesn't return
			// ctx.Err() when the context is canceled, but i/o timeout to the database.
			select {
			case <-ctx.Done():
				if ctx.Err() == context.Canceled {
					err = nil
				}
			default:
			}
			break
		}

		if err = ctx.Err(); err != nil {
			break
		}

		noti <- true
	}

	return nil
}

// ListenAndServe opens a connection to the database and starts listening
// for incoming jobs. When a matching job appears, it is dispatched
// to a HandlerFunc.
func (w *Worker) ListenAndServe(ctx context.Context) error {
	if w.listening {
		return errors.New("worker is already listening")
	}

	if len(w.supportedIDs) == 0 {
		return errors.New("no registered tasks")
	}

	noti := make(chan bool)
	errs := make(chan error)

	go func() {
		err := w.waitForNotification(ctx, noti)
		if err != nil {
			errs <- err
		}
	}()

	var err error

	for {
		select {
		case <-noti:
			if err = w.getJob(context.Background()); err != nil {
				goto done
			}
		case <-time.Tick(w.opts.PollInterval):
			if err = w.getJob(context.Background()); err != nil {
				goto done
			}
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				err = nil
			}
			goto done
		case e := <-errs:
			err = e
			goto done
		}
	}

done:

	w.runningtasks.Wait()
	return err
}

// Use appends a middleware handler to the Worker middleware stack.
//
// The middleware stack for a Worker will execute before searching for a matching
// taskID to a specific handler, which provides opportunity to respond early,
// change the course of the request execution, or set request-scoped values for
// the next Handler.
func (w *Worker) Use(middlewares ...MiddlewareFunc) {
	if w.listening {
		panic("adding middleware when listening is forbidden")
	}

	w.middlewares = append(w.middlewares, middlewares...)
}

func (w *Worker) getJob(ctx context.Context) error {
	for {
		var id sql.NullInt64
		var queueName sql.NullString
		var taskID sql.NullString
		var payload []byte

		if err := w.pool.QueryRow(ctx, w.stmts[getJobStmt], w.opts.ID, w.supportedIDs).
			Scan(&id, &queueName, &taskID, &payload); err != nil {
			return err
		}

		if !id.Valid || !taskID.Valid {
			// get_job() returned null
			return nil
		}

		job := &Job{
			ID:        id.Int64,
			TaskID:    taskID.String,
			QueueName: queueName.String,
			payload:   payload,
		}

		go func(job *Job) {
			w.runningtasks.Add(1)
			defer w.runningtasks.Done()
			w.doJob(ctx, job)
		}(job)
	}
}

func (w *Worker) failJob(ctx context.Context, job *Job, e error) error {
	_, err := w.pool.Exec(ctx, w.stmts[failJobStmt], w.opts.ID, job.ID, e.Error())
	return err
}

func (w *Worker) completeJob(ctx context.Context, job *Job) error {
	_, err := w.pool.Exec(ctx, w.stmts[completeJobStmt], w.opts.ID, job.ID)
	return err
}

func (w *Worker) doJob(ctx context.Context, job *Job) {
	h, ok := w.handlerfuncs[job.TaskID]
	if !ok {
		panic("got task without handler. this should never happen")
	}

	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("panic: %v", r)
			}
			w.failJob(ctx, job, err)
		}
	}()

	for i := len(w.middlewares) - 1; i >= 0; i-- {
		h = w.middlewares[i](h)
	}

	ctx = context.WithValue(ctx, CtxWorker, w)

	if err := h(ctx, job); err != nil {
		// TODO: retry mechanism
		ferr := w.failJob(ctx, job, err)
		if ferr != nil {
			panic("couldn't fail job: " + ferr.Error())
		}
	} else {
		// TODO: retry mechanism
		ferr := w.completeJob(ctx, job)
		if ferr != nil {
			panic("couldn't complete job: " + ferr.Error())
		}
	}
}
