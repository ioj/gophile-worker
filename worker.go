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

	"github.com/lib/pq"
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

	db *sql.DB

	middlewares []MiddlewareFunc

	handlerfuncs map[string]HandlerFunc
	supportedIDs pq.StringArray
	runningtasks sync.WaitGroup

	// Prepared statements.
	stmts map[int]*sql.Stmt

	listening bool
	shutdown  chan bool
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
func New(opts *Opts) *Worker {
	return &Worker{opts: optsWithDefaults(opts)}
}

// ID returns the worker ID.
func (w *Worker) ID() string {
	return w.opts.ID
}

// DB returns the pointer to the underlying *sql.DB.
// It might be null if worker wasn't started yet.
func (w *Worker) DB() *sql.DB {
	return w.db
}

// Schema returns a quoted schema name used by the worker,
// as configured in Opts.
func (w *Worker) Schema() string {
	return w.opts.Schema
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

// ListenAndServe opens a connection to the database and starts listening
// for incoming jobs. When a matching job appears, it is dispatched
// to a HandlerFunc.
func (w *Worker) ListenAndServe(conninfo string) error {
	if w.listening {
		return errors.New("worker is already listening")
	}

	if len(w.supportedIDs) == 0 {
		return errors.New("no registered tasks")
	}

	w.shutdown = make(chan bool, 1)

	var err error
	if w.db, err = sql.Open("postgres", conninfo); err != nil {
		return err
	}

	if err := w.db.Ping(); err != nil {
		return err
	}

	if err := w.prepareStmts(); err != nil {
		return err
	}

	listener := pq.NewListener(conninfo,
		w.opts.MinReconnectInterval, w.opts.MaxReconnectInterval, nil)
	err = listener.Listen("jobs:insert")
	if err != nil {
		return err
	}

	for {
		if err := w.getJob(); err != nil {
			panic(err)
		}

		select {
		case <-w.shutdown:
			goto shutdown
		case <-listener.Notify:
		case <-time.After(w.opts.PollInterval):
			go listener.Ping()
		}
	}

shutdown:
	w.runningtasks.Wait()
	if err := listener.Close(); err != nil {
		return err
	}

	if err := w.closePreparedStmts(); err != nil {
		return nil
	}

	return w.db.Close()
}

// Shutdown gracefully shuts down the worker without interrupting any active tasks.
func (w *Worker) Shutdown() {
	w.shutdown <- true
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

func (w *Worker) closePreparedStmts() error {
	for _, stmt := range w.stmts {
		if err := stmt.Close(); err != nil {
			return err
		}
	}

	w.stmts = nil
	return nil
}

func (w *Worker) prepareStmts() error {
	var err error

	if w.stmts == nil {
		w.stmts = make(map[int]*sql.Stmt)
	} else {
		w.closePreparedStmts()
	}

	w.stmts[getJobStmt], err = w.db.Prepare(fmt.Sprintf(`
		SELECT id, queue_name, task_identifier, payload
		FROM %v.get_job($1, $2)`, w.opts.Schema))
	if err != nil {
		return err
	}

	w.stmts[completeJobStmt], err = w.db.Prepare(fmt.Sprintf(`
		SELECT FROM %v.complete_job($1, $2)`, w.opts.Schema))
	if err != nil {
		return err
	}

	w.stmts[failJobStmt], err = w.db.Prepare(fmt.Sprintf(`
		SELECT FROM %v.fail_job($1, $2, $3)`, w.opts.Schema))

	return err
}

func (w *Worker) getJob() error {
	for {
		var id sql.NullInt64
		var queueName sql.NullString
		var taskID sql.NullString
		var payload []byte

		if err := w.stmts[getJobStmt].QueryRow(w.opts.ID, w.supportedIDs).
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
			w.doJob(job)
		}(job)
	}
}

func (w *Worker) failJob(job *Job, e error) error {
	_, err := w.stmts[failJobStmt].Exec(w.opts.ID, job.ID, e.Error())
	return err
}

func (w *Worker) completeJob(job *Job) error {
	_, err := w.stmts[completeJobStmt].Exec(w.opts.ID, job.ID)
	return err
}

func (w *Worker) doJob(job *Job) {
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
			w.failJob(job, err)
		}
	}()

	for i := len(w.middlewares) - 1; i >= 0; i-- {
		h = w.middlewares[i](h)
	}

	ctx := context.WithValue(context.Background(), CtxWorker, w)

	if err := h(ctx, job); err != nil {
		// TODO: retry mechanism
		ferr := w.failJob(job, err)
		if ferr != nil {
			panic("couldn't fail job: " + ferr.Error())
		}
	} else {
		// TODO: retry mechanism
		ferr := w.completeJob(job)
		if ferr != nil {
			panic("couldn't complete job: " + ferr.Error())
		}
	}
}
