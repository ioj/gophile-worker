# gophile-worker

[![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/ioj/gophile-worker)
[![Go Report Card](https://goreportcard.com/badge/github.com/ioj/gophile-worker?style=flat-square)](https://goreportcard.com/report/github.com/ioj/gophile-worker)
[![License](http://img.shields.io/badge/license-mit-blue.svg?style=flat-square)](https://raw.githubusercontent.com/ioj/gophile-worker/master/LICENSE.md)

A simple task runner compatible with [graphile-worker](https://github.com/graphile/worker)
&mdash; if you prefer to run your background tasks with Go instead of Nodejs.

## Quickstart

#### Create a test database

```
createdb worker_test
```

#### Install graphile-worker schema

```
npx graphile-worker -c "worker_test" --schema-only
```

#### Implement a simple task runner

```go
package main

import (
  "context"
  "fmt"
  "log"

  worker "github.com/ioj/gophile-worker"
)

type HelloPayload struct {
  Name string `json:"name"`
}

func main() {
  w := worker.New(nil)

  w.Handle("hello", func(ctx context.Context, job *worker.Job) error {
    p := HelloPayload{}
    if err := job.UnmarshalPayload(&p); err != nil {
      return err
    }

    if p.Name == "" {
      p.Name = "stranger"
    }

    fmt.Printf("Hello, %v!\n", p.Name)
    return nil
  })

  conninfo := "dbname=worker_test sslmode=disable"
  log.Fatal(w.ListenAndServe(conninfo))
}
```

#### Schedule a job via SQL

```sql
SELECT graphile_worker.add_job('hello', json_build_object('name', 'Bobby Tables'));
```

You should see the worker output `Hello, Bobby Tables!`

## What's next?

Please take a look at [Documentation](http://godoc.org/github.com/ioj/gophile-worker) and a more elaborate example, which shows how to use middleware and handle graceful shutdown.

## License

This library is distributed under the terms of the MIT License. See the included file for more detail.

## Contributing

All suggestions and patches welcome, preferably via a git repository I can pull from. If this library proves useful to you, please let me know.
