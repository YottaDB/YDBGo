# YottaDB Go Wrapper v2

[![Go Report Card](https://goreportcard.com/badge/gitlab.com/YottaDB/Lang/YDBGo?style=flat-square)](https://goreportcard.com/report/gitlab.com/YottaDB/Lang/YDBGo/v2) [![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/gitlab.com/YottaDB/Lang/YDBGo/v2) [![Coverage report](https://gitlab.com/YottaDB/Lang/YDBGo/v2/badges/master/coverage.svg?job=coverage)](https://gitlab.com/YottaDB/Lang/YDBGo/v2/-/jobs)

Warning: YDBGo v2 is in alpha testing.

# Quick Start

YottaDB must be installed. See [Get Started](https://yottadb.com/product/get-started/) for instructions for installing YottaDB. YDBGo supports versions YottaDB r1.34 or later.

1. A YottaDB database must be set up and the environment variables configured.
   This can be easily done by sourcing `/path/to/yottadb/ydb_env_set`. For example:

```sh
source $(pkg-config --variable=prefix yottadb)/ydb_env_set
```

2. Create an empty directory anywhere on your file system and go there.

```
mkdir ydb-example
cd ydb-example
```

3. Use `go mod init` to create a package for your code in a unique namespace:

```
go mod init example.com/yottadb-example
```

4. Create a Go program (e.g. `ydb.go`) containing your main program with an import of `lang.yottadb.com/go/yottadb`, e.g. the [example script in the YDBGo documentation](https://pkg.go.dev/lang.yottadb.com/go/yottadb/v2/#pkg-overview).

```go
package main

import (
	"lang.yottadb.com/go/v2/yottadb"
)

func main() {
	defer yottadb.Shutdown(yottadb.InitPanic())
	conn := yottadb.NewConn()

	// Store unicode greeting into global node ^hello("world")
	greeting := conn.Node("^hello", "world")
	greeting.Set("สวัสดี") // Sawadee (hello in Thai)
	fmt.Println(greeting.Get())
}
```

5. Download the YottaDB module by using `go get .`

6. Run the code using `go run .`

7. You can verify that the data got saved by running `mupip extract -sel=^hello -stdout`

8. `go build` will create an exe for you (`yottadb-example` in this case). You
   can run that directly (e.g. `./yottadb-example`).

9. `go install` will install the exe for you in $GOPATH/bin or `~/go` by default.

## Mixing YDBGo v1 and v2

Applications that use v1 will continue to operate without change. Moreover, to aid migration of large applications from YDBGo v1 to v2, it is even possible to use v1 and v2 in one application. However, the the two versions cannot use each other's data types so this will only make sense where the old and new functionality is fairly modular. All signal handling will need to be migrated to v2 since v1 signal handlers will no longer be called.

To use v1 and v2 in one application you will need to add a named import like this:

```go
import v1 "lang.yottadb.com/go/yottadb"
```

You must remove any calls to v1 `Init()` and `Exit()` and instead add the following line immediately after calling v2 `Init()`:

```go
v1.ForceInit()  // only available in v1.2.8
```

This will let v1 know that v2 has already done the initialization.

## Testing

To test this wrapper:

- `go build` only does a test compilation; it does not produce any files; `go install` has no effect.
- To run tests, run `make test`
- To run benchmarks, run `make bench`

Notes:

* Some CPUs gradually warm up during benchmarking, making the first few tests unfairly faster. Benchmark results are a lot more accurate and fair if you install [`perflock`](https://github.com/aclements/perflock), which `make bench` will then invoke it automatically.
* Perflock is apparently less effective on some CPUs. You can test whether it's working to produce consistent results by running  `make check` which will run each benchmark several times so that you can see whether you're experiencing warm-up effects as the multiple tests run.

## Contributing

Last, if you plan to commit, you should set-up pre-commit hooks.

```sh
ln -s ../../pre-commit .git/hooks
go install honnef.co/go/tools/cmd/staticcheck@latest
```

To develop the YottaDB Go wrapper itself you may wish to import a *local* version of the wrapper instead of the public wrapper on the internet. To do this, clone the wrapper, then in a separate directory create your application that uses the wrapper and use `go work` commands to point it to the wrapper on your local file system rather than the internet repository.

To do so, run the following commands in the client app directory:

```sh
go work init
go work use . /your/local/path/to/YDBGo/v2  # Set this path to your YDBGo clone
git ignore go.work
```

The `git ignore` line prevents you from committing this local change to the public who will not have your local wrapper clone.

Now you can modify the YottaDB Go wrapper elsewhere on your local file system, and it will be immediately used by your client application, even before you commit the wrapper changes.

## Docker Container

The Dockerfile included in this repository creates an Ubuntu image with YottaDB, Go, and some basic development tools (git, gcc, make, vim, etc).

### Building the Container

To build the container run:

```sh
docker build . -t ydbgo
```

### Using the container to develop YDBGo
```sh
docker run --rm -it -v ~/goprojects:/goprojects -v ~/work/gitlab/YDBGo:/YDBGo -v ${PWD}/data:/data -w /goprojects ydbgo bash
```

Then follow the instructions for usage and setting up for development above.
