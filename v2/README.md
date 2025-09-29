# YottaDB Go Wrapper v2

[![Go Report Card](https://goreportcard.com/badge/lang.yottadb.com/go/yottadb/v2?style=flat-square)](https://goreportcard.com/report/lang.yottadb.com/go/yottadb/v2) | [![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](https://godoc.org/lang.yottadb.com/go/yottadb/v2) | [![Coverage report](https://gitlab.com/YottaDB/Lang/YDBGo/badges/master/coverage.svg?job=v2_coverage)](https://gitlab.com/YottaDB/Lang/YDBGo/-/jobs?name=v2_coverage)

Warning: YDBGo v2 is in alpha testing. Reference documentation is on the [Go packages website](https://pkg.go.dev/lang.yottadb.com/go/yottadb/v2).

## Quick Start

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

4. Create a Go program (e.g. `ydb.go`) containing your main program with an import of `lang.yottadb.com/go/yottadb/v2`, e.g. the [example script in the YDBGo documentation](https://pkg.go.dev/lang.yottadb.com/go/yottadb/v2/#pkg-overview), which amounts to:

```go
package main

import "fmt"
import "lang.yottadb.com/go/yottadb/v2"

func main() {
	defer yottadb.Shutdown(yottadb.MustInit())
	conn := yottadb.NewConn()

	// Store unicode greeting into node ^hello("world")
	greeting := conn.Node("^hello", "world")
	greeting.Set("สวัสดี") // Sawadee (hello in Thai)
	fmt.Println(greeting.Get())

	// Output:
	// สวัสดี
}
```

5. Download the YottaDB module by using `go get .`

6. Run the code using `go run .`

7. You can verify that the data got saved by running `mupip extract -sel=^hello -stdout`

8. `go build` will create an executable for you (`yottadb-example` in this case). You
   can run that directly (e.g. `./yottadb-example`).

9. `go install` will install the executable for you in your Go path ($GOPATH/bin or `~/go` by default).

## Intro by example

To see how to increment or clear database node values, or kill entire node trees, replace `main()` above with:

```go
defer yottadb.Shutdown(yottadb.MustInit())
conn := yottadb.NewConn()

hi := conn.Node("^howdy")            // create Node instance pointing to YottaDB global ^hello
hi.Set("western")
cowboy := hi.Child("cowboy")         // new variable pointing to subnode "cowboy" subscript
cowboy.Set("Howdy partner!")         // set ^hello("cowboy") to "Howdy partner!"
ranches := cowboy.Child("ranches")
ranches.Incr(2)                      // Increment empty node by 2 to get 2

fmt.Printf("First dump:\n%#v\n", hi) // %#v produces the same string as hi.Dump()
cowboy.Kill()                        // delete node, its children, and all values
fmt.Printf("Second dump:\n%#v\n", hi)
hi.Clear()                           // clear this node's value, too

// Output:
// First dump:
// ^howdy="western"
// ^howdy("cowboy")="Howdy partner!"
// ^howdy("cowboy","ranches")=2
//
// Second dump:
// ^howdy="western"
```

**Doing something useful**

Let's use Go to calculate the height of 3 oak trees, based on their shadow length and the angle of the sun. Replace main() with the following code:

```go
defer yottadb.Shutdown(yottadb.MustInit())
conn := yottadb.NewConn()

// capture initial data values into a Go map
data := []map[string]int{
    {"shadow": 10, "angle": 30},
    {"shadow": 13, "angle": 30},
    {"shadow": 15, "angle": 45},
}

// Enter data into the database
trees := conn.Node("^oaks") // node object pointing to YottaDB global ^oaks
for i, items := range data {
    for key, value := range items {
        trees.Child(i, key).Set(value)
    }
}

// Iterate data in the database and calculate results
for tree, i := range trees.Children() {
    tree.Child("height").Set(tree.Child("shadow").GetFloat() *
        math.Tan(tree.Child("angle").GetFloat()*math.Pi/180))
    fmt.Printf("Oak %s is %.1fm high\n", i, tree.Child("height").GetFloat())
}

// Output:
// Oak 1 is 5.8m high
// Oak 2 is 7.5m high
// Oak 3 is 15.0m high
```

## Advanced Configuration

### Installing pkg-config

Go's installation of this package uses pkg-config to find `libyottadb.h`. This requires installation of the `yottadb.pc` file, which is generated only when YottaDB is installed with `ydbinstall`.

If you need to manually generate the `yottadb.pc` file, the contents should look something similar to:

```sh
prefix=/usr/local/lib/yottadb/r202

exec_prefix=${prefix}
includedir=${prefix}
libdir=${exec_prefix}

Name: YottaDB
Description: YottaDB database library
Version: r2.02
Cflags: -I${includedir}
Libs: -L${libdir} -lyottadb -Wl,-rpath,${libdir}
```

Change the prefix to the correct path for your environment.

NOTE: you cannot use environment variables for the prefix path (e.g. `$ydb_dist`) it must be a fully qualified path.

You can also override the path used by pkg-config to find `yottadb.pc` with the environment variable `PKG_CONFIG_PATH` and a path to where the `yottadb.pc` file resides.

### Migrating v1 to v2

Applications that use YDBGo v1 will continue to operate without change. Moreover, to aid migration of large applications from YDBGo v1 to v2, it is even possible to mix v1 and v2 concurrently in one application. However, the the two versions cannot use each other's data types so this will only make sense where the old and new functionality is fairly modular. All signal handling will need to be migrated to v2 since v1 signal handlers will no longer be called.

To use v1 and v2 in one application you will need to add a named import like this:

```go
import v1 "lang.yottadb.com/go/yottadb"
```

You must remove any calls to v1 `Init()` and `Exit()` and instead add the following line immediately after calling v2 `Init()`:

```go
v1.ForceInit()  // only available in v1.2.8
```

This will let v1 know that v2 has already done the initialization.

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

If you are developing YDBGo itself, you may find it useful to increase debugMode to increase debug logging as documented in `yottadb.go`. This is a private variable that cannot be set by the YDBGo user. It is only intended for use by developers of YDBGo. Coverage tests turn it on to ensure coverage of that debug portion of the code.

### Testing

To test this wrapper:

- `go build` only does a test compilation; it does not produce any files; `go install` has no effect.

- To run tests, run `make test`

- To run benchmarks, run `make bench`

  * Some CPUs gradually warm up during benchmarking, making the first few benchmark tests unfairly faster. Benchmark results are a lot more accurate and fair if you install [`perflock`](https://github.com/aclements/perflock), which `make bench` will then invoke it automatically.

  * Perflock is apparently less effective on some CPUs. You can test whether it's working to produce consistent results by running  `make check` which will run each benchmark several times so that you can see whether you're experiencing warm-up effects as the multiple tests run.

### Docker Container

The Dockerfile included in this repository creates an Ubuntu image with YottaDB, Go, and some basic development tools (git, gcc, make, vim, etc).

To **build the Docker container** run:

```sh
docker build . -t ydbgo
```

To **use the container to develop** YDBGo:

```sh
docker run --rm -it -v ~/goprojects:/goprojects -v ~/work/gitlab/YDBGo:/YDBGo -v ${PWD}/data:/data -w /goprojects ydbgo bash
```

Then follow the instructions for usage and setting up for development above.
