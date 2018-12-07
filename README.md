# YottaDB Go Wrapper

[![Build Status](https://gitlab.com/YottaDB/Lang/YDBGo/badges/master/build.svg)](https://gitlab.com/YottaDB/Lang/YDBGo/commits/master)
[![Go Report Card](https://goreportcard.com/badge/gitlab.com/YottaDB/Lang/YDBGo?style=flat-square)](https://goreportcard.com/report/gitlab.com/YottaDB/Lang/YDBGo)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/gitlab.com/YottaDB/Lang/YDBGo)
[![Coverage report](https://gitlab.com/YottaDB/Lang/YDBGo/badges/master/coverage.svg?job=coverage)](https://gitlab.com/YottaDB/Lang/YDBGo/-/jobs)

## Compilation

This package uses pkg-config to find libyottadb.so. The appropriate file is generated by YottaDB r1.23 and greater via ydbinstall (any other installation methods do not install the yottadb.pc file).

If you need to manually generate the yottadb.pc file the contents should look something similar to:

```
prefix=/usr/local/lib/yottadb/r123

exec_prefix=${prefix}
includedir=${prefix}
libdir=${exec_prefix}

Name: YottaDB
Description: YottaDB database library
Version: r1.23
Cflags: -I${includedir}
Libs: -L${libdir} -lyottadb -Wl,-rpath,${libdir}
```

Change the prefix to the correct path for your environment.

NOTE: you cannot use environment variables for the prefix path (e.g. $ydb_dist) it must be a fully qualified path.

You can also override the path used by pkg-config to find yottadb.pc with the environment variable `PKG_CONFIG_PATH` and a path to where the yottadb.pc file resides.

Using the package:

```
go get lang.yottadb.com/go/yottadb
```

Build the package:

```
go build lang.yottadb.com/go/yottadb
```

Before running code using YottaDB or running the YottaDB tests, you need to make sure you have all the needed environment variables configured.
The best way to do this is by sourcing ydb_env_set:

```
source $(pkg-config --variable=prefix yottadb)/ydb_env_set
```

Run the tests:

```
go get -t lang.yottadb.com/go/yottadb
go test lang.yottadb.com/go/yottadb
```

## Developer builds

To use a local development version of YDBGo

```
mkdir -p $GOPATH/src/lang.yottadb.com/go/yottadb
git clone https://gitlab.com/YottaDB/Lang/YDBGo.git $GOPATH/src/lang.yottadb.com/go/yottadb
```
