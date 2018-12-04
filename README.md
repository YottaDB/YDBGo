# YottaDB Go Wrapper

[![Go Report Card](https://goreportcard.com/badge/gitlab.com/charles.hathaway/YDBGo?style=flat-square)](https://goreportcard.com/report/gitlab.com/charles.hathaway/YDBGo)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/gitlab.com/charles.hathaway/YDBGo)

## Compilation

Update all references to ``/usr/library/V976/dbg`` to instead point to your YottaDB installation.
``sed`` is your friend.

Link the package to your gopath:

```
ln -s $(pwd) $GOPATH/src/yottadb
```

Build the package:

```
go build yottadb
```

Currently, tests are not working. But in theory, run them with:

```
go test yottadb
```
