
Build command for debug, so can be started in IntelliJ in debug mode.
```
go build -gcflags "all=-N -l" -buildmode=plugin ../mrapps/wc.go

go run mrmaster.go pg-*.txt

go run -gcflags "all=-N -l" mrworker.go wc.so

sh test-mr.sh
```