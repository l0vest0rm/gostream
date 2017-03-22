package main

import (
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/l0vest0rm/gostream"
)

func main() {

	//debug perf
	runtime.SetBlockProfileRate(1)

	//这里实现了远程获取pprof数据的接口
	go func() {
		http.ListenAndServe(":6666", nil)
	}()

	builder := gostream.NewTopologyBuilder()
	builder.SetSpout("randomWords1", NewSpout(), 2)
	bolt := builder.SetBolt("wordcount", NewBolt(), 4, 1000)
	bolt.KeyGrouping("randomWords1", "word")
	builder.Run()
}
