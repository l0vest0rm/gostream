package main

import (
	"github.com/l0vest0rm/gostream"
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

func main() {

	//debug perf
	runtime.SetBlockProfileRate(1)

	//这里实现了远程获取pprof数据的接口
	go func() {
		http.ListenAndServe(":6666", nil)
	}()

	builder := gostream.NewTopologyBuilder()
	builder.SetSpout("randomWords1", NewSpout(), 4, 100)
	builder.SetSpout("randomWords2", NewSpout(), 4, 100)
	bolt := builder.SetBolt("wordcount", NewBolt(), 4, 100)
	bolt.KeyGrouping("randomWords1", "word")
	bolt.KeyGrouping("randomWords2", "word")
	builder.Run()
}
