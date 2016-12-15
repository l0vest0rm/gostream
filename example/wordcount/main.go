package main

import (
	"git.xiaojukeji.com/QaTools/omega-go/src/utils/gostream"
)

func main() {
	builder := gostream.NewTopologyBuilder()
	builder.SetSpout("randomWords1", NewSpout(), 4, 100)
	builder.SetSpout("randomWords2", NewSpout(), 4, 100)
	bolt := builder.SetBolt("wordcount", NewBolt(), 4, 100)
	bolt.KeyGrouping("randomWords1", "word")
	bolt.KeyGrouping("randomWords2", "word")
	builder.Run()
}
