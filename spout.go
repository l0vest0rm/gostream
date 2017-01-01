package gostream

import (
	"log"
	"sync"
)

type ISpout interface {
	NewInstance() ISpout
	Open(taskid int, context TopologyContext, collector IOutputCollector)
	Close()
	NextTuple()
}

type BaseSpout struct {
	Index     int
	Context   TopologyContext
	Collector IOutputCollector
}

func NewBaseSpout() *BaseSpout {
	t := &BaseSpout{}
	return t
}

func (t *BaseSpout) Copy() *BaseSpout {
	log.Println("BaseSpout NewInstance")
	t1 := &BaseSpout{}
	return t1
}

func (t *BaseSpout) Open(index int, context TopologyContext, collector IOutputCollector) {
	log.Printf("BaseSpout Open,%d", index)
	t.Index = index
	t.Context = context
	t.Collector = collector
}

func (t *BaseSpout) Close() {
	log.Printf("BaseSpout Close,%d", t.Index)
}

func (t *TopologyBuilder) goSpout(wg *sync.WaitGroup, stop chan bool, id string, taskid int) {
	defer wg.Done()

	log.Printf("goSpout,%s,%d start\n", id, taskid)
	cc := t.commons[id]
	ispout := t.spouts[id].ispout.NewInstance()

	ispout.Open(taskid, cc, cc)

loop:
	for {
		select {
		case <-stop:
			log.Printf("goSpout id:%s,%d receive stop signal", id, taskid)
			break loop
		default:
			ispout.NextTuple()
		}
	}

	ispout.Close()
	cc.closeDownstream()
	log.Printf("goSpout,%s,%d stopped", id, taskid)
}
