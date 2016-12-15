package gostream

import (
	"log"
	"sync"
	"time"
)

type ISpout interface {
	NewInstance() ISpout
	Open(taskid int, context TopologyContext, collector IOutputCollector, messages chan<- interface{})
	Close()
	Execute(message interface{})
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

func (t *BaseSpout) Open(index int, context TopologyContext, collector IOutputCollector, messages chan<- interface{}) {
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

	messages := make(chan interface{}, t.spouts[id].bufSize)
	ispout.Open(taskid, cc, cc, messages)

loop:
	for {
		select {
		case <-stop:
			log.Printf("goSpout id:%s,%d receive stop signal", id, taskid)
			break loop
		case message := <-messages:
			ispout.Execute(message)
		default:
			time.Sleep(1e9)
		}
	}

	ispout.Close()
	cc.closeDownstream()
	log.Printf("goSpout,%s,%d stopped", id, taskid)
}
