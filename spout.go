package gostream

import (
	"log"
	"sync"
	"time"
)

type ISpout interface {
	NewInstance() ISpout
	Open(index int, context TopologyContext, collector IOutputCollector)
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

func (t *TopologyBuilder) goSpout(wg *sync.WaitGroup, stop chan bool, id, index int) {
	defer wg.Done()

	log.Printf("goSpout,%s,%d start\n", id, index)
	cc := t.commons[id]
	ispout := t.spouts[id].ispout.NewInstance()

	task := cc.tasks[index]
	ispout.Open(index, task, task)

	lastTs := time.Now().Unix()
	counter := int64(0) //计数器

loop:
	for {
		select {
		case <-stop:
			log.Printf("goSpout id:%s,%d receive stop signal", id, index)
			break loop
		default:
			now := time.Now().Unix()
			if t.statInterval > 0 && now > lastTs+t.statInterval {
				log.Printf("goSpout id:%s,%d speed %d/s", id, index, counter/t.statInterval)
				lastTs = now
				counter = 0
			}
			ispout.NextTuple()
			counter++
		}
	}

	ispout.Close()
	cc.closeDownstream()
	if t.statInterval > 0 {
		log.Printf("goSpout,%d,%d stopped, speed %d/s", id, index, counter/t.statInterval)
	} else {
		log.Printf("goSpout,%d,%d stopped", id, index)
	}
}
