package gostream

import (
	"log"
	"sync"
	"time"
)

type IBolt interface {
	NewInstance() IBolt
	Prepare(index int, context TopologyContext, collector IOutputCollector)
	Execute(message Message)
	ExecuteB(message []byte)
	Cleanup()
}

type BaseBolt struct {
	Index     int
	Context   TopologyContext
	Collector IOutputCollector
}

func NewBaseBolt() *BaseBolt {
	t := &BaseBolt{}
	return t
}

func (t *BaseBolt) Copy() *BaseBolt {
	log.Println("BoltBase NewInstance")
	t1 := &BaseBolt{}
	return t1
}

func (t *BaseBolt) Prepare(index int, context TopologyContext, collector IOutputCollector) {
	log.Printf("BaseBolt Prepare,%d", index)
	t.Index = index
	t.Context = context
	t.Collector = collector
}

func (t *BaseBolt) Cleanup() {
	log.Printf("BaseBolt Cleanup,%d", t.Index)
}

func (t *BaseBolt) Execute(message Message) {
}

func (t *TopologyBuilder) goBolt(wg *sync.WaitGroup, id, index int) {
	defer wg.Done()
	log.Printf("goBolt,%s,%d start\n", id, index)
	cc := t.commons[id]
	ibolt := t.bolts[id].ibolt.NewInstance()

	task := cc.tasks[index]
	ibolt.Prepare(index, task, task)

	var message Message
	var more bool
	lastTs := time.Now().Unix()
	counter := int64(0) //计数器

loop:
	for {
		message, more = <-cc.tasks[index].messages
		if !more {
			//no more message
			log.Printf("goBolt id:%s,%d receive stop signal", id, index)
			break loop
		}

		now := time.Now().Unix()
		if t.statInterval > 0 && now > lastTs+t.statInterval {
			log.Printf("goSpout id:%s,%d speed %d/s", id, index, counter/t.statInterval)
			lastTs = now
			counter = 0
		}
		ibolt.Execute(message)
		counter++
	}

	ibolt.Cleanup()
	cc.closeDownstream()
	if t.statInterval > 0 {
		log.Printf("goBolt,%d,%d stopped, speed %d/s", id, index, counter/t.statInterval)
	} else {
		log.Printf("goBolt,%d,%d stopped", id, index)
	}
}
