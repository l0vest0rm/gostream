package gostream

import (
	"log"
	"sync"
)

type IBolt interface {
	NewInstance() IBolt
	Prepare(taskid int, context TopologyContext, collector IOutputCollector)
	Execute(message Message)
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

func (t *TopologyBuilder) goBolt(wg *sync.WaitGroup, id string, taskid int) {
	defer wg.Done()
	log.Printf("goBolt,%s,%d start\n", id, taskid)
	cc := t.commons[id]
	ibolt := t.bolts[id].ibolt.NewInstance()
	ibolt.Prepare(taskid, cc, cc)

    var message interface{}

loop:
	for {
		message = cc.tasks[taskid].queue.Get()
        if message == nil {
            //no more message
            break loop
        }

        ibolt.Execute(message.(Message))
	}

	ibolt.Cleanup()
	cc.closeDownstream()
	log.Printf("goBolt,%s,%d stopped", id, taskid)
}
