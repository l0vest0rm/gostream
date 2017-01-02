package gostream

import (
	"log"
	"sync"
)

type IBolt interface {
	NewInstance() IBolt
	Prepare(index int, context TopologyContext, collector IOutputCollector)
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

func (t *TopologyBuilder) goBolt(wg *sync.WaitGroup, id string, index int) {
	defer wg.Done()
	log.Printf("goBolt,%s,%d start\n", id, index)
	cc := t.commons[id]
	ibolt := t.bolts[id].ibolt.NewInstance()

	task := cc.tasks[index]
	ibolt.Prepare(index, task, task)

	//var message Message
	//var more bool
	var message interface{}

loop:
	for {
		//message, more = <- cc.tasks[index].messages
		message = cc.tasks[index].messages.Get()
		if message == nil {
			//no more message
			log.Printf("goBolt id:%s,%d receive stop signal", id, index)
			break loop
		}

		ibolt.Execute(message.(Message))
	}

	ibolt.Cleanup()
	cc.closeDownstream()
	log.Printf("goBolt,%s,%d stopped", id, index)
}
