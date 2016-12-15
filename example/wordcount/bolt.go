package main

import (
	"github.com/l0vest0rm/gostream"
	"log"
)

type MyBolt struct {
	*gostream.BaseBolt
	wordMap map[string]int
}

func NewBolt() gostream.IBolt {
	t := &MyBolt{}
	t.BaseBolt = gostream.NewBaseBolt()
	return t
}

func (t *MyBolt) NewInstance() gostream.IBolt {
	t1 := &MyBolt{}
	t1.BaseBolt = t.BaseBolt.Copy()

	return t1
}

func (t *MyBolt) Prepare(index int, context gostream.TopologyContext, collector gostream.IOutputCollector) {
	t.BaseBolt.Prepare(index, context, collector)
	t.wordMap = make(map[string]int)
}

func (t *MyBolt) Cleanup() {
	t.PrintResult()
}

func (t *MyBolt) Execute(message gostream.Message) {
	msg := message.(*WordMsg)
	cnt, ok := t.wordMap[msg.Key]
	if ok {
		t.wordMap[msg.Key] = cnt + 1
		//log.Printf("%s,%d", word, cnt+1)
	} else {
		t.wordMap[msg.Key] = 1
	}
}

func (t *MyBolt) PrintResult() {
	for key, value := range t.wordMap {
		log.Printf("word:%s,cnt:%d\n", key, value)
	}

}
