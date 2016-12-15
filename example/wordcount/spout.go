package main

import (
	"log"

	"git.xiaojukeji.com/QaTools/omega-go/src/utils/gostream"
)

type WordMsg struct {
	Key string
}

type MySpout struct {
	*gostream.BaseSpout
	stop chan bool
}

func goRandomWords(stop chan bool, messages chan<- interface{}) {
	for {
		select {
		case <-stop:
			log.Println("goRandomWords receive stop signal")
			return
		default:
			b := Krand(1, KC_RAND_KIND_LOWER)
			messages <- string(b)
		}
	}
}

func (t *WordMsg) GetHashKey() interface{} {
	return t.Key
}

func NewSpout() gostream.ISpout {
	t := &MySpout{}
	t.BaseSpout = gostream.NewBaseSpout()
	return t
}

func (t *MySpout) NewInstance() gostream.ISpout {
	t1 := &MySpout{}
	t1.BaseSpout = t.BaseSpout.Copy()
	t1.stop = make(chan bool)

	return t1
}

func (t *MySpout) Open(index int, context gostream.TopologyContext, collector gostream.IOutputCollector, messages chan<- interface{}) {
	t.BaseSpout.Open(index, context, collector, messages)
	go goRandomWords(t.stop, messages)
}

func (t *MySpout) Close() {
	close(t.stop)
}

func (t *MySpout) Execute(message interface{}) {
	word := message.(string)
	msg := &WordMsg{Key: word}
	t.Collector.Emit(msg)
	//log.Printf("emit word:%s\n", word)
}
