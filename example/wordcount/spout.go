package main

import (
	"log"

	"github.com/l0vest0rm/gostream"
    "time"
)

type WordMsg struct {
	Key string
}

type MySpout struct {
	*gostream.BaseSpout
	stop chan bool
	sum  int64
    ts int64
}

func (t *WordMsg) GetHashKey() interface{} {
	return t.Key
}

func (t *WordMsg) GetMsgType() int {
	return 0
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
    t.ts = time.Now().Unix()
    go goRandomWords(t.stop, messages)
}

func (t *MySpout) Close() {
    usedTs := time.Now().Unix() - t.ts
    close(t.stop)

	log.Printf("MySpout,index:%d,sum:%d,usedTs:%d, %d/s\n", t.Index, t.sum, usedTs, t.sum/usedTs)
}

func (t *MySpout) Execute(message interface{}) {
	word := message.(string)
	msg := &WordMsg{Key: word}
	t.Collector.Emit(msg)
	//log.Printf("emit word:%s\n", word)
	t.sum += 1
}
