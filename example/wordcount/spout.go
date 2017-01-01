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

	return t1
}

func (t *MySpout) Open(index int, context gostream.TopologyContext, collector gostream.IOutputCollector) {
	t.BaseSpout.Open(index, context, collector)
    t.ts = time.Now().Unix()
}

func (t *MySpout) Close() {
    usedTs := time.Now().Unix() - t.ts

	log.Printf("MySpout,index:%d,sum:%d,usedTs:%d, %d/s\n", t.Index, t.sum, usedTs, t.sum/usedTs)
}

func (t *MySpout) NextTuple() {
	word := string(Krand(1, KC_RAND_KIND_LOWER))
	msg := &WordMsg{Key: word}
	t.Collector.Emit(msg)
	//log.Printf("emit word:%s\n", word)
	t.sum += 1
}
