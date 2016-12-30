/**
 * Copyright 2016 l0vest0rm.gostream.example.emitto authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"): you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http: *www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

// Created by xuning on 2016/12/30

package main

import (
	"log"
	"math/rand"

	"github.com/l0vest0rm/gostream"
)

type WordMsg struct {
	Key string
}

type MySpout struct {
	*gostream.BaseSpout
	stop chan bool
}

func (t *WordMsg) GetHashKey() interface{} {
	return t.Key
}

func (t *WordMsg) GetMsgType() int {
	return 0
}

func goRandomWords(stop chan bool, messages chan<- interface{}) {
	var word string
	for {
		select {
		case <-stop:
			log.Println("goRandomWords receive stop signal")
			return
		default:
			id := rand.Int() % 2
			if id == 0 {
				word = STREAMID1
			} else {
				word = STREAMID2
			}
			messages <- word
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
	go goRandomWords(t.stop, messages)
}

func (t *MySpout) Close() {
	close(t.stop)
}

func (t *MySpout) Execute(message interface{}) {
	word := message.(string)
	msg := &WordMsg{Key: word}
	t.Collector.EmitTo(msg, word)
	//log.Printf("emit word:%s\n", word)
}
