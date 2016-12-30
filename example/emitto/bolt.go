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

	"github.com/l0vest0rm/gostream"
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
	log.Printf("Cleanup,boltid:%s,index:%d\n", t.Context.GetThisComponentId(), t.Index)
	t.PrintResult()
}

func (t *MyBolt) Execute(message gostream.Message) {
	msg := message.(*WordMsg)
	cnt, ok := t.wordMap[msg.Key]
	if ok {
		t.wordMap[msg.Key] = cnt + 1
		//log.Printf("%s,%d", msg.Key, cnt+1)
	} else {
		t.wordMap[msg.Key] = 1
	}
}

func (t *MyBolt) PrintResult() {
	for key, value := range t.wordMap {
		log.Printf("word:%s,cnt:%d\n", key, value)
	}

}
