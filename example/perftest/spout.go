/**
 * Copyright 2017  authors
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

// Created by xuning on 2017/1/2

package main

import (
	"log"

	"math/rand"
	"time"

	"github.com/l0vest0rm/gostream"
)

type MyMsg uint64

type MySpout struct {
	*gostream.BaseSpout
	sum int64
	ts  int64
}

func (t MyMsg) GetHashKey(srcPrallelism int, srcIndex int, dstPrallelism int) uint64 {
	return uint64(t) % uint64(dstPrallelism)
}

func (t MyMsg) GetMsgType() int {
	return 0
}

func NewMySpout() gostream.ISpout {
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
	t.Collector.Emit(MyMsg(rand.Int63()))
	t.sum += 1
}
