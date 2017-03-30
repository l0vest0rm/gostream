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
	"os"

	"strconv"

	"fmt"

	"github.com/l0vest0rm/gostream"
)

const (
	componentIDMySpout = 1
	componentIDMyBolt  = 2
	streamIDDefault    = 0
)

func main() {
	myIdx, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(fmt.Sprintf("wrong os.Args:%v", os.Args))
	}
	addrs := []string{"127.0.0.1:9001", "127.0.0.1:9002"}
	builder := gostream.NewTopologyDistBuilder(addrs, myIdx, 10000)
	builder.SetSpout(componentIDMySpout, NewMySpout(), 4)
	bolt := builder.SetBolt(componentIDMyBolt, NewMyBolt(), 4, 1000)
	bolt.KeyGrouping(componentIDMySpout, streamIDDefault)
	builder.Run()
}
