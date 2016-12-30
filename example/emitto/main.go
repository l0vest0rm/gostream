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
"github.com/l0vest0rm/gostream"
)

const (
    STREAMID1 = "streamid1"
    STREAMID2 = "streamid2"
)

func main() {
    builder := gostream.NewTopologyBuilder()
    builder.SetSpout("randomWords", NewSpout(), 4, 100)
    bolt := builder.SetBolt("wordcount1", NewBolt(), 4, 100)
    bolt.KeyGrouping("randomWords", STREAMID1)
    bolt2 := builder.SetBolt("wordcount2", NewBolt(), 4, 100)
    bolt2.KeyGrouping("randomWords", STREAMID2)
    builder.Run()
}