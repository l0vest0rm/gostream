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

package gostream

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func reader(wg *sync.WaitGroup, mq *Mqueue) {
	defer wg.Done()
	sum := 0

	for {
		message := mq.Get()
		if message == nil {
			break
		}
		//fmt.Println("reader Get")
		sum += 1
	}

	fmt.Printf("reader receive stop signal,sum:%d\n", sum)
}

func writer(wg *sync.WaitGroup, stop chan bool, id int, mq *Mqueue) {
	defer wg.Done()
	sum := 0
	startts := time.Now().Unix()

loop:
	for {
		select {
		case <-stop:
			break loop
		default:
			//fmt.Println("writer Append")
			mq.Append(id, rand.Int63())
			sum += 1
		}
	}

	ts := int(time.Now().Unix() - startts)
	fmt.Printf("writer id:%d receive stop signal,sum:%d,%d/s\n", id, sum, sum/ts)
}

func TestMqueue(t *testing.T) {
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	stop := make(chan bool)

	//监听退出信号
	go goSignalListen(stop)

	rqsize := 10000
	wqsize := 1000
	writers := 4

	mq := NewMqueue(rqsize, wqsize)

	for i := 0; i < writers; i++ {
		mq.RegisterWriter(i)
	}

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go writer(&wg, stop, i, mq)
	}

	wg2.Add(1)
	go reader(&wg2, mq)

	wg.Wait()

	mq.Close()
	wg2.Wait()

}
