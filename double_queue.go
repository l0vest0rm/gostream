/**
 * Copyright 2016  authors
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

// Created by xuning on 2016/12/31

package gostream

import (
    "sync"
    //"fmt"
)

type Queue struct {
    head int
    tail int
    count int //actual used num
    buf []interface{}
}

// DoubleQueue represents a single instance of the queue data structure.
type DoubleQueue struct {
    rlock sync.RWMutex
    wlock sync.RWMutex
    notEmpty *sync.Cond
    notFull *sync.Cond
    closed bool
    rq *Queue
    wq *Queue
}

//size must be power of 2
func NewDoubleQueue(size int) *DoubleQueue {
    maxsize := 1
    for {
        if maxsize < size {
            maxsize = maxsize << 1
        }else {
            break
        }
    }

    t := &DoubleQueue{rq : &Queue{buf : make([]interface{}, maxsize)},
        wq : &Queue{buf : make([]interface{}, maxsize)}}

    return t
}

func (t *DoubleQueue) Close()  {
    t.wlock.Lock()

    t.closed = true
    if t.notEmpty != nil {
        t.notEmpty.Broadcast()
    }

    t.wlock.Unlock()
}

func (t *DoubleQueue) Get() interface{} {
    var elem interface{}
    t.rlock.Lock()

    var rq *Queue

    for {
        rq = t.rq
        if rq.count > 0 {
            // bitwise modulus
            elem = rq.buf[rq.head & (len(rq.buf)-1)]
            rq.head = (rq.head + 1) & (len(rq.buf)-1)
            rq.count--

            t.rlock.Unlock()
            return elem
        }

        //swith queue
        t.wlock.Lock()
        if t.wq.count > 0 {
            t.rq = t.wq
            t.wq = rq

            if t.notFull != nil {
                //fmt.Println("notFull.Signal()")
                t.notFull.Signal()
            }

            t.wlock.Unlock()

            continue
        }

        if t.closed {
            t.wlock.Unlock()
            t.rlock.Unlock()
            return nil
        }

        //fmt.Println("notEmpty.Wait()")
        if t.notEmpty == nil {
            t.notEmpty = sync.NewCond(&t.wlock)
        }

        t.notEmpty.Wait()
        t.wlock.Unlock()
        //fmt.Println("notEmpty.recover()")
    }
}

func (t *DoubleQueue) Append(elem interface{})  {
    t.wlock.Lock()
    var wq *Queue

    for {
        wq = t.wq
        if wq.count < len(wq.buf) {
            //not full
            wq.buf[wq.tail] = elem
            // bitwise modulus
            wq.tail = (wq.tail + 1) & (len(wq.buf) - 1)
            wq.count++

            if wq.count == 1 {
                if t.notEmpty != nil {
                    //fmt.Println("notEmpty.Signal()")
                    t.notEmpty.Signal()
                }
            }

            t.wlock.Unlock()
            return
        }

        //fmt.Println("notFull.Wait()")
        //full
        if t.notFull == nil {
            t.notFull = sync.NewCond(&t.wlock)
        }

        t.notFull.Wait()
        //fmt.Println("notFull.recover()")
    }
}