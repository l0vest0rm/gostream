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
    "log"
)

// Queue represents a single instance of the queue data structure.
type Queue struct {
    mu sync.RWMutex
    condEmpty *sync.Cond
    condFull *sync.Cond
    // If Wait is true and the queue is at the maxsize limit, then Get() waits
    // for a element to be returned to the queue before returning.
    wait bool
    closed bool
    head int
    tail int
    count int //actual used num
    buf []interface{}
}

//size must be power of 2
func NewQueue(size int, wait bool) *Queue {
    t := &Queue{}
    t.wait = wait

    maxsize := 1
    for {
        if maxsize < size {
            maxsize = maxsize << 1
        }else {
            break
        }
    }

    t.buf = make([]interface{}, maxsize)
    log.Printf("size:%d,maxsize:%d\n", size, maxsize)
    return t
}

func (t *Queue) Length() int {
    //consider defer low perf
    t.mu.Lock()
    count := t.count
    t.mu.Unlock()
    return count
}

func (t *Queue) Close()  {
    for {
        t.mu.Lock()
        if t.count > 0 {
            //not empty
            t.mu.Unlock()
            continue
        }

        t.closed = true
        if t.condEmpty != nil {
            t.condEmpty.Broadcast()
        }
        t.mu.Unlock()
        return
    }

}

func (t *Queue) Get() interface{} {
    var elem interface{}
    t.mu.Lock()

    for {
        if t.count > 0 {
            // bitwise modulus
            elem = t.buf[t.head & (len(t.buf)-1)]
            t.head = (t.head + 1) & (len(t.buf)-1)
            t.count--

            if t.condFull != nil {
                t.condFull.Signal()
            }

            t.mu.Unlock()
            return elem
        }

        if t.closed {
            t.mu.Unlock()
            return nil
        }

        if !t.wait {
            t.mu.Unlock()
            return nil
        }

        if t.condEmpty == nil {
            t.condEmpty = sync.NewCond(&t.mu)
        }

        t.condEmpty.Wait()
    }
}

func (t *Queue) Append(elem interface{}) bool  {
    t.mu.Lock()

    for {
        if t.count < len(t.buf) {
            //not full
            t.buf[t.tail] = elem
            // bitwise modulus
            t.tail = (t.tail + 1) & (len(t.buf) - 1)
            t.count++

            if t.condEmpty != nil {
                t.condEmpty.Signal()
            }

            t.mu.Unlock()
            return true
        } else {
            //full
            if !t.wait {
                t.mu.Unlock()
                return false
            }

            if t.condFull == nil {
                t.condFull = sync.NewCond(&t.mu)
            }

            t.condFull.Wait()
        }
    }
}