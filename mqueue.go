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
)


type Queue2 struct {
    index uint64
    count int //actual used num
    buf []interface{}
}

// Mqueue represents a single reader and multi writer queue data structure.
type Mqueue struct {
    mu sync.Mutex
    rw sync.RWMutex
    rqMask uint64
    wqMask uint64
    notEmpty *sync.Cond
    notFull *sync.Cond
    closed bool
    rq *Queue2 //read queue
    eq *Queue2 //exchange queue
    wq map[int]*Queue2 //write queue, map[writerId]
}

func alignPower2(size int) uint64 {
    maxsize := 1
    for {
        if maxsize < size {
            maxsize = maxsize << 1
        }else {
            break
        }
    }
    return uint64(maxsize)
}

//size must be power of 2
func NewMqueue(qsize int) *Mqueue {
    rqMask := alignPower2(qsize) -1
    wqMask := alignPower2(int(rqMask+1)/4) -1

    t := &Mqueue{rqMask: rqMask, wqMask: wqMask, rq : &Queue2{buf : make([]interface{}, rqMask + 1)},
        eq : &Queue2{buf : make([]interface{}, rqMask + 1)},
        wq : make(map[int]*Queue2)}

    return t
}

func (t *Mqueue) Close()  {
    t.mu.Lock()

    t.closed = true
    if t.notEmpty != nil {
        t.notEmpty.Broadcast()
    }

    t.mu.Unlock()
}

func (t *Mqueue) RegisterWriter(writerId int)  {
    t.rw.Lock()
    t.wq[writerId] = &Queue2{buf : make([]interface{}, t.wqMask + 1)}
    t.rw.Unlock()
}

func (t *Mqueue) Get() interface{} {
    var elem interface{}

    var rq *Queue2

    for {
        rq = t.rq
        if rq.count > 0 {
            // bitwise modulus
            elem = rq.buf[rq.index & t.rqMask]
            rq.index += 1
            rq.count--

            return elem
        }

        //swith queue
        t.mu.Lock()

        queue_switch:
        if t.eq.count > 0 {
            rq.index = 0
            t.rq = t.eq
            t.eq = rq

            if t.notFull != nil {
                //fmt.Println("notFull.Signal()")
                t.notFull.Signal()
            }

            t.mu.Unlock()

            continue
        }

        if t.closed {
            t.mu.Unlock()
            return nil
        }

        if t.notEmpty == nil {
            t.notEmpty = sync.NewCond(&t.mu)
        }

        //fmt.Println("notEmpty.Wait()")
        t.notEmpty.Wait()
        //fmt.Println("notEmpty.recover()")
        goto queue_switch
    }
}

func (t *Mqueue) Append(writerId int, elem interface{})  {
    t.rw.RLock()
    wq, ok := t.wq[writerId]
    t.rw.RUnlock()
    if !ok {
        t.RegisterWriter(writerId)
        wq = t.wq[writerId]
    }

    for {
        if wq.count < len(wq.buf) {
            //not full
            wq.buf[wq.index & t.wqMask] = elem
            // bitwise modulus
            wq.index += 1
            wq.count++

            return
        }

        //copyt to exchange queue
        t.mu.Lock()

        copy_to_exchange_queue:
        if t.eq.count + wq.count <= len(t.eq.buf) {
            //copy(t.eq.buf[t.eq.count:], wq.buf)
            for i := 0; i < len(wq.buf);i++ {
                t.eq.buf[t.eq.count + i] = wq.buf[i]
            }

            t.eq.count += wq.count

            if t.notEmpty != nil {
                //fmt.Println("notEmpty.Signal()")
                t.notEmpty.Signal()
            }

            t.mu.Unlock()

            wq.index = 0

            continue
        }

        //full
        if t.notFull == nil {
            t.notFull = sync.NewCond(&t.mu)
        }

        //fmt.Println("notFull.Wait()")
        t.notFull.Wait()
        //fmt.Println("notFull.recover()")
        goto copy_to_exchange_queue
    }
}