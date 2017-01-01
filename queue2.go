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
    //"fmt"
    "sync/atomic"
    "runtime"
)

// RwQueue represents a single instance of the queue data structure.
type LockFreeQueue2 struct {
    // The padding members 1 to 5 below are here to ensure each item is on a separate cache line.
    // This prevents false sharing and hence improves performance.
    padding1 [8]uint64
    readIndex uint64
    padding2 [8]uint64
    writeIndex uint64
    padding3 [8]uint64
    indexMask uint64
    padding4 [8]uint64
    buf []interface{}
    padding5 [8]uint64
    closed bool
}

//size must be power of 2
func NewLockFreeQueue2(size int) *LockFreeQueue2 {
    qsize := 1
    for {
        if qsize < size {
            qsize = qsize << 1
        }else {
            break
        }
    }

    t := &LockFreeQueue2{readIndex : 1, writeIndex : 1, indexMask: uint64(qsize-1), buf : make([]interface{}, qsize)}

    return t
}

func (t *LockFreeQueue2) Close()  {
    t.closed = true
}

func (t *LockFreeQueue2) Get() interface{} {
    var myIndex = atomic.AddUint64(&t.readIndex, 1) - 1

    //If reader has out-run writer, wait for a value to be committed
    for myIndex > (t.writeIndex - 1) {
        //dead loop
        if t.closed {
            return nil
        }
        runtime.Gosched()
    }

    return t.buf[myIndex & t.indexMask]
}

func (t *LockFreeQueue2) Append(elem interface{})  {
    var myIndex = atomic.AddUint64(&t.writeIndex, 1) - 1
    //Wait for reader to catch up, so we don't clobber a slot which it is (or will be) reading
    for myIndex > (t.readIndex + t.indexMask) {
        //dead loop
        runtime.Gosched()
    }
    //Write the item into it's slot
    t.buf[myIndex & t.indexMask] = elem
}