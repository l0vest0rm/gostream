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
type LockFreeQueue struct {
    // The padding members 1 to 6 below are here to ensure each item is on a separate cache line.
    // This prevents false sharing and hence improves performance.
    padding1 [8]uint64
    lastCommittedIndex uint64
    padding2 [8]uint64
    nextFreeIndex uint64
    padding3 [8]uint64
    readerIndex uint64
    padding4 [8]uint64
    indexMask uint64
    padding5 [8]uint64
    buf []interface{}
    padding6 [8]uint64
    closed bool
}

//size must be power of 2
func NewLockFreeQueue(size int) *LockFreeQueue {
    qsize := 1
    for {
        if qsize < size {
            qsize = qsize << 1
        }else {
            break
        }
    }

    t := &LockFreeQueue{lastCommittedIndex : 0, nextFreeIndex : 1, readerIndex : 1, indexMask: uint64(qsize-1), buf : make([]interface{}, qsize)}

    return t
}

func (t *LockFreeQueue) Close()  {
    t.closed = true
}

func (t *LockFreeQueue) Get() interface{} {
    var myIndex = atomic.AddUint64(&t.readerIndex, 1) - 1

    //If reader has out-run writer, wait for a value to be committed
    for myIndex > t.lastCommittedIndex {
        //dead loop
        if t.closed {
            return nil
        }
        runtime.Gosched()
    }

    return t.buf[myIndex & t.indexMask]
}

func (t *LockFreeQueue) Append(elem interface{})  {
    var myIndex = atomic.AddUint64(&t.nextFreeIndex, 1) - 1
    //Wait for reader to catch up, so we don't clobber a slot which it is (or will be) reading
    for myIndex > (t.readerIndex + t.indexMask - 1) {
        //dead loop
        runtime.Gosched()
    }
    //Write the item into it's slot
    t.buf[myIndex & t.indexMask] = elem
    //Increment the lastCommittedIndex so the item is available for reading
    for !atomic.CompareAndSwapUint64(&t.lastCommittedIndex, myIndex - 1, myIndex) {
        //dead loop
        runtime.Gosched()
    }
}