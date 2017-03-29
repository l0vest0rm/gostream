package gostream

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/l0vest0rm/gostream/rpc"
)

const (
	GROUPING_SHUFFLE = 1
	GROUPING_KEY     = 2
)

type Spout struct {
	cc     *ComponentCommon
	ispout ISpout
}

type Bolt struct {
	cc    *ComponentCommon
	ibolt IBolt
}

type TaskInfo struct {
	cc           *ComponentCommon
	componentID  int
	index        int //本component内的索引
	dependentCnt int //依赖messages的上游发送者的数目
	messages     chan Message
}

//目标下游信息
type targetInfo struct {
	peerIdx     int //peer process index
	componentID int
	index       int //本component内的索引
}

type StreamInfo struct {
	groupingType int
	targets      []*targetInfo
}

type ComponentCommon struct {
	id          int
	tb          *TopologyBuilder
	parallelism int
	tasks       []*TaskInfo
	//inputs       map[string]string      //输入map[componentID]streamId
	streams []*StreamInfo //输出
}

type GroupInfo struct {
	PeerIdx        int //my index
	RcvID          int //receiver component id
	RcvParallelism int // receiver component's parallelism
	SndID          int //sender component id
	StreamID       int //sender streamId
	GroupingType   int
	isLocal        bool //wether inner process grouping
}

type distInfo struct {
	mu     sync.RWMutex
	addr   string // addr
	client *rpc.ServiceClient
}

// TopologyBuilder topo struct
type TopologyBuilder struct {
	statInterval  int64 //统计reset周期，单位秒
	mu            sync.RWMutex
	spouts        map[int]*Spout
	bolts         map[int]*Bolt
	commons       map[int]*ComponentCommon
	pendGroupings []*GroupInfo
	dist          []*distInfo //all distributed peers
	myIdx         int         //my addr idx
	ready         bool        //ready for receiving messages
}

type IOutputCollector interface {
	Emit(message Message)
	EmitTo(message Message, streamid int)
}

type TopologyContext interface {
	GetThisComponentID() int
}

func (t *TaskInfo) GetThisComponentID() int {
	return t.componentID
}

func (t *TaskInfo) Emit(message Message) {
	var tg *targetInfo
	cc := t.cc
	if cc.streams == nil || len(cc.streams) == 0 {
		return
	}

	streamInfo := cc.streams[len(cc.streams)-1]
	l := len(streamInfo.targets)
	if l > 1 {
		switch streamInfo.groupingType {
		case GROUPING_SHUFFLE:
			tg = streamInfo.targets[rand.Intn(l)]
		case GROUPING_KEY:
			hashid := message.GetHashKey(cc.parallelism, t.index, l)
			tg = streamInfo.targets[hashid]
		default:
			log.Fatalf("unknown groupingType:%d\n", streamInfo.groupingType)
			return
		}
	} else {
		tg = streamInfo.targets[0]
	}

	cc.tb.EmitMessage(message, tg)
}

func (t *TaskInfo) EmitTo(message Message, streamID int) {
	var tg *targetInfo
	cc := t.cc
	streamInfo := t.cc.streams[streamID]
	l := len(streamInfo.targets)
	if l > 1 {
		switch streamInfo.groupingType {
		case GROUPING_SHUFFLE:
			tg = streamInfo.targets[rand.Intn(l)]
		case GROUPING_KEY:
			hashid := message.GetHashKey(cc.parallelism, t.index, l)
			tg = streamInfo.targets[hashid]
		default:
			log.Fatalf("unknown groupingType:%d\n", streamInfo.groupingType)
			return
		}
	} else {
		tg = streamInfo.targets[0]
	}

	cc.tb.EmitMessage(message, tg)
}

func (t *TopologyBuilder) EmitMessage(message Message, tg *targetInfo) {
	if t.myIdx == tg.peerIdx {
		//local Emit
		t.commons[tg.componentID].tasks[tg.index].messages <- message
		return
	}

	//remote
	b, err := message.Marshal()
	if err != nil {
		log.Printf("EmitMessage,message.Marshal,err:%s\n", err.Error())
		return
	}

	dataR := &rpc.DataRequest{RcvID: int32(tg.componentID),
		RcvIdx: int32(tg.index),
		Data:   b}
	t.dist[tg.peerIdx].client.SendData(dataR)
}

//关闭下游
func (t *ComponentCommon) closeDownstream() {
	for _, streamInfo := range t.streams {
		for _, tg := range streamInfo.targets {
			if tg.peerIdx == t.tb.myIdx {
				//local
				t.tb.mu.Lock()
				t.tb.commons[tg.componentID].tasks[tg.index].dependentCnt--
				if t.tb.commons[tg.componentID].tasks[tg.index].dependentCnt == 0 {
					log.Printf("close channel,componentID:%d,taskid:%d\n", tg.componentID, tg.index)
					close(t.tb.commons[tg.componentID].tasks[tg.index].messages)
				}
				t.tb.mu.Unlock()
			}
		}
	}
}

// ShuffleGrouping grouping messages random
func (t *Bolt) ShuffleGrouping(componentID, streamID int) {
	if t.cc.tb.dist == nil {
		t.cc.tb.pendGrouping(t.cc.id, t.cc.parallelism, componentID, streamID, GROUPING_SHUFFLE, true)
	} else {
		t.cc.tb.pendGrouping(t.cc.id, t.cc.parallelism, componentID, streamID, GROUPING_SHUFFLE, false)
	}
}

//KeyGrouping grouping messages by key
func (t *Bolt) KeyGrouping(componentID, streamID int) {
	if t.cc.tb.dist == nil {
		t.cc.tb.pendGrouping(t.cc.id, t.cc.parallelism, componentID, streamID, GROUPING_KEY, true)
	} else {
		t.cc.tb.pendGrouping(t.cc.id, t.cc.parallelism, componentID, streamID, GROUPING_KEY, false)
	}
}

// ShuffleGroupingLocal grouping messages random local
func (t *Bolt) ShuffleGroupingLocal(componentID, streamID int) {
	t.cc.tb.pendGrouping(t.cc.id, t.cc.parallelism, componentID, streamID, GROUPING_SHUFFLE, true)
}

//KeyGroupingLocal grouping messages by key local
func (t *Bolt) KeyGroupingLocal(componentID, streamID int) {
	t.cc.tb.pendGrouping(t.cc.id, t.cc.parallelism, componentID, streamID, GROUPING_KEY, true)
}

func (t *TopologyBuilder) pendGrouping(rcvID, rcvParallelism, sndID, streamID int, groupingType int, isLocal bool) {
	gi := &GroupInfo{}
	gi.RcvID = rcvID
	gi.RcvParallelism = rcvParallelism
	gi.SndID = sndID
	gi.StreamID = streamID
	gi.GroupingType = groupingType
	gi.isLocal = isLocal
	gi.PeerIdx = t.myIdx
	t.pendGroupings = append(t.pendGroupings, gi)
}

// NewTopologyBuilder new one
func NewTopologyBuilder() *TopologyBuilder {
	tb := &TopologyBuilder{}
	tb.commons = make(map[int]*ComponentCommon)
	tb.pendGroupings = make([]*GroupInfo, 0)
	return tb
}

// NewTopologyDistBuilder new one
func NewTopologyDistBuilder(addrs []string, myIdx int) *TopologyBuilder {
	tb := NewTopologyBuilder()
	if addrs == nil || len(addrs) < 2 {
		return tb
	}

	//distributed
	tb.myIdx = myIdx
	tb.dist = make([]*distInfo, 0, len(addrs))
	for i := 0; i < len(addrs); i++ {
		di := &distInfo{addr: addrs[i]}
		tb.dist = append(tb.dist, di)
	}

	return tb
}

func (t *TopologyBuilder) SetSpout(id int, ispout ISpout, parallelism int) *Spout {
	if _, ok := t.commons[id]; ok {
		panic("SetSpout,id exist")
	}

	if t.spouts == nil {
		t.spouts = make(map[int]*Spout)
	}

	//先不考虑重复的问题
	cc := &ComponentCommon{}
	cc.id = id
	cc.parallelism = parallelism
	cc.tb = t
	cc.tasks = make([]*TaskInfo, 0, parallelism)
	for i := 0; i < parallelism; i++ {
		task := &TaskInfo{}
		task.componentID = id
		task.index = i
		task.cc = cc
		cc.tasks = append(cc.tasks, task)
	}
	t.commons[id] = cc

	spout := &Spout{}
	spout.cc = cc
	spout.ispout = ispout
	t.spouts[id] = spout

	return spout
}

func (t *TopologyBuilder) SetBolt(id int, ibolt IBolt, parallelism int, bufSize int) *Bolt {
	if t.bolts == nil {
		t.bolts = make(map[int]*Bolt)
	}

	cc := &ComponentCommon{}
	cc.id = id
	cc.parallelism = parallelism
	cc.tb = t
	cc.tasks = make([]*TaskInfo, 0, parallelism)
	for i := 0; i < parallelism; i++ {
		task := &TaskInfo{}
		task.componentID = id
		task.index = i
		task.cc = cc
		task.messages = make(chan Message, bufSize) //缓冲设置
		cc.tasks = append(cc.tasks, task)
	}

	t.commons[id] = cc

	bolt := &Bolt{}
	bolt.cc = cc
	bolt.ibolt = ibolt
	t.bolts[id] = bolt

	return bolt
}

//设置数据统计周期，即多少秒输出一次统计数据(0表示不周期输出)
func (t *TopologyBuilder) SetStatistics(statInterval int64) {
	t.statInterval = statInterval
}

func goSignalListen(stop chan bool) {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)
	select {
	case <-c:
		close(stop)
		log.Println("SignalListen closed stop chan")
	}
}

func (t *TopologyBuilder) startSpouts(wg *sync.WaitGroup, stop chan bool) {
	for id := range t.spouts {
		for i := 0; i < t.commons[id].parallelism; i++ {
			wg.Add(1)
			go t.goSpout(wg, stop, id, i)
		}
	}
}

func (t *TopologyBuilder) startBolts(wg *sync.WaitGroup) {
	for id := range t.bolts {
		for i := 0; i < t.commons[id].parallelism; i++ {
			wg.Add(1)
			go t.goBolt(wg, id, i)
		}
	}
}

func (t *TopologyBuilder) grouping(gi *GroupInfo) {
	log.Println("grouping")
	t.groupingLocal(gi)
	if !gi.isLocal {
		t.groupingRemote(gi)
	}
}

func (t *TopologyBuilder) groupingRemote(gi *GroupInfo) {
	log.Println("groupingRemote")
	var err error
	var reply int32

	req := &rpc.GroupingRequest{PeerIdx: int32(gi.PeerIdx),
		RcvID:          int32(gi.RcvID),
		RcvParallelism: int32(gi.RcvParallelism),
		SndID:          int32(gi.SndID)}
	for i := 0; i < len(t.dist); i++ {
		//loop for client connected
		for {
			if i == t.myIdx {
				break
			}

			t.dist[i].mu.Lock()
			reply, err = t.dist[i].client.Grouping(req)
			t.dist[i].mu.Unlock()
			if err != nil {
				panic(fmt.Sprintf("groupingRemote,call,err:%s", err.Error()))
			}
			for i := 0; i < t.commons[gi.RcvID].parallelism; i++ {
				t.commons[gi.RcvID].tasks[i].dependentCnt += int(reply)
			}
			log.Printf("groupingRemote,success,gi:%v", gi)
			break
		}
	}
}

func (t *TopologyBuilder) groupingLocal(gi *GroupInfo) {
	log.Println("groupingLocal")
	if t.commons[gi.SndID].streams == nil {
		t.commons[gi.SndID].streams = make([]*StreamInfo, 0, gi.StreamID+1)
	}

	//extend the slice
	for i := len(t.commons[gi.SndID].streams); i < gi.StreamID+1; i++ {
		si := &StreamInfo{}
		si.targets = make([]*targetInfo, 0, gi.RcvParallelism)
		t.commons[gi.SndID].streams = append(t.commons[gi.SndID].streams, si)
	}

	t.commons[gi.SndID].streams[gi.StreamID].groupingType = gi.GroupingType
	for i := 0; i < gi.RcvParallelism; i++ {
		tg := &targetInfo{}
		tg.peerIdx = gi.PeerIdx
		tg.componentID = gi.RcvID
		tg.index = i
		t.commons[gi.SndID].streams[gi.StreamID].targets = append(t.commons[gi.SndID].streams[gi.StreamID].targets, tg)
		t.commons[gi.RcvID].tasks[i].dependentCnt += t.commons[gi.SndID].parallelism
	}
}

func (t *TopologyBuilder) dealPendGroupings() {
	for _, gi := range t.pendGroupings {
		t.grouping(gi)
	}
}

func (t *TopologyBuilder) goStartServer(wg *sync.WaitGroup, stop chan bool, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) {
	defer wg.Done()

	if t.dist == nil {
		return
	}

	//connect peers
	for i := 0; i < len(t.dist); i++ {
		if i == t.myIdx {
			continue
		}

		wg.Add(1)
		go t.connectPeer(wg, stop, i, transportFactory, protocolFactory)
	}

	//var transport thrift.TServerTransport
	transport, err := thrift.NewTServerSocket(t.dist[t.myIdx].addr)
	if err != nil {
		panic(fmt.Sprintf("NewTServerSocket,err:%s\n", err.Error()))
	}
	handler := NewServiceHandler(t)
	processor := rpc.NewServiceProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)
	log.Println("Starting the simple server... on ", t.dist[t.myIdx].addr)
	server.Serve()
}

// connectPeer connect other peer
func (t *TopologyBuilder) connectPeer(wg *sync.WaitGroup, stop chan bool, peerIdx int, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) {
	defer wg.Done()

	var transport thrift.TTransport
	log.Printf("connectPeer,peerIdx:%d\n", peerIdx)
	var err error
	for {
		transport, err = thrift.NewTSocket(t.dist[peerIdx].addr)
		transport = transportFactory.GetTransport(transport)
		if err := transport.Open(); err == nil {
			// ok
			break
		}
		log.Printf("connectPeer,addr:%s,err:%s\n", t.dist[peerIdx].addr, err.Error())
		time.Sleep(time.Second)
	}

	client := rpc.NewServiceClientFactory(transport, protocolFactory)
	if client == nil {
		//handle error
		log.Printf("connectPeer,addr:%s,NewClient,err:%s\n", t.dist[peerIdx].addr, err.Error())
	}

	t.dist[peerIdx].mu.Lock()
	t.dist[peerIdx].client = client
	t.dist[peerIdx].mu.Unlock()
}

//distributed peers sync
func (t *TopologyBuilder) distSync() {
	t.mu.Lock()
	t.ready = true
	t.mu.Unlock()
	if t.dist == nil {
		return
	}

	//check peers's ready or not
	var err error
	var ready bool
	for i := 0; i < len(t.dist); i++ {
		//loop for client connected
		for {
			if i == t.myIdx {
				break
			}

			//Ready
			t.dist[i].mu.Lock()
			ready, err = t.dist[i].client.IsReady()
			t.dist[i].mu.Unlock()
			if err != nil {
				log.Printf("distSync,Call IsReady,err:%s\n", err.Error())
				time.Sleep(time.Second)
				continue
			}

			if ready {
				break
			}
		}
	}

}

//distributed peers ping,check the connection is ok
func (t *TopologyBuilder) peersPing() {
	var err error
	for i := 0; i < len(t.dist); i++ {
		//loop for client connected
		for {
			if i == t.myIdx {
				break
			}
			t.dist[i].mu.Lock()
			if t.dist[i].client == nil {
				//wait
				t.dist[i].mu.Unlock()
				time.Sleep(time.Second)
				continue
			}

			//Ping
			err = t.dist[i].client.Ping()
			t.dist[i].mu.Unlock()
			if err != nil {
				log.Printf("peersPing,Ping,err:%s\n", err.Error())
				time.Sleep(time.Second)
				continue
			}
			break
		}
	}
}

// Run TopologyBuilder
func (t *TopologyBuilder) Run() {
	var wg sync.WaitGroup
	stop := make(chan bool)

	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	go t.goStartServer(&wg, stop, transportFactory, protocolFactory)
	t.peersPing()
	t.dealPendGroupings()
	t.distSync()
	t.startSpouts(&wg, stop)
	t.startBolts(&wg)

	//监听退出信号
	go goSignalListen(stop)

	wg.Wait()

	log.Println("all finished")
}
