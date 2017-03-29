package gostream

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
	peerIdx        int //my index
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
	client *rpc.Client
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
	}
}

//关闭下游
func (t *ComponentCommon) closeDownstream() {
	//for _, streamInfo := range t.streams {
	//	for _, taskInfo := range streamInfo.targets {
	//	t.tb.mu.Lock()
	//taskInfo.dependentCnt--
	//if taskInfo.dependentCnt == 0 {
	//log.Printf("close channel,componentID:%s,taskid:%d\n", taskInfo.componentID, taskInfo.taskid)
	//	close(taskInfo.messages)
	//}
	//	t.tb.mu.Unlock()
	//}
	//}
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
	if !isLocal {
		gi.peerIdx = t.myIdx
	}
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
	if gi.isLocal {
		t.groupingLocal(gi)
	} else {
		t.groupingRemote(gi)
	}
}

func (t *TopologyBuilder) groupingRemote(gi *GroupInfo) {
	var err error
	var reply int
	for i := 0; i < len(t.dist); i++ {
		t.dist[i].mu.Lock()
		err = t.dist[i].client.Call("TopologyBuilder.RpcGrouping", gi, &reply)
		if err != nil {
			panic(fmt.Sprintf("groupingRemote,call,err:%s", err.Error()))
		}
	}
}

func (t *TopologyBuilder) groupingLocal(gi *GroupInfo) {
	log.Printf("groupingLocal,gi:%v\n", gi)
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
		tg.peerIdx = t.myIdx
		tg.componentID = gi.RcvID
		tg.index = i
		t.commons[gi.SndID].streams[gi.StreamID].targets = append(t.commons[gi.SndID].streams[gi.StreamID].targets, tg)
	}
}

func (t *TopologyBuilder) RpcGrouping(args *GroupInfo, reply *int) {

}

func (t *TopologyBuilder) dealPendGroupings() {
	for _, gi := range t.pendGroupings {
		t.grouping(gi)
	}
}

func (t *TopologyBuilder) startServer(wg *sync.WaitGroup, stop chan bool) {
	if t.dist == nil {
		return
	}

	//listen
	ln, err := net.Listen("tcp", t.dist[t.myIdx].addr)
	if err != nil {
		// handle error
		panic(err.Error())
	}

	//connect peers
	for i := 0; i < len(t.dist); i++ {
		if i == t.myIdx {
			continue
		}

		wg.Add(1)
		go t.connectPeer(wg, stop, i)
	}

	//rpc server
	rpcServer := rpc.NewServer()
	rpc.Register(t)

	//wait for connecttion
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			log.Printf("accept err:%s\n", err.Error())
			continue
		}
		//wg.Add(1)
		//go t.handleConnection(wg, stop, conn)
		go rpcServer.ServeConn(conn)
	}
}

// connectPeer connect other peer
func (t *TopologyBuilder) connectPeer(wg *sync.WaitGroup, stop chan bool, peerIdx int) {
	defer wg.Done()
	conn, err := net.Dial("tcp", t.dist[peerIdx].addr)
	if err != nil {
		// handle error
	}

	client := rpc.NewClient(conn)
	if client == nil {
		//handle error
	}

	t.dist[peerIdx].mu.Lock()
	t.dist[peerIdx].client = client
	t.dist[peerIdx].mu.Unlock()
}

// Run TopologyBuilder
func (t *TopologyBuilder) Run() {
	var wg sync.WaitGroup
	stop := make(chan bool)

	t.startServer(&wg, stop)
	t.dealPendGroupings()
	t.startSpouts(&wg, stop)
	t.startBolts(&wg)

	//监听退出信号
	go goSignalListen(stop)

	wg.Wait()

	log.Println("all finished")
}
