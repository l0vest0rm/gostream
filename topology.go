package gostream

import (
	"log"
	"math/rand"
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
	componentId  string
	index        int //本component内的索引
	dependentCnt int //依赖messages的上游发送者的数目
	messages     chan Message
}

type StreamInfo struct {
	groupingType int
	tasks        []*TaskInfo //下游Task
}

type ComponentCommon struct {
	id          string
	tb          *TopologyBuilder
	parallelism int
	tasks       []*TaskInfo
	//inputs       map[string]string      //输入map[componentId]streamId
	streams map[string]*StreamInfo //输出
}

type groupInfo struct {
	rcvID        string //receiver component id
	sndID        string //sender component id
	streamID     string //sender streamId
	groupingType int
	isLocal      bool //wether inner process grouping
}

// TopologyBuilder topo struct
type TopologyBuilder struct {
	statInterval  int64 //统计reset周期，单位秒
	mu            sync.RWMutex
	spouts        map[string]*Spout
	bolts         map[string]*Bolt
	commons       map[string]*ComponentCommon
	pendGroupings []*groupInfo
	peers         []string //peers address to connect
	myAddr        string   //my addr for listen
}

type IOutputCollector interface {
	Emit(message Message)
	EmitTo(message Message, streamid string)
}

type TopologyContext interface {
	GetThisComponentId() string
}

func (t *TaskInfo) GetThisComponentId() string {
	return t.componentId
}

func (t *TaskInfo) Emit(message Message) {
	var messages chan Message
	cc := t.cc
	//todo此处可并发
	for _, streamInfo := range cc.streams {
		l := len(streamInfo.tasks)
		if l > 1 {
			switch streamInfo.groupingType {
			case GROUPING_SHUFFLE:
				messages = streamInfo.tasks[rand.Intn(l)].messages
			case GROUPING_KEY:
				hashid := message.GetHashKey(cc.parallelism, t.index, l)
				messages = streamInfo.tasks[hashid].messages
			default:
				log.Fatalf("unknown groupingType:%d\n", streamInfo.groupingType)
				return
			}
		} else {
			messages = streamInfo.tasks[0].messages
		}

		messages <- message
	}
}

func (t *TaskInfo) EmitTo(message Message, streamid string) {
	var messages chan Message
	cc := t.cc
	if streamInfo, ok := t.cc.streams[streamid]; ok {
		l := len(streamInfo.tasks)
		if l > 1 {
			switch streamInfo.groupingType {
			case GROUPING_SHUFFLE:
				messages = streamInfo.tasks[rand.Intn(l)].messages
			case GROUPING_KEY:
				hashid := message.GetHashKey(cc.parallelism, t.index, l)
				messages = streamInfo.tasks[hashid].messages
			default:
				log.Fatalf("unknown groupingType:%d\n", streamInfo.groupingType)
				return
			}
		} else {
			messages = streamInfo.tasks[0].messages
		}

		messages <- message
	}
}

//关闭下游
func (t *ComponentCommon) closeDownstream() {
	for _, streamInfo := range t.streams {
		for _, taskInfo := range streamInfo.tasks {
			t.tb.mu.Lock()
			taskInfo.dependentCnt--
			if taskInfo.dependentCnt == 0 {
				//log.Printf("close channel,componentId:%s,taskid:%d\n", taskInfo.componentId, taskInfo.taskid)
				close(taskInfo.messages)
			}
			t.tb.mu.Unlock()
		}
	}
}

// ShuffleGrouping grouping messages random
func (t *Bolt) ShuffleGrouping(componentID string, streamID string) {
	if t.cc.tb.peers == nil {
		t.cc.tb.pendGrouping(t.cc.id, componentID, streamID, GROUPING_SHUFFLE, true)
	} else {
		t.cc.tb.pendGrouping(t.cc.id, componentID, streamID, GROUPING_SHUFFLE, false)
	}
}

//KeyGrouping grouping messages by key
func (t *Bolt) KeyGrouping(componentID string, streamID string) {
	if t.cc.tb.peers == nil {
		t.cc.tb.pendGrouping(t.cc.id, componentID, streamID, GROUPING_KEY, true)
	} else {
		t.cc.tb.pendGrouping(t.cc.id, componentID, streamID, GROUPING_KEY, false)
	}
}

// ShuffleGroupingLocal grouping messages random local
func (t *Bolt) ShuffleGroupingLocal(componentID string, streamID string) {
	t.cc.tb.pendGrouping(t.cc.id, componentID, streamID, GROUPING_SHUFFLE, true)
}

//KeyGroupingLocal grouping messages by key local
func (t *Bolt) KeyGroupingLocal(componentID string, streamID string) {
	t.cc.tb.pendGrouping(t.cc.id, componentID, streamID, GROUPING_KEY, true)
}

func (t *TopologyBuilder) pendGrouping(rcvID, sndID, streamID string, groupingType int, isLocal bool) {
	gi := &groupInfo{}
	gi.rcvID = rcvID
	gi.sndID = sndID
	gi.streamID = streamID
	gi.groupingType = groupingType
	gi.isLocal = isLocal
	t.pendGroupings = append(t.pendGroupings, gi)
}

// NewTopologyBuilder new one
func NewTopologyBuilder() *TopologyBuilder {
	tb := &TopologyBuilder{}
	tb.commons = make(map[string]*ComponentCommon)
	tb.pendGroupings = make([]*groupInfo, 0)
	return tb
}

// NewTopologyDistBuilder new one
func NewTopologyDistBuilder(addrs []string, myIdx int) *TopologyBuilder {
	tb := NewTopologyBuilder()
	if addrs == nil || len(addrs) < 2 {
		return tb
	}

	//distributed
	tb.peers = make([]string, 0, len(addrs)-1)
	for idx, addr := range addrs {
		if idx == myIdx {
			tb.myAddr = addr
		} else {
			tb.peers = append(tb.peers, addr)
		}
	}

	return tb
}

func (t *TopologyBuilder) SetSpout(id string, ispout ISpout, parallelism int) *Spout {
	if _, ok := t.commons[id]; ok {
		panic("SetSpout,id exist")
	}

	if t.spouts == nil {
		t.spouts = make(map[string]*Spout)
	}

	//先不考虑重复的问题
	cc := &ComponentCommon{}
	cc.id = id
	cc.parallelism = parallelism
	cc.tb = t
	cc.tasks = make([]*TaskInfo, 0, parallelism)
	for i := 0; i < parallelism; i++ {
		task := &TaskInfo{}
		task.componentId = id
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

func (t *TopologyBuilder) SetBolt(id string, ibolt IBolt, parallelism int, bufSize int) *Bolt {
	if t.bolts == nil {
		t.bolts = make(map[string]*Bolt)
	}

	cc := &ComponentCommon{}
	cc.id = id
	cc.parallelism = parallelism
	cc.tb = t
	cc.tasks = make([]*TaskInfo, 0, parallelism)
	for i := 0; i < parallelism; i++ {
		task := &TaskInfo{}
		task.componentId = id
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

func (t *TopologyBuilder) grouping(gi *groupInfo) {
	if t.commons[gi.sndID].streams == nil {
		t.commons[gi.sndID].streams = make(map[string]*StreamInfo)
	}

	streamInfo, ok := t.commons[gi.sndID].streams[gi.streamID]
	if !ok {
		streamInfo = &StreamInfo{}
		streamInfo.groupingType = gi.groupingType
		streamInfo.tasks = make([]*TaskInfo, 0, t.commons[gi.sndID].parallelism)
		t.commons[gi.sndID].streams[gi.streamID] = streamInfo
	}

	for i := 0; i < t.commons[gi.rcvID].parallelism; i++ {
		t.commons[gi.rcvID].tasks[i].dependentCnt += t.commons[gi.sndID].parallelism
		streamInfo.tasks = append(streamInfo.tasks, t.commons[gi.rcvID].tasks[i])
	}
}

func (t *TopologyBuilder) dealPendGroupings() {
	for _, gi := range t.pendGroupings {
		t.grouping(gi)
	}
}

// Run TopologyBuilder
func (t *TopologyBuilder) Run() {
	var wg sync.WaitGroup
	stop := make(chan bool)

	t.dealPendGroupings()
	t.startSpouts(&wg, stop)
	t.startBolts(&wg)

	//监听退出信号
	go goSignalListen(stop)

	wg.Wait()

	log.Println("all finished")
}
