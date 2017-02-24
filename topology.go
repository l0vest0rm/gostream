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
	cc      *ComponentCommon
	ispout  ISpout
}

type Bolt struct {
	cc    *ComponentCommon
	ibolt IBolt
}

type TaskInfo struct {
    cc *ComponentCommon
    taskid       int //taskid 全局唯一
	componentId  string
	index int //本component内的索引
	dependentCnt int //依赖messages的上游发送者的数目
	messages chan Message
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

type TopologyBuilder struct {
    statInterval int64 //统计reset周期，单位秒
	mu      sync.RWMutex
    nextTaskid int
	spouts  map[string]*Spout
	bolts   map[string]*Bolt
	commons map[string]*ComponentCommon
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
    if streamInfo, ok := t.cc.streams[streamid];ok{
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
			taskInfo.dependentCnt -= 1
			if taskInfo.dependentCnt == 0 {
                //log.Printf("close channel,componentId:%s,taskid:%d\n", taskInfo.componentId, taskInfo.taskid)
                close(taskInfo.messages)
            }
			t.tb.mu.Unlock()
		}
	}
}

func (t *Bolt) grouping(componentId string, streamId string, groupingType int) {
	if t.cc.tb.commons[componentId].streams == nil {
		t.cc.tb.commons[componentId].streams = make(map[string]*StreamInfo)
	}

	streamInfo, ok := t.cc.tb.commons[componentId].streams[streamId]
	if !ok {
		streamInfo = &StreamInfo{}
		streamInfo.groupingType = groupingType
		streamInfo.tasks = make([]*TaskInfo, 0, t.cc.parallelism)
		t.cc.tb.commons[componentId].streams[streamId] = streamInfo
	}

	for i := 0; i < t.cc.parallelism; i++ {
		t.cc.tasks[i].dependentCnt += t.cc.tb.commons[componentId].parallelism
		streamInfo.tasks = append(streamInfo.tasks, t.cc.tasks[i])
	}
}

func (t *Bolt) ShuffleGrouping(componentId string, streamId string) {
	t.grouping(componentId, streamId, GROUPING_SHUFFLE)
}

func (t *Bolt) KeyGrouping(componentId string, streamId string) {
	t.grouping(componentId, streamId, GROUPING_KEY)
}

func NewTopologyBuilder() *TopologyBuilder {
	tb := &TopologyBuilder{}
	tb.commons = make(map[string]*ComponentCommon)
	return tb
}

func (t *TopologyBuilder) newTaskid() int {
    t.mu.Lock()
    defer t.mu.Unlock()

    taskid := t.nextTaskid
    t.nextTaskid++
    return taskid
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
        task.taskid = t.newTaskid()
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
		task.taskid = t.newTaskid()
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
	for id, _ := range t.spouts {
		for i := 0; i < t.commons[id].parallelism; i++ {
			wg.Add(1)
			go t.goSpout(wg, stop, id, i)
		}
	}
}

func (t *TopologyBuilder) startBolts(wg *sync.WaitGroup) {
	for id, _ := range t.bolts {
		for i := 0; i < t.commons[id].parallelism; i++ {
			wg.Add(1)
			go t.goBolt(wg, id, i)
		}
	}
}

func (t *TopologyBuilder) Run() {
	var wg sync.WaitGroup
	stop := make(chan bool)

	t.startSpouts(&wg, stop)
	t.startBolts(&wg)

	//监听退出信号
	go goSignalListen(stop)

	wg.Wait()

	log.Println("all finished")
}
