package kc

import (
	"github.com/l0vest0rm/gostream"
	"github.com/Shopify/sarama"
	log "github.com/cihub/seelog"
	"github.com/wvanbergen/kafka/consumergroup"
	"time"
    "math/rand"
)

type KafkaConsumerMessage sarama.ConsumerMessage

//尽量确保发送者多个goroutine之间不竞争,srcPrallelism和dstPrallelism之间必须是倍数关系
func (t *KafkaConsumerMessage) GetHashKey(srcPrallelism int, srcIndex int, dstPrallelism int) uint64 {
    if srcPrallelism == dstPrallelism {
        return uint64(srcIndex)
    } else if srcPrallelism > dstPrallelism {
        return uint64(srcIndex % dstPrallelism)
    } else {
        return uint64(rand.Intn(dstPrallelism/srcPrallelism) * srcPrallelism + srcIndex)
    }
}

func (t *KafkaConsumerMessage) GetMsgType() int {
    return 0
}

type KafkaCfg struct {
	ZkHosts           []string
	Chroot            string
	Topics            []string
	ConsumerGroup     string
	OffsetInitial     int64
	ChannelBufferSize int //default 256
}

type KafkaSpout struct {
	*gostream.BaseSpout
	kafkaCfg *KafkaCfg
	cg       *consumergroup.ConsumerGroup
	offsets  map[string]map[int32]int64
	Messages <-chan *sarama.ConsumerMessage
}

func NewKafkaSpout(kafkaCfg *KafkaCfg) gostream.ISpout {
	return NewKafkaSpout2(kafkaCfg)
}

func NewKafkaSpout2(kafkaCfg *KafkaCfg) *KafkaSpout {
    t := &KafkaSpout{}
    t.BaseSpout = gostream.NewBaseSpout()
    t.kafkaCfg = kafkaCfg
    return t
}

func (t *KafkaSpout) NewInstance() gostream.ISpout {
    log.Debug("KafkaSpout NewInstance")
    t1 := &KafkaSpout{}
    t1.BaseSpout = t.BaseSpout.Copy()
    t1.kafkaCfg = t.kafkaCfg

    return t1
}

func (t *KafkaSpout) Copy() *KafkaSpout {
	log.Debug("KafkaSpout Copy")
	t1 := &KafkaSpout{}
	t1.BaseSpout = t.BaseSpout.Copy()
	t1.kafkaCfg = t.kafkaCfg

	return t1
}

func (t *KafkaSpout) Open(index int, context gostream.TopologyContext, collector gostream.IOutputCollector) {
	log.Debugf("KafkaSpout Open,%d", index)
	t.BaseSpout.Open(index, context, collector)
	t.offsets = make(map[string]map[int32]int64)

	var err error
	cfg := consumergroup.NewConfig()
	cfg.Zookeeper.Chroot = t.kafkaCfg.Chroot
    cfg.Zookeeper.Timeout = 120 * time.Second
	cfg.Offsets.Initial = t.kafkaCfg.OffsetInitial
	cfg.Offsets.ProcessingTimeout = 10 * time.Second
	cfg.Consumer.MaxProcessingTime = 200 * time.Millisecond
	if t.kafkaCfg.ChannelBufferSize != 0 {
		cfg.ChannelBufferSize = t.kafkaCfg.ChannelBufferSize
	}

	t.cg, err = consumergroup.JoinConsumerGroup(t.kafkaCfg.ConsumerGroup, t.kafkaCfg.Topics, t.kafkaCfg.ZkHosts, cfg)
	if err != nil {
		log.Criticalf("KafkaSpout Open,JoinConsumerGroup,err:%s", err.Error())
		return
	}

	t.Messages = t.cg.Messages()

	return
}

func (t *KafkaSpout) Close() {
	log.Debugf("KafkaSpout Close,%d", t.Index)
	t.cg.Close()
	t.BaseSpout.Close()
}

func (t *KafkaSpout) NextTuple() {
    message := <- t.Messages
    if !t.Excpected(message) {
        return
    }

    t.Collector.Emit((*KafkaConsumerMessage)(message))
    t.cg.CommitUpto(message)
}

//extra api
func (t *KafkaSpout) Excpected(msg *sarama.ConsumerMessage) bool {
	if t.offsets[msg.Topic] == nil {
		t.offsets[msg.Topic] = make(map[int32]int64)
	}
	if t.offsets[msg.Topic][msg.Partition] != 0 && t.offsets[msg.Topic][msg.Partition] > msg.Offset-1 {
		log.Errorf("SpoutKafka Execute Unexpected offset on %s:%d. Expected %d, found %d, diff %d", msg.Topic, msg.Partition, t.offsets[msg.Topic][msg.Partition]+1, msg.Offset, msg.Offset-t.offsets[msg.Topic][msg.Partition]+1)
		return false
	}

	//记录下当前offset的值
	t.offsets[msg.Topic][msg.Partition] = msg.Offset

	return true
}

func (t *KafkaSpout) CommitUpto(message *sarama.ConsumerMessage) {
	t.cg.CommitUpto(message)
}
