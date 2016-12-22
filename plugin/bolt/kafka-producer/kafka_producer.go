package kafkap

import (
	"github.com/l0vest0rm/gostream"
	log "github.com/cihub/seelog"
	"github.com/Shopify/sarama"
	kazoo "github.com/wvanbergen/kazoo-go"
)

type ProducerMessage struct {
	Topic string
	Key string
	Value []byte
}

type KafkaProducerBolt struct {
	*gostream.BaseBolt
    zkHosts  []string
    chroot   string
    producer sarama.SyncProducer
}

func (t *ProducerMessage) GetHashKey() interface{} {
	return t.Key
}

func (t *ProducerMessage) GetMsgType() int {
	return 0
}

func NewKafkaProducerBolt(zkHosts []string, chroot string) *KafkaProducerBolt {
	t := &KafkaProducerBolt{}
	t.BaseBolt = gostream.NewBaseBolt()
	t.zkHosts = zkHosts
	t.chroot = chroot
	return t
}

func (t *KafkaProducerBolt) NewInstance() gostream.IBolt {
	log.Debug("KafkaProducerBolt NewInstance")
	t1 := &KafkaProducerBolt{}
	t1.BaseBolt = t.BaseBolt.Copy()
	t1.zkHosts = t.zkHosts
	t1.chroot = t.chroot
	return t1
}

func (t *KafkaProducerBolt) Copy() *KafkaProducerBolt {
	log.Debug("KafkaProducerBolt Copy")
	t1 := &KafkaProducerBolt{}
	t1.BaseBolt = t.BaseBolt.Copy()
	t1.zkHosts = t.zkHosts
	t1.chroot = t.chroot
	return t1
}

func (t *KafkaProducerBolt) Prepare(index int, context gostream.TopologyContext, collector gostream.IOutputCollector) {
	log.Debugf("KafkaProducerBolt Prepare,%d", index)
	t.BaseBolt.Prepare(index, context, collector)

    //获取zookeeper连接
    zkConfig := kazoo.NewConfig()
    if t.chroot != "" {
        zkConfig.Chroot = t.chroot
    }
    kz, err := kazoo.NewKazoo(t.zkHosts, zkConfig)
    if err != nil {
        return
    }
    defer kz.Close()
    brokers, err := kz.BrokerList()
    if err != nil {
        return
    }
    if len(brokers) == 0 {
        log.Errorf("KafkaProducerBolt,Prepare,len(brokers) == 0")
        return
    }

    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.NoResponse // Wait for all in-sync replicas to ack the message
    config.Producer.Compression = sarama.CompressionGZIP
    //按照key hash
    config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
    t.producer, err = sarama.NewSyncProducer(brokers, config)
    if err != nil {
        log.Errorf("KafkaProducerBolt,Prepare,err:%s",err.Error())
        return
    }
}

func (t *KafkaProducerBolt) Cleanup() {
	log.Debugf("HbasePutBolt Cleanup,%d", t.Index)
	t.producer.Close()
	t.BaseBolt.Cleanup()
}

func (t *KafkaProducerBolt) Execute(imessage gostream.Message) {
	msg := imessage.(*ProducerMessage)
    message := &sarama.ProducerMessage{
        Topic: msg.Topic,
        Key:   sarama.StringEncoder(msg.Key),
        Value: sarama.ByteEncoder(msg.Value)}
    _, _, err := t.producer.SendMessage(message)
    if err != nil {
        log.Errorf("KafkaProducerBolt,Execute,topic:%s,err:%s", msg.Topic,err.Error)
    }
}