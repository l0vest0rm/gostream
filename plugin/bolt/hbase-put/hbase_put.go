package bhp

import (
	"fmt"
	"time"

	"git.xiaojukeji.com/QaTools/omega-go/src/utils/go-hbase"
	"git.xiaojukeji.com/QaTools/omega-go/src/utils/gostream"
	log "github.com/cihub/seelog"
)

type HbasePutMessage struct {
	RowKey []byte
	Table  string
	Put    *hbase.Put
}

type HbasePutBolt struct {
	*gostream.BaseBolt
	userName string
	zkHosts  []string
	hc       hbase.HBaseClient
}

func (t *HbasePutMessage) GetHashKey() interface{} {
	return t.RowKey
}

func (t *HbasePutMessage) GetMsgType() int {
	return 0
}

func NewBoltHbasePut(userName string, zkHosts []string) *HbasePutBolt {
	t := &HbasePutBolt{}
	t.BaseBolt = gostream.NewBaseBolt()
	t.userName = userName
	t.zkHosts = zkHosts
	return t
}

func (t *HbasePutBolt) NewInstance() gostream.IBolt {
	log.Debug("HbasePutBolt NewInstance")
	t1 := &HbasePutBolt{}
	t1.BaseBolt = t.BaseBolt.Copy()
	t1.userName = t.userName
	t1.zkHosts = t.zkHosts
	return t1
}

func (t *HbasePutBolt) Copy() *HbasePutBolt {
	log.Debug("HbasePutBolt Copy")
	t1 := &HbasePutBolt{}
	t1.BaseBolt = t.BaseBolt.Copy()
	t1.userName = t.userName
	t1.zkHosts = t.zkHosts
	return t1
}

func (t *HbasePutBolt) Prepare(index int, context gostream.TopologyContext, collector gostream.IOutputCollector) {
	log.Debugf("HbasePutBolt Prepare,%d", index)
	t.BaseBolt.Prepare(index, context, collector)

	var err error
	hbase.UserName = t.userName
	t.hc, err = hbase.NewClient(t.zkHosts, "/hbase")
	if err != nil {
		panic(fmt.Sprintf("hbase.NewClient,err:%s", err.Error()))
	}
}

func (t *HbasePutBolt) Cleanup() {
	log.Debugf("HbasePutBolt Cleanup,%d", t.Index)
	t.hc.Close()
	t.BaseBolt.Cleanup()
}

func (t *HbasePutBolt) Execute(message gostream.Message) {
	msg := message.(*HbasePutMessage)
	//log.Debugf("BoltHbasePut Execute,index:%d", msg.Index)
	for {
		succ, err := t.hc.Put(msg.Table, msg.Put)
		if err != nil {
			log.Errorf("HbasePutBolt,Execute,Put,index:%d,err:%s", t.Index, err.Error())
		} else if !succ {
			log.Errorf("HbasePutBolt,Execute,Put,index:%d,fail", t.Index)
		} else {
			return
		}

		time.Sleep(time.Second)
	}
}
