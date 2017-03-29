package gostream

import (
	"log"

	"github.com/l0vest0rm/gostream/rpc"
)

type ServiceHandler struct {
	tb *TopologyBuilder
}

func NewServiceHandler(tb *TopologyBuilder) *ServiceHandler {
	return &ServiceHandler{tb: tb}
}

func (t *ServiceHandler) Ping() (err error) {
	return nil
}

func (t *ServiceHandler) IsReady() (r bool, err error) {
	return t.tb.ready, nil
}

func (t *ServiceHandler) Grouping(req *rpc.GroupingRequest) (r int32, err error) {
	log.Printf("RpcGrouping,gi:%v\n", req)
	if t.tb.commons[int(req.SndID)].streams == nil {
		t.tb.commons[int(req.SndID)].streams = make([]*StreamInfo, 0, int(req.StreamID)+1)
	}

	//extend the slice
	for i := len(t.tb.commons[int(req.SndID)].streams); i < int(req.StreamID)+1; i++ {
		si := &StreamInfo{}
		si.targets = make([]*targetInfo, 0, int(req.RcvParallelism))
		t.tb.commons[int(req.SndID)].streams = append(t.tb.commons[int(req.SndID)].streams, si)
	}

	t.tb.commons[int(req.SndID)].streams[int(req.StreamID)].groupingType = int(req.GroupingType)
	for i := 0; i < int(req.RcvParallelism); i++ {
		tg := &targetInfo{}
		tg.peerIdx = int(req.PeerIdx)
		tg.componentID = int(req.RcvID)
		tg.index = i
		t.tb.commons[int(req.SndID)].streams[int(req.StreamID)].targets = append(t.tb.commons[int(req.SndID)].streams[int(req.StreamID)].targets, tg)
	}

	r = int32(t.tb.commons[int(req.SndID)].parallelism)
	return r, nil
}

func (t *ServiceHandler) SendData(req *rpc.DataRequest) (err error) {
	t.tb.commons[int(req.RcvID)].tasks[int(req.RcvIdx)].messages <- req
	return nil
}
