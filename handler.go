package gostream

import (
	"io"
	"log"

	"golang.org/x/net/context"

	"github.com/l0vest0rm/gostream/service"
)

type IPCServiceHandler struct {
	tb *TopologyBuilder
}

func (t *IPCServiceHandler) Ping(context.Context, *service.EmptyParams) (*service.EmptyParams, error) {
	return nil, nil
}

func (t *IPCServiceHandler) IsReady(context.Context, *service.EmptyParams) (*service.IsReadyRsp, error) {
	rsp := &service.IsReadyRsp{IsReady: t.tb.ready}
	return rsp, nil
}

func (t *IPCServiceHandler) Grouping(ctx context.Context, req *service.GroupingReq) (*service.GroupingRsp, error) {
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

	rsp := &service.GroupingRsp{Parallelism: int32(t.tb.commons[int(req.SndID)].parallelism)}
	return rsp, nil
}

func (t *IPCServiceHandler) SendData(stream service.IPCService_SendDataServer) error {
	//t.tb.commons[int(req.RcvID)].tasks[int(req.RcvIdx)].messages <- req
	var dataR *service.DataReq
	var err error
	for {
		dataR, err = stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(nil)
		}
		if err != nil {
			return err
		}
		t.tb.commons[int(dataR.RcvID)].tasks[int(dataR.RcvIdx)].messages <- dataR
	}

	return nil
}
