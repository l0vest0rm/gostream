package gostream

import (
	"github.com/spaolacci/murmur3"
)

type Message interface {
	GetHashKey() interface{} //for hash,return uint64 is best,can be string/[]byte/uint64
	GetMsgType() int //获取消息类型,可以不同类型传输，然后下游根据类型区分
}

func convertKey(key interface{}) uint64 {
	switch key.(type) {
	case string:
		return murmur3.Sum64([]byte(key.(string)))
	case []byte:
		return murmur3.Sum64(key.([]byte))
	case uint64:
		return key.(uint64)
	case int64:
		return uint64(key.(int64))
	default:
		return 0
	}
}