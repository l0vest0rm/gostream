package gostream

const (
	msgMinLen = 4
)

// Message comunication inferface
type Message interface {
	//@srcIndex index of the component
	//return [0, dstPrallelism)
	GetHashKey(srcPrallelism int, srcIndex int, dstPrallelism int) uint64
	Marshal() ([]byte, error)
}

type DistMessage struct {
	RcvID   int32
	RcvIdx  int32
	message Message
}
