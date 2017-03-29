package gostream

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	msgMinLen        = 4
	msgTypeHeartBeat = 1
	msgTypeEOF       = 2
	msgTypeConnect   = 3
	msgTypeData      = 4
)

// Message comunication inferface
type Message interface {
	//@srcIndex index of the component
	//return [0, dstPrallelism)
	GetHashKey(srcPrallelism int, srcIndex int, dstPrallelism int) uint64
	Marshal() ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

// SocketMessage comunication msg struct
type SocketMessage struct {
	msgType        int
	sndProcessID   int //sender's processID
	rcvComponentID int //receiver's componentID
	rcvIdx         int //receiver's index with the componentID
	data           []byte
}

// ReadPeerMessage reads out the message
func ReadPeerMessage(reader io.Reader) (m []byte, err error) {
	var length int32
	err = binary.Read(reader, binary.LittleEndian, &length)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message length: %v", err)
	}
	if length < msgMinLen {
		return nil, fmt.Errorf("Failed to read message length=%d", length)
	}
	m = make([]byte, length)
	n, err := io.ReadFull(reader, m)
	if err == io.EOF {
		return nil, fmt.Errorf("Unexpected EOF when reading message size %d, but actual only %d", length, n)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message content size %d, but read only %d: %v", length, n, err)
	}

	return
}

// ReadSocketMessage reads out the message
func ReadSocketMessage2(reader io.Reader) (*SocketMessage, error) {
	var length int32
	err := binary.Read(reader, binary.LittleEndian, &length)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message length: %v", err)
	}
	if length < msgMinLen {
		return nil, fmt.Errorf("Failed to read message length=%d", length)
	}
	m := make([]byte, length)
	n, err := io.ReadFull(reader, m)
	if err == io.EOF {
		return nil, fmt.Errorf("Unexpected EOF when reading message size %d, but actual only %d", length, n)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message content size %d, but read only %d: %v", length, n, err)
	}

	sm := &SocketMessage{}
	sm.msgType = int(binary.LittleEndian.Uint32(m))
	sm.sndProcessID = int(binary.LittleEndian.Uint32(m[4:]))
	sm.rcvComponentID = int(binary.LittleEndian.Uint32(m[8:]))
	sm.rcvIdx = int(binary.LittleEndian.Uint32(m[12:]))
	if length > msgMinLen {
		sm.data = m[16:]
	}

	return sm, nil
}
