package gostream

import (
	"fmt"
	"testing"
)

func TestHashKey(t *testing.T) {
	eventid := "OMGH5PageView"
	hashid := convertKey(eventid)
	fmt.Printf("eventid:%s,hashid:%d\n", eventid, hashid)
}
