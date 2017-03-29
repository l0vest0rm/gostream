package gostream

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestHashKey(t *testing.T) {
	eventid := "OMGH5PageView"
	hashid := convertKey(eventid)
	fmt.Printf("eventid:%s,hashid:%d\n", eventid, hashid)
}

type Event struct {
	Un json.Number            `json:"un,Number"`
	Ex map[string]interface{} `json:"ex"`
}

type Event2 struct {
	Un json.Number            `json:"un"`
	Ex map[string]json.Number `json:"ex"`
}

func TestJsonCompatily(t *testing.T) {
	str := `{"un": "user", "ex":{"key": 99999999, "ca": "xyz"}}`
	var e1 Event
	err := json.Unmarshal([]byte(str), &e1)
	if err != nil {
		fmt.Printf("err:%s\n", err.Error())
	}

	var e2 Event2
	err = json.Unmarshal([]byte(str), &e2)
	if err != nil {
		fmt.Printf("err:%s\n", err.Error())
	}

	str1, err := json.Marshal(e1)
	fmt.Printf("str1:%s\n", str1)
	str2, err := json.Marshal(e2)
	fmt.Printf("str2:%s\n", str2)
}
