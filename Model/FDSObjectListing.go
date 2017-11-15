package Model

import (
	"encoding/json"
	"strings"
)

type FDSObjectListing struct {
	BucketName      string `json:"name"`
	Prefix          string
	Delimiter       string
	Marker          string
	NextMarker      string
	MaxKeys         int
	Truncated       bool
	ObjectSummaries []FDSObjectSummary `json:"objects"`
	CommonPrefixes  []string
	rawJsonValue    []byte
}

func NewFDSObjectListing(jsonValue []byte) (*FDSObjectListing, error) {
	var fdsObjectListing FDSObjectListing
	if len(jsonValue) == 0 {
		return &fdsObjectListing, nil
	}
	// 为了绕过null装换成时间类型的bug，在这里对包含"lastModified":null字段的json数据进行删除
	if strings.Contains(string(jsonValue), "\"lastModified\":null,") {
		jsonValue = []byte(strings.Replace(string(jsonValue), "\"lastModified\":null,", "", -1))
	}
	err := json.Unmarshal(jsonValue, &fdsObjectListing)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	fdsObjectListing.rawJsonValue = jsonValue
	return &fdsObjectListing, nil
}

func (f *FDSObjectListing) IsTuncated() bool {
	return f.Truncated
}
