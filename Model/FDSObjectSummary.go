package Model

import (
	"encoding/json"
	"time"
)

type FDSObjectSummary struct {
	Etag       string
	ObjectName string `json:"name"`
	Owner      Owner
	Size       int64
	LastModified time.Time
	UploadTime int64
	rawJsonValue []byte
}

func NewFDSObjectSummary(jsonValue []byte) (*FDSObjectSummary, error) {
	var fdsObjectSummary FDSObjectSummary
	err := json.Unmarshal(jsonValue, &fdsObjectSummary)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	return &fdsObjectSummary, nil
}