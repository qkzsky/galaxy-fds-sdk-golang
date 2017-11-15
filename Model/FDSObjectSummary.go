package Model

import (
	"encoding/json"
	"time"
)

type FDSObjectSummary struct {
	Etag         string    `json:"etag"`
	ObjectName   string    `json:"name"`
	Owner        Owner     `json:"owner"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"lastModified"`
	UploadTime   int64     `json:"uploadTime"`
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
