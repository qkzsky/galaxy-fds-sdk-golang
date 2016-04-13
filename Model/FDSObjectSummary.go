package Model

import (
	"encoding/json"
	"time"
)

type FDSObjectSummary struct {
	BucketName string `json:"name"`
	Etag       string
	ObjectName string
	Owner      Owner
	Size       int64
	LastModified time.Time
	UploadTime int64
	rawJsonValue []byte
}

func NewFDSObjectSummary(jsonValue json) (*FDSObjectSummary, error) {
	var fdsObjectSummary FDSObjectSummary
	err := json.Unmarshal(jsonValue, &fdsObjectSummary)
	if err != nil {
		return nil
	}
	return &fdsObjectSummary, nil
}