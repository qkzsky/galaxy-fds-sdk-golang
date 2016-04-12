package Model

import "encoding/json"

type FDSObjectSummary struct {
	BucketName string
	ObjectName string
	Owner      Owner
	Size       int64
	rawJsonValue []byte
}

func NewFDSObjectSummary(jsonValue json) (*FDSObjectSummary, error) {
	var fdsObjectSummary FDSObjectSummary
	err := json.Unmarshal(jsonValue, fdsObjectSummary)
	if err != nil {
		return nil
	}
	return &fdsObjectSummary, nil
}