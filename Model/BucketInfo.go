package Model

import (
	"encoding/json"
)

type BucketInfo struct {
	AllowOutsideAccess bool   `json:"allowOutsideAccess"`
	CreationTime       int64  `json:"creationTime"`
	BucketName         string `json:"name"`
	ObjectNum          int64  `json:"numObjects"`
	UsedSpace          int64  `json:"usedSpace"`
}

func NewBucketInfo(jsonValue []byte) (*BucketInfo, error) {
	var bucketInfo BucketInfo
	if len(jsonValue) == 0 {
		return nil, nil
	}
	err := json.Unmarshal(jsonValue, &bucketInfo)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	return &bucketInfo, nil
}
