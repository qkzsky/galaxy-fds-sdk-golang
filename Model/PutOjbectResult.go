package Model

import (
	"encoding/json"
)

type PutObjectResult struct {
	BucketName   string
	ObjectName   string
	AccessKeyId  string
	Signature    string
	Expires      int64
	rawJsonValue []byte
}

func NewPutObjectResult(jsonValue []byte) (*PutObjectResult, error) {
	var putObjectResult PutObjectResult
	err := json.Unmarshal(jsonValue, &putObjectResult)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	putObjectResult.rawJsonValue = jsonValue
	return &putObjectResult, nil
}
