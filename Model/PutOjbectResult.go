package Model

import "encoding/json"

type PutObjectResult struct {
	BucketName string
	ObjectName string
	AccessKeyId string
	Signature string
	Expires int64
	rawJsonValue []byte
}

func NewPutObjectResult (jsonValue []byte) (*PutObjectResult, error){
	var putObjectResult PutObjectResult
	err := json.Unmarshal(jsonValue, &putObjectResult)
	if err != nil {
		return nil, err
	}
	putObjectResult.rawJsonValue = jsonValue
	return &putObjectResult, nil
}
