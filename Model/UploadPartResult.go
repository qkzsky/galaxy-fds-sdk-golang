package Model

import "encoding/json"

type InitMultipartUploadResult struct {
	BucketName string
	ObjectName string
	UploadId   string
	rawJsonValue []byte
}

func NewInitMultipartUploadResult (jsonValue []byte) (*InitMultipartUploadResult, error){

	var initMultipartUploadResult InitMultipartUploadResult
	err := json.Unmarshal(initMultipartUploadResult, jsonValue)
	if err != nil {
		return nil, err
	}
	initMultipartUploadResult.rawJsonValue = jsonValue
	return initMultipartUploadResult, nil
}

type UploadPartList struct {
	uploadPartResultList []UploadPartResult
}

func (u *UploadPartList) AddUploadPartResult(i *UploadPartResult) {
	append(u.uploadPartResultList, *i)
}

type UploadPartResult struct {
	PartNumber int
	Etag       string
	PartSize   int64
	rawJsonValue []byte
}

func NewUploadPartResult(jsonValue []byte) (*UploadPartResult, error) {
	var uploadPartResult UploadPartResult
	err := json.Unmarshal(uploadPartResult, jsonValue)
	if err != nil {
		return nil, err
	}
	uploadPartResult.rawJsonValue = jsonValue

	return &uploadPartResult, nil
}