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
	err := json.Unmarshal(jsonValue, &initMultipartUploadResult)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	initMultipartUploadResult.rawJsonValue = jsonValue
	return &initMultipartUploadResult, nil
}

type UploadPartList struct {
	UploadPartResultList []UploadPartResult
}

func NewUploadPartList(jsonValue []byte) (*UploadPartList, error) {
	var uploadPartList UploadPartList
	err := json.Unmarshal(jsonValue, &uploadPartList)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	return &uploadPartList, nil
}

func (u *UploadPartList) AddUploadPartResult(i *UploadPartResult) {
	u.UploadPartResultList = append(u.UploadPartResultList, *i)
}

type UploadPartResult struct {
	PartNumber int
	Etag       string
	PartSize   int64
	rawJsonValue []byte
}

func NewUploadPartResult(jsonValue []byte) (*UploadPartResult, error) {
	var uploadPartResult UploadPartResult
	err := json.Unmarshal(jsonValue, &uploadPartResult)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	uploadPartResult.rawJsonValue = jsonValue

	return &uploadPartResult, nil
}