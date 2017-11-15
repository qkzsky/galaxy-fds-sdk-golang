package Model

import "encoding/json"

type FDSListMultipartUploadsResult struct {
	BucketName     string
	Prefix         string
	MaxKeys        int
	Marker         string
	IsTruncated    bool
	NextMarker     string
	Uploads        []MultipartUploadResult
	CommonPrefixes []string
	Delimiter      string
	rawJsonValue   []byte
}

func NewFDSListMultipartUploadsResult(jsonValue []byte) (*FDSListMultipartUploadsResult, error) {
	var listMultipartUploadsResult FDSListMultipartUploadsResult
	err := json.Unmarshal(jsonValue, &listMultipartUploadsResult)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	listMultipartUploadsResult.rawJsonValue = jsonValue
	return &listMultipartUploadsResult, nil
}

type MultipartUploadResult struct {
	ObjectName  string
	UploadId    string
	UploadParts []string
}
