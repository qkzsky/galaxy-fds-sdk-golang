package Model

import "github.com/bitly/go-simplejson"

type UploadPartResult struct {
	PartNumber int
	Etag       string
	PartSize   int64
	InternalValue *simplejson.Json
}

func NewUploadPartResult(jsonVal simplejson.Json) (*UploadPartResult, error) {
	partNumber, err := jsonVal.Get("partNumber").Int()
	if err != nil {
		return nil, err
	}
	etag, err := jsonVal.Get("etag").String()
	if err != nil {
		return nil, err
	}
	partSize, err := jsonVal.Get("partSize").Int64()
	if err != nil {
		return nil, err
	}

	return &UploadPartResult{
		PartNumber:    partNumber,
		Etag:          etag,
		PartSize:      partSize,
		InternalValue: &jsonVal,
	}
}