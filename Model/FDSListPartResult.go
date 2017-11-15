package Model

import "encoding/json"

type FDSUploadPartResultList struct {
	UploadPartResultList []FDSUploadPartResult
}

func NewFDSUploadPartResultList(rawJson []byte) (*FDSUploadPartResultList, error) {
	var fdslistpartresultlist FDSUploadPartResultList
	err := json.Unmarshal(rawJson, &fdslistpartresultlist)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	return &fdslistpartresultlist, nil
}

type FDSUploadPartResult struct {
	PartNumber int
	Etag       string
	PartSize   int64
}
