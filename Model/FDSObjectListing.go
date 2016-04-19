package Model

import (
	"encoding/json"
)

type FDSObjectListing struct {
	BucketName string `json:"name"`
	Prefix     string
	Delimiter  string
	Marker     string
	NextMarker string
	MaxKeys    int
	Truncated  bool
	ObjectSummaries []FDSObjectSummary `json:"objects"`
	CommonPrefixes  []string
	rawJsonValue    []byte
}

func NewFDSObjectListing(jsonValue []byte) (*FDSObjectListing, error) {
	var fdsObjectListing FDSObjectListing
	err := json.Unmarshal(jsonValue, &fdsObjectListing)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	fdsObjectListing.rawJsonValue = jsonValue
	return &fdsObjectListing, nil
}