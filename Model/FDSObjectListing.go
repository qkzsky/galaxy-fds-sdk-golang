package Model

import "encoding/json"

type FDSObjectListing struct {
	BucketName string
	Prefix     string
	Delimiter  string
	Marker     string
	NextMarker string
	MaxKeys    int
	Truncated  bool
	ObjectSummaries []FDSObjectSummary
	CommonPrefixes  []string
	rawJsonValue    []byte
}

func NewFDSObjectListing(jsonValue json) (*FDSObjectListing, error) {
	var fdsObjectListing FDSObjectListing
	err := json.Unmarshal(jsonValue, fdsObjectListing)
	if err != nil {
		return nil, err
	}
	fdsObjectListing.rawJsonValue = jsonValue
	return &fdsObjectListing, nil
}