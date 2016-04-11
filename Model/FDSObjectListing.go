package Model

import "github.com/bitly/go-simplejson"


type FDSObjectListing struct {
	bucketName string
	prefix string
	delimiter string
	marker string
	nextMarker string
	maxKeys int
	truncated bool
	objectSummaries []FDSObjectSummary
	commonPrefix []string
}

func NewFDSObjectListing(jsonValue simplejson.Json) (FDSObjectListing, error) {
	objectSummaryArray, err := jsonValue.Get("objectSummaries").Array()
	if err != nil {
		return nil, err
	}

	objectSummaries := make([]FDSObjectSummary, len(objectSummaryArray))
	for idx, objectSummaryJson := range(objectSummaryArray) {
		objectSummaries[idx], err = NewFDSObjectSummary(objectSummaryJson)
		if err != nil {
			return nil, err
		}
	}

	bucketName, err := jsonValue.Get("bucketName").String()
	if err != nil {
		return nil, err
	}
	prefix, err     := jsonValue.Get("prefix").String()
	if err != nil {
		return nil, err
	}
	delimiter, err  := jsonValue.Get("delimiter").String()
	if err != nil {
		return nil, err
	}
	marker, err     := jsonValue.Get("marker").String()
	if err != nil {
		return nil, err
	}
	nextMarker, err  := jsonValue.Get("nexMarker").String()
	if err != nil {
		return nil, err
	}
	maxKeys, err    := jsonValue.Get("maxKeys").Int()
	if err != nil {
		return nil, err
	}
	truncated, err  := jsonValue.Get("truncated").Bool()
	if err != nil {
		return nil, err
	}
	commonPrefix, err := jsonValue.Get("commonPrefix").StringArray()
	if err != nil {
		return nil, err
	}
	return FDSObjectListing{
		bucketName: bucketName,
		prefix:     prefix,
		delimiter:  delimiter,
		marker:     marker,
		nextMarker: nextMarker,
		maxKeys:    maxKeys,
		truncated:  truncated,
		objectSummaries: objectSummaries,
		commonPrefix: commonPrefix,
	}, nil
}