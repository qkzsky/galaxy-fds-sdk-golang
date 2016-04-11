package Model

import "github.com/bitly/go-simplejson"

type Owner struct {
	id string
	displayName string
}

func NewOwner(jsonValue simplejson.Json) (Owner, error){
	id, err:= jsonValue.Get("id").String()
	if err != nil {
		return nil, err
	}
	displayName, err := jsonValue.Get("displayName").String()
	if err != nil {
		return nil, err
	}
	return Owner {
		id: id,
		displayName: displayName,
	}, nil
}