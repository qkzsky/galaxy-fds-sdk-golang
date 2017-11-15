package Model

import "encoding/json"

type Grantee struct {
	Id          string `json:"id"`
	DisplayName string `json:"displayName"`
}

type AccessControlList struct {
	Grantees   Grantee `json:"grantee"`
	Permission string  `json:"permission"`
	Type       string  `json:"type"`
}

type ACL struct {
	AccessControlLists []AccessControlList `json:"accessControlList"`
	Owners             Owner               `json:"owner"`
}

func NewACL(jsonValue []byte) (*ACL, error) {
	var acl ACL
	if len(jsonValue) == 0 {
		return &acl, nil
	}
	err := json.Unmarshal(jsonValue, &acl)
	if err != nil {
		return nil, NewFDSError(err.Error(), -1)
	}
	return &acl, nil
}
