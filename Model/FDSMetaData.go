package Model

import (
	"encoding/json"
	"errors"
	"strconv"
)

const (
	CacheControl = "cache-control"
	ContentEncoding = "content-encoding"
	ContentLength = "content-length"
	LastModified = "last-modified"
	ContentMD5 = "content-md5"
	ContentType = "content-type"
	LastChecked = "last-checked"
	UploadTime = "upload-time"
)

type FDSMetaData struct {
	m        map[string]interface{}
	RawData  []byte
}

func NewFDSMetaData(rawValue []byte) (*FDSMetaData, error){
	var fdsMetaData FDSMetaData
	err := json.Unmarshal(rawValue, &fdsMetaData.m)
	if err != nil {
		return err
	}

	fdsMetaData.RawData = rawValue
	return &fdsMetaData, nil
}

func (d *FDSMetaData) GetKey(k string) (string, error) {
	r, ok := d.m[k]
	if !ok {
		return nil, errors.New("No such meta: " + k)
	}
	r, ok = r.(string)
	if !ok {
		return nil, errors.New("Invalid type for: " + k)
	}
	return r, nil
}

func (d *FDSMetaData) GetContentEncoding() (string, error) {
	return d.GetKey(ContentEncoding)
}

func (d *FDSMetaData) GetContentType() (string, error) {
	return d.GetKey(ContentType)
}

func (d *FDSMetaData) GetCacheControl() (string, error) {
	return d.GetKey(CacheControl)
}

func (d *FDSMetaData) GetContentLength() (int64, error) {
	s, err := d.GetKey(ContentLength)
	if err != nil {
		return nil, err
	}
	return strconv.ParseInt(s, 10, 64)
}

func (d *FDSMetaData) GetContentMD5() (string, error) {
	return d.GetKey(ContentMD5)
}

func (d *FDSMetaData) GetLastChecked() (int64, error) {
	s, err := d.GetKey(LastChecked)
	if err != nil {
		return nil, err
	}
	return strconv.ParseInt(s, 10, 64)
}

func (d *FDSMetaData) GetLastModified() (int64, error) {
	s, err := d.GetKey(LastModified)
	if err != nil {
		return nil, err
	}
	return strconv.ParseInt(s, 10, 64)
}

func (d *FDSMetaData) GetRawMetadata() ([]byte, error) {
	return d.RawData
}

func (d *FDSMetaData) GetUploadTime() (int64, error) {
	s, err := d.GetKey(UploadTime)
	if err != nil {
		return nil, err
	}
	return strconv.ParseInt(s, 10, 64)
}


