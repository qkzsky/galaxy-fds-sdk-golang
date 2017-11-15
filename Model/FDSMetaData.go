package Model

import (
	"strconv"
	"strings"
)

const (
	CacheControl          = "cache-control"
	ContentEncoding       = "content-encoding"
	ContentLength         = "content-length"
	LastModified          = "last-modified"
	ContentMD5            = "content-md5"
	ContentType           = "content-type"
	LastChecked           = "last-checked"
	UploadTime            = "upload-time"
	ContentMetadataLength = "x-xiaomi-meta-content-length"
)

type FDSMetaData struct {
	m map[string][]string
}

func NewFDSMetaData(rawValue map[string][]string) *FDSMetaData {
	var fdsMetaData FDSMetaData
	fdsMetaData.m = map[string][]string{}
	for k, v := range rawValue {
		fdsMetaData.m[strings.ToLower(k)] = v
	}
	return &fdsMetaData
}

func (d *FDSMetaData) GetKey(k string) (string, error) {
	r, ok := d.m[k]
	if !ok {
		return "", NewFDSError("No such meta: "+k, -1)
	}
	if len(r) > 0 {
		return r[0], nil
	}
	return "", nil
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
		return 0, NewFDSError(err.Error(), -1)
	}
	return strconv.ParseInt(s, 10, 64)
}

func (d *FDSMetaData) GetContentMD5() (string, error) {
	return d.GetKey(ContentMD5)
}

func (d *FDSMetaData) GetLastChecked() (int64, error) {
	s, err := d.GetKey(LastChecked)
	if err != nil {
		return 0, NewFDSError(err.Error(), -1)
	}
	return strconv.ParseInt(s, 10, 64)
}

func (d *FDSMetaData) GetLastModified() (string, error) {
	return d.GetKey(LastModified)
}

func (d *FDSMetaData) GetRawMetadata() map[string][]string {
	return d.m
}

func (d *FDSMetaData) GetUploadTime() (int64, error) {
	s, err := d.GetKey(UploadTime)
	if err != nil {
		return 0, NewFDSError(err.Error(), -1)
	}
	return strconv.ParseInt(s, 10, 64)
}

func (d *FDSMetaData) GetMetadataContentLength() (int64, error) {
	s, err := d.GetKey(ContentMetadataLength)
	if err != nil {
		return 0, NewFDSError(err.Error(), -1)
	}
	return strconv.ParseInt(s, 10, 64)
}
