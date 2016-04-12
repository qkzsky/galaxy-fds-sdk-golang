package Model

import (
	"github.com/bitly/go-simplejson"
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
	RawValue *simplejson.Json
}

func NewFDSMetaData(jsonValue *simplejson.Json) (*FDSMetaData){
	return &FDSMetaData{
		RawValue: jsonValue,
	}
}

func (d *FDSMetaData) GetContentEncoding() (string, error) {
	contentEncoding, err := d.RawValue.Get(ContentEncoding).String()
	if err != nil {
		return nil, err
	}
	return contentEncoding, nil
}

func (d *FDSMetaData) GetContentType() (string, error) {
	contentType, err := d.RawValue.Get(ContentType).String()
	if err != nil {
		return nil, err
	}
	return contentType, nil
}

func (d *FDSMetaData) GetCacheControl() (string, error) {
	contentEncoding, err := d.RawValue.Get(CacheControl).String()
	if err != nil {
		return nil, err
	}
	return contentEncoding, nil
}

func (d *FDSMetaData) GetContentLength() (int64, error) {
	contentLength, err := d.RawValue.Get(ContentLength).Int64()
	if err != nil {
		return nil, err
	}
	return contentLength, nil
}

func (d *FDSMetaData) GetContentMD5() (string, error) {
	contentMD5, err := d.RawValue.Get(ContentMD5).String()
	if err != nil {
		return nil, err
	}
	return contentMD5, nil
}

func (d *FDSMetaData) GetLastChecked() (int64, error) {
	lastChecked, err := d.RawValue.Get(LastChecked).Int64()
	if err != nil {
		return nil, err
	}
	return lastChecked, nil
}

func (d *FDSMetaData) GetLastModified() (int64, error) {
	lastModified, err := d.RawValue.Get(LastModified).Int64()
	if err != nil {
		return nil, err
	}
	return lastModified, nil
}

func (d *FDSMetaData) GetRawMetadata() (*simplejson.Json, err) {
	return d.RawValue
}

func (d *FDSMetaData) GetUploadTime() (int64, error) {
	uploadTime, err := d.RawValue.Get(UploadTime).Int64()
	if err != nil {
		return nil, err
	}
	return uploadTime, nil
}


