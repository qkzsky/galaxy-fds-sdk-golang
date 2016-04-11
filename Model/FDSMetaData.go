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

func (d *FDSMetaData) getContentEncoding() (string) {
	contentEncoding, err := d.RawValue.Get(ContentEncoding).String()
	if err != nil {
		return nil, err
	}
	return contentEncoding, nil
}

func (d *FDSMetaData) getContentType() (string) {
	contentType, err := d.RawValue.Get(ContentType).String()
	if err != nil {
		return nil, err
	}
	return contentType, nil
}

func (d *FDSMetaData) getCacheControl() (string) {
	contentEncoding, err := d.RawValue.Get(CacheControl).String()
	if err != nil {
		return nil, err
	}
	return contentEncoding, nil
}

func (d *FDSMetaData) getContentLength() (int64) {
	contentLength, err := d.RawValue.Get(ContentLength).Int64()
	if err != nil {
		return nil, err
	}
	return contentLength, nil
}

func (d *FDSMetaData) getContentMD5() (string) {
	contentMD5, err := d.RawValue.Get(ContentMD5).String()
	if err != nil {
		return nil, err
	}
	return contentMD5, nil
}

func (d *FDSMetaData) gteLastChecked() (int64) {
	lastChecked, err := d.RawValue.Get(LastChecked).Int64()
	if err != nil {
		return nil, err
	}
	return lastChecked, nil
}

func (d *FDSMetaData) getLastModified() (int64) {
	lastModified, err := d.RawValue.Get(LastModified).Int64()
	if err != nil {
		return nil, err
	}
	return lastModified, nil
}

func (d *FDSMetaData) getRawMetadata() (*simplejson.Json) {
	return d.RawValue
}

func (d *FDSMetaData) getUploadTime() (int64) {
	uploadTime, err := d.RawValue.Get(UploadTime).Int64()
	if err != nil {
		return nil, err
	}
	return uploadTime, nil
}


