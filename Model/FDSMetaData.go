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
	rawValue *simplejson.Json
}

func NewFDSMetaData(jsonValue *simplejson.Json) {
	return FDSMetaData{
		rawValue: jsonValue,
	}
}

func (d *FDSMetaData) getContentEncoding() (string) {
	contentEncoding, err := d.rawValue.Get(ContentEncoding).String()
	if err != nil {
		return nil, err
	}
	return contentEncoding, nil
}

func (d *FDSMetaData) getContentType() (string) {
	contentType, err := d.rawValue.Get(ContentType).String()
	if err != nil {
		return nil, err
	}
	return contentType, nil
}

func (d *FDSMetaData) getCacheControl() (string) {
	contentEncoding, err := d.rawValue.Get(CacheControl).String()
	if err != nil {
		return nil, err
	}
	return contentEncoding, nil
}

func (d *FDSMetaData) getContentLength() (int64) {
	contentLength, err := d.rawValue.Get(ContentLength).Int64()
	if err != nil {
		return nil, err
	}
	return contentLength, nil
}

func (d *FDSMetaData) getContentMD5() (string) {
	contentMD5, err := d.rawValue.Get(ContentMD5).String()
	if err != nil {
		return nil, err
	}
	return contentMD5, nil
}

func (d *FDSMetaData) gteLastChecked() (int64) {
	lastChecked, err := d.rawValue.Get(LastChecked).Int64()
	if err != nil {
		return nil, err
	}
	return lastChecked, nil
}

func (d *FDSMetaData) getLastModified() (int64) {
	lastModified, err := d.rawValue.Get(LastModified).Int64()
	if err != nil {
		return nil, err
	}
	return lastModified, nil
}

func (d *FDSMetaData) getRawMetadata() (*simplejson.Json) {
	return d.rawValue
}

func (d *FDSMetaData) getUploadTime() (int64) {
	uploadTime, err := d.rawValue.Get(UploadTime).Int64()
	if err != nil {
		return nil, err
	}
	return uploadTime, nil
}


