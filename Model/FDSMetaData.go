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

func (d *FDSMetaData) getContentEncoding() {
	return d.rawValue.Get(ContentEncoding)
}

func (d *FDSMetaData) getContentType() {
	return d.rawValue.Get(ContentType)
}

func (d *FDSMetaData) getCacheControl() {
	return d.rawValue.Get(CacheControl)
}

func (d *FDSMetaData) getContentLength() {
	return d.rawValue.Get(ContentLength)
}

func (d *FDSMetaData) getContentMD5() {
	return d.rawValue.Get(ContentMD5)
}

func (d *FDSMetaData) gteLastChecked() {
	return d.rawValue.Get(LastChecked)
}

func (d *FDSMetaData) getLastModified() {
	return d.rawValue.Get(LastModified)
}

func (d *FDSMetaData) getRawMetadata() {
	return d.rawValue
}

func (d *FDSMetaData) getUploadTime() {
	return d.rawValue.Get(UploadTime)
}


