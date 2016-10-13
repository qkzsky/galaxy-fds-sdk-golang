package galaxy_fds_sdk_golang

import (
	// "crypto/md5"
	"encoding/json"
	// "io"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"strings"
	"bytes"
	"github.com/XiaoMi/galaxy-fds-sdk-golang/Model"
	"time"
	"crypto/md5"
	"net/url"
	"strconv"
	"io"
)

const (
	URI_CDN                            = "cdn"
	URI_FDS_SUFFIX                     = ".fds.api.xiaomi.com/"
	URI_FDS_CDN_SUFFIX                 = ".fds.api.mi-img.com/"
	URI_HTTP_PREFIX                    = "http://"
	URI_HTTPS_PREFIX                   = "https://"
	USER_DEFINED_METADATA_PREFIX       = "x-xiaomi-meta-"
	DELIMITER                          = "/"
	DEFAULT_LIST_MAX_KEYS              = 1000
	GALAXY_ACCESS_KEY_ID               = "GalaxyAccessKeyId"
	EXPIRES                            = "Expires"
	SIGNATURE                          = "Signature"
)

// permission
const (
	PERMISSION_READ         = "READ"
	PERMISSION_WRITE        = "WRITE"
	PERMISSION_FULL_CONTROL = "FULL_CONTROL"
	PERMISSION_USER         = "USER"
	PERMISSION_GROUP        = "GROUP"
)

const (
	REGION_CNBJ0 = "cnbj0"
	REGION_CNBJ1 = "cnbj1"
	REGION_CNBJ2 = "cnbj2"
	REGION_AWSBJ0 = "awsbj0"
	REGION_AWSUSOR0 = "awsusor0"
	REGION_AWSSGP0 = "awssgp0"
	REGION_AWSDE0 = "awsde0"
)

var ALL_USERS = map[string]string{"id": "ALL_USERS"}
var AUTHENTICATED_USERS = map[string]string{"id": "AUTHENTICATED_USERS"}

var PRE_DEFINED_METADATA = []string{"cache-control",
	"content-encoding",
	"content-length",
	"content-md5",
	"content-type",
}

type FDSClient struct {
	AppKey     string
	AppSecret  string
	RegionName string
        EndPoint string
	EnableHttps bool
	EnableCDN  bool
}

type FDSAuth struct {
	UrlBase      string
	Method       string
	Data         []byte
	Content_Md5  string
	Content_Type string
	Headers      *map[string]string
	Params       *map[string]string
}


func NEWFDSClient(appkey, appSecret, regionName string, endPoint string, enableHttps, enableCDN bool) *FDSClient {
	if len(regionName) == 0 &&  len(endPoint) == 0 {
		// default to cnbj0
		regionName = REGION_CNBJ0
	}

	return &FDSClient {
		AppKey: appkey,
		AppSecret: appSecret,
		RegionName: regionName,
                EndPoint: endPoint,
		EnableHttps: enableHttps,
		EnableCDN: enableCDN,
	}
}

func (c *FDSClient) getBaseUriPrefix() string {
	if c.EnableCDN {
                return URI_CDN + "." + c.RegionName;
	}
	return c.RegionName;
}

func (c *FDSClient) getUploadUriPrefix() string {
	if c.EnableCDN {
		return URI_CDN + "." + c.RegionName;
	}
	return c.RegionName;
}

func (c *FDSClient) getUploadUriSuffix () string {
	return URI_FDS_SUFFIX
}

func (c *FDSClient) getBaseUriSuffix () string {
	if c.EnableCDN {
		return URI_FDS_CDN_SUFFIX
	}
	return URI_FDS_SUFFIX
}

func (c *FDSClient) GetBaseUri() string {
	u := bytes.Buffer{}
	if c.EnableHttps {
		u.WriteString(URI_HTTPS_PREFIX)
	} else {
		u.WriteString(URI_HTTP_PREFIX)
	}

        if len(c.EndPoint) > 0 {
               u.WriteString(c.EndPoint)
               u.WriteString("/")
        } else {
	       u.WriteString(c.getBaseUriPrefix())
	       u.WriteString(c.getBaseUriSuffix())
        }
	return u.String()
}

func (c *FDSClient) GetUploadURL() string {
	u := bytes.Buffer{}
	if c.EnableHttps {
		u.WriteString(URI_HTTPS_PREFIX)
	} else {
		u.WriteString(URI_HTTP_PREFIX)
	}

        if len(c.EndPoint) > 0 {
              u.WriteString(c.EndPoint)
              u.WriteString("/")
        } else {
	      u.WriteString(c.getUploadUriPrefix())
	      u.WriteString(c.getUploadUriSuffix())
        }
	return u.String()
}


func (c *FDSClient) Auth(auth FDSAuth) (*http.Response, error) {
	client := &http.Client{
	}

	urlParsed, err := url.Parse(auth.UrlBase)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	params := url.Values{}
	for k, v := range(urlParsed.Query()) {
		if len(v) > 0 {
			params.Add(k, v[0])
		} else {
			params.Add(k, "")
		}
	}
	if auth.Params != nil {
		for k, v := range (*auth.Params) {
			params.Add(k, v)
		}
	}
	urlParsed.RawQuery = params.Encode()
	urlStr := urlParsed.String()

	req, _ := http.NewRequest(auth.Method, urlStr, ioutil.NopCloser(bytes.NewReader(auth.Data)))
	if auth.Headers != nil {
		for k, v := range *auth.Headers {
			req.Header.Add(k, v)
		}
	}
	req.Header.Add("date", time.Now().Format(time.RFC1123))
	req.Header.Add("content-md5", auth.Content_Md5)
	req.Header.Add("content-type", auth.Content_Type)

	signature, err := Signature(c.AppSecret, req.Method, urlStr, req.Header)
	if err != nil {
		return nil, err
	}

	req.Header.Add("authorization", fmt.Sprintf("Galaxy-V2 %s:%s", c.AppKey, signature))
	res, err := client.Do(req)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	return res, nil
}

func (c *FDSClient) Is_Bucket_Exists(bucketname string) (bool, error) {
	url := c.GetBaseUri() + bucketname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "HEAD",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) List_Bucket() ([]string, error) {
	bucketlist := []string{}
	url := c.GetBaseUri()
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return bucketlist, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return bucketlist, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		var sj map[string]interface{}
		err := json.Unmarshal(body, &sj)
		if err != nil {
			return bucketlist, Model.NewFDSError(err.Error(), -1)
		}
		buckets, ok := sj["buckets"]
		if !ok {
			return bucketlist, Model.NewFDSError(err.Error(), -1)
		}
		bucketsList, ok := buckets.([]interface{})
		if !ok {
			return bucketlist, Model.NewFDSError(err.Error(), -1)
		}
		for _, bucket := range bucketsList {
			// fmt.Printf("%#v\n", bucket.(map[string]interface{})["name"])
			bucket = bucket.(map[string]interface{})["name"]
			bucketlist = append(bucketlist, bucket.(string))
		}
		return bucketlist, nil
	} else {
		return bucketlist, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Create_Bucket(bucketname string) (bool, error) {
	url := c.GetUploadURL() + bucketname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Delete_Bucket(bucketname string) (bool, error) {
	url := c.GetBaseUri() + bucketname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "DELETE",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Is_Object_Exists(bucketname, objectname string) (bool, error) {
	url := c.GetBaseUri() + bucketname + DELIMITER + objectname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "HEAD",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else if res.StatusCode == 404 {
		return false, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Get_Object(bucketname, objectname string, position int64, size int64) (*Model.FDSObject, error) {
	if position < 0 {
		return nil, Model.NewFDSError("Seek position should be no less than 0", -1)
	}
	url := c.GetBaseUri() + bucketname + DELIMITER + objectname
	headers := map[string]string{}
	if position >= 0 && size < 0 {
		headers["range"] = fmt.Sprintf("bytes=%d-", position)
	} else if position > 0 && size > 0 {
		headers["range"] = fmt.Sprintf("bytes=%d-%d", position, position + size - 1)
	}
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      &headers,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusPartialContent {
		return &Model.FDSObject{
			BucketName: bucketname,
			ObjectName: objectname,
			Metadata:   *Model.NewFDSMetaData(res.Header),
			ObjectContent: body,
		}, nil
	} else {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Get_Object_Reader(bucketname, objectname string, position int64, size int64) (*io.ReadCloser, error) {
	if position < 0 {
		return nil, Model.NewFDSError("Seek position should be no less than 0", -1)
	}
	url := c.GetBaseUri() + bucketname + DELIMITER + objectname
	headers := map[string]string{}
	if position > 0 && size < 0 {
		headers["range"] = fmt.Sprintf("bytes=%d-", position)
	} else if position > 0 && size > 0 {
		headers["range"] = fmt.Sprintf("bytes=%d-%d", position, position + size - 1)
	}
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      &headers,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusPartialContent {
		return &res.Body, nil
	} else {
		body, err := ioutil.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			return nil, Model.NewFDSError(err.Error(), res.StatusCode)
		}
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
}
// prefix需要改进
func (c *FDSClient) List_Object(bucketname, prefix, delimiter string, maxKeys int) (*Model.FDSObjectListing, error) {
	urlStr := c.GetBaseUri() + bucketname
	auth := FDSAuth{
		UrlBase:          urlStr,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
		Params:       &map[string]string{
			"prefix": prefix,
			"delimiter": delimiter,
			"maxKeys": strconv.Itoa(maxKeys),
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return Model.NewFDSObjectListing(body)
	} else {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c* FDSClient) List_Multipart_Uploads(bucketName, prefix,
delimiter string, maxKeys int) (*Model.FDSListMultipartUploadsResult, error) {
	url := c.GetBaseUri() + bucketName
	auth := FDSAuth {
		UrlBase:      url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
		Params:       &map[string]string {
			"uploads": "",
			"prefix": prefix,
			"delimiter": delimiter,
			"maxKeys": strconv.Itoa(maxKeys),
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		listMultipartUploadsResult, err := Model.NewFDSListMultipartUploadsResult(body)
		if err != nil {
			return nil, Model.NewFDSError(err.Error(), -1)
		}
		return listMultipartUploadsResult, nil
	} else {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c* FDSClient) List_Parts(bucketName, objectName, uploadId string) (*Model.UploadPartList, error){
	url := c.GetBaseUri() + bucketName + DELIMITER + objectName
	headers := map[string]string{}
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      &headers,
		Params:       &map[string]string {
			"uploadId": uploadId,
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == http.StatusOK {
		return Model.NewUploadPartList(body)
	} else {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) List_Next_Batch_Of_Objects(previous *Model.FDSObjectListing) (*Model.FDSObjectListing, error) {
	if !previous.Truncated {
		return nil, errors.New("No more objects")
	}
	bucketName := previous.BucketName
	prefix := previous.Prefix
	delimiter := previous.Delimiter
	marker := previous.NextMarker
	maxKeys := previous.MaxKeys
	url := c.GetBaseUri() + bucketName

	auth := FDSAuth{
		UrlBase:      url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
		Params:       &map[string]string {
			"prefix": prefix,
			"delimiter": delimiter,
			"maxKeys": strconv.Itoa(maxKeys),
			"marker": marker,
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return Model.NewFDSObjectListing(body)
	} else {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
}

// v1类型
func (c *FDSClient) Post_Object(bucketname string, data []byte, filetype string) (string, error) {
	url := c.GetBaseUri() + bucketname + DELIMITER
	if !strings.HasPrefix(filetype, ".") {
		filetype = "." + filetype
	}
	content_type := mime.TypeByExtension(filetype)
	if content_type == "" {
		content_type = "application/octet-stream"
	}
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "POST",
		Data:         data,
		Content_Md5:  "",
		Content_Type: content_type,
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return "", Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return "", Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		var sj map[string]interface{}
		err := json.Unmarshal(body, &sj)
		if err != nil {
			return "", Model.NewFDSError(err.Error(), -1)
		}
		objectname, ok := sj["objectName"]
		if !ok {
			return "", Model.NewFDSError(err.Error(), -1)
		}
		objectNameStr, ok := objectname.(string)
		if !ok {
			return "", Model.NewFDSError(err.Error(), -1)
		}
		return objectNameStr, nil
	} else {
		return "", Model.NewFDSError(string(body), res.StatusCode)
	}
}

// v2类型  自定义文件名 如果object已存在，将会覆盖
func (c *FDSClient) Put_Object(bucketname string, objectname string,
                               data []byte, contentType string,
headers *map[string]string) (*Model.PutObjectResult, error) {
	url := c.GetUploadURL() + bucketname + DELIMITER + objectname
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	md5sum := fmt.Sprintf("%x", md5.Sum(data))
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "PUT",
		Data:         data,
		Content_Md5:  md5sum,
		Content_Type: contentType,
		Headers:      headers,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return Model.NewPutObjectResult(body)
	} else {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func checkNotEmpty(s string) bool {
	return len(s) > 0
}

func (c *FDSClient) Delete_Object(bucketname, objectname string) (bool, error) {
	if !checkNotEmpty(bucketname) || !checkNotEmpty(objectname) {
		return false, errors.New("empty argument")
	}
	url := c.GetBaseUri() + bucketname + DELIMITER + objectname
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "DELETE",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Rename_Object(bucketname, src_objectname, dst_objectname string) (bool, error) {
	url := c.GetUploadURL() + bucketname + DELIMITER + src_objectname +
		"?renameTo=" + dst_objectname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Prefetch_Object(bucketname, objectname string) (bool, error) {
	url := c.GetUploadURL() + bucketname + DELIMITER + objectname + "?prefetch"
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Refresh_Object(bucketname, objectname string) (bool, error) {
	url := c.GetUploadURL() + bucketname + DELIMITER + objectname + "?refresh"
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Set_Object_Acl(bucketname, objectname string, acl map[string]interface{}) (bool, error) {
	acp := make(map[string]interface{})
	acp["owner"] = map[string]string{"id": c.AppKey}
	acp["accessControlList"] = []interface{}{acl}
	jsonString, _ := json.Marshal(acp)
	url := c.GetUploadURL() + bucketname + DELIMITER + objectname + "?acl"
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         jsonString,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, Model.NewFDSError(string(body), res.StatusCode)
	}
}

func (c *FDSClient) Set_Public(bucketname, objectname string, disable_prefetch bool) (bool, error) {
	grant := map[string]interface{}{
		"grantee":    ALL_USERS,
		"type":       PERMISSION_GROUP,
		"permission": string(PERMISSION_READ),
	}
	// acl := make(map[string]interface{})
	// key := ALL_USERS["id"] + ":" + PERMISSION_GROUP
	// acl[key] = grant
	// result := Set_Object_Acl(bucketname, objectname, acl)
	_, err := c.Set_Object_Acl(bucketname, objectname, grant)
	if err != nil {
		return false, Model.NewFDSError(err.Error(), -1)
	}
	if !disable_prefetch {
		_, err := c.Prefetch_Object(bucketname, objectname)
		if err != nil {
			return false, Model.NewFDSError(err.Error(), -1)
		}
	}
	return true, nil
}

func (c *FDSClient) Init_MultiPart_Upload(bucketname, objectname string, contentType string) (*Model.InitMultipartUploadResult, error) {
	url := c.GetUploadURL() + bucketname + DELIMITER + objectname
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	md5sum := fmt.Sprintf("%x", md5.Sum([]byte("")))
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "PUT",
		Data:         []byte(""),
		Content_Md5:  md5sum,
		Content_Type: contentType,
		Headers:      &map[string]string{"x-xiaomi-estimated-object-size": "1000000000"},
		Params:       &map[string]string {
			"uploads": "",
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode != 200 {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}

	return Model.NewInitMultipartUploadResult(body)
}

func (c *FDSClient) Upload_Part(initUploadPartResult *Model.InitMultipartUploadResult, partnumber int, data []byte) (*Model.UploadPartResult, error) {
	bucketname := initUploadPartResult.BucketName
	objectname := initUploadPartResult.ObjectName
	uploadId   := initUploadPartResult.UploadId
	url := c.GetUploadURL() + bucketname + DELIMITER + objectname
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "PUT",
		Data:         data,
		Content_Md5:  "",
		Headers:      nil,
		Params:       &map[string]string {
			"uploadId": uploadId,
			"partNumber": strconv.Itoa(partnumber),
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()

	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode != 200 {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
	return Model.NewUploadPartResult(body)
}

func (c *FDSClient) Complete_Multipart_Upload(initPartuploadResult *Model.InitMultipartUploadResult,
uploadPartResultList *Model.UploadPartList) (*Model.PutObjectResult, error) {
	bucketName := initPartuploadResult.BucketName
	objectName := initPartuploadResult.ObjectName
	uploadId := initPartuploadResult.UploadId
	url := c.GetUploadURL() + bucketName + DELIMITER + objectName
	uploadPartResultListByteArray, err := json.Marshal(*uploadPartResultList)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         uploadPartResultListByteArray,
		Content_Md5:  "",
		Headers:      nil,
		Params:       &map[string]string {
			"uploadId": uploadId,
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode != 200 {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
	return Model.NewPutObjectResult(body)
}


func (c *FDSClient) Abort_MultipartUpload(initPartuploadResult *Model.InitMultipartUploadResult) error {
	bucketName := initPartuploadResult.BucketName
	objectName := initPartuploadResult.ObjectName
	uploadId := initPartuploadResult.UploadId
	url := c.GetUploadURL() + bucketName + DELIMITER + objectName
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "DELETE",
		Data:         nil,
		Content_Md5:  "",
		Headers:      nil,
		Params:       &map[string]string {
			"uploadId": uploadId,
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode != 200 {
		return Model.NewFDSError(string(body), res.StatusCode)
	}
	return nil
}

func (c *FDSClient) Get_Object_Meta(bucketname, objectname string) (*Model.FDSMetaData, error) {
	url := c.GetBaseUri() + bucketname +
	DELIMITER + objectname + "?metadata"
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Headers:      nil,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode != 200 {
		return nil, Model.NewFDSError(string(body), res.StatusCode)
	}
	return Model.NewFDSMetaData(res.Header), nil
}

func (c *FDSClient) Generate_Presigned_URI(bucketname, objectname, method string,
expiration int64, headers map[string][]string) (string, error) {
	urlStr := c.GetBaseUri() + bucketname + DELIMITER +
	objectname

	urlParsed, err := url.Parse(urlStr)
	if err != nil {
		return "", Model.NewFDSError(err.Error(), -1)
	}
	params := url.Values{}
	if method == "HEAD" {
		params.Add("metadata", "");
	}
	params.Add(GALAXY_ACCESS_KEY_ID, c.AppKey)
	params.Add(EXPIRES, fmt.Sprintf("%d", expiration))
	urlParsed.RawQuery = params.Encode()
	signature, err := Signature(c.AppSecret, method, urlParsed.String(), headers)
	if err != nil {
		return "", Model.NewFDSError(err.Error(), -1)
	}

	//params.Add(SIGNATURE, signature)
	//urlParsed.RawQuery = params.Encode()
	return urlParsed.String() + "&" + SIGNATURE + "=" + signature, nil
}

func (c *FDSClient) Delete_Objects(bucketname string, prefix []string) error {
	url := c.GetUploadURL() + bucketname
	prefixJson, err := json.Marshal(prefix)
	if err != nil {
		return Model.NewFDSError(err.Error(), -1)
	}
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "PUT",
		Data:         prefixJson,
		Content_Md5:  "",
		Headers:      nil,
		Params:       &map[string]string {
			"deleteObjects": "",
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return Model.NewFDSError(err.Error(), -1)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return Model.NewFDSError(err.Error(), -1)
	}
	if res.StatusCode != 200 {
		return Model.NewFDSError(string(body), res.StatusCode)
	}
	return nil
}

func (c *FDSClient) Delete_Objects_With_Prefix(bucketname, prefix string) error {
	listObjectResult, err := c.List_Object(bucketname, prefix, "", DEFAULT_LIST_MAX_KEYS)
	if err != nil {
		return Model.NewFDSError(err.Error(), -1)
	}

	for true {
		prefixArray := []string{}
		for _, k := range(listObjectResult.ObjectSummaries) {
			prefixArray = append(prefixArray, k.ObjectName)
		}

		err = c.Delete_Objects(bucketname, prefixArray)
		if err != nil {
			return Model.NewFDSError(err.Error(), -1)
		}

		if !listObjectResult.Truncated {
			break
		}
		listObjectResult, err = c.List_Next_Batch_Of_Objects(listObjectResult)
		if err != nil {
			return Model.NewFDSError(err.Error(), -1)
		}
	}

	return nil
}

// list_object_next
// set_bucket_acl
// get_bucket_acl
// get_object_acl
// generate_presigned_uri
// generate_download_object_uri


