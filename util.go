package galaxy_fds_sdk_golang

import (
	// "crypto/md5"
	"encoding/json"
	sJson "github.com/bitly/go-simplejson"
	// "io"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"strings"
	"bytes"
	"github.com/Shenjiaqi/galaxy-fds-sdk-golang/Model"
	"time"
	"crypto/md5"
	"net/url"
	"strconv"
)

const (
	DEFAULT_FDS_SERVICE_BASE_URI       = "http://files.fds.api.xiaomi.com/"
	DEFAULT_FDS_SERVICE_BASE_URI_HTTPS = "https://files.fds.api.xiaomi.com/"
	DEFAULT_CDN_SERVICE_URI            = "http://cdn.fds.api.xiaomi.com/"
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

var ALL_USERS = map[string]string{"id": "ALL_USERS"}
var AUTHENTICATED_USERS = map[string]string{"id": "AUTHENTICATED_USERS"}

var PRE_DEFINED_METADATA = []string{"cache-control",
	"content-encoding",
	"content-length",
	"content-md5",
	"content-type",
}

type FDSClient struct {
	App_key    string
	App_secret string
}

type FDSAuth struct {
	UrlBase      string
	Method       string
	Data         []byte
	Content_Md5  string
	Content_Type string
	Headers      map[string]string
	Params       map[string]string
}

func NEWFDSClient(App_key, App_secret string) *FDSClient {
	c := new(FDSClient)
	c.App_key = App_key
	c.App_secret = App_secret
	return c
}

func (c *FDSClient) Auth(auth FDSAuth) (*http.Response, error) {
	client := &http.Client{}

	urlParsed, err := url.Parse(auth.UrlBase)
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	for k, v := range(urlParsed.Query()) {
		if len(v) > 0 {
			params.Add(k, v[0])
		} else {
			params.Add(k, "")
		}
	}
	if len(auth.Params) > 0 {
		for k, v := range (auth.Params) {
			params.Add(k, v)
		}
	}
	urlParsed.RawQuery = params.Encode()
	urlStr := urlParsed.String()

	req, _ := http.NewRequest(auth.Method, urlStr, bytes.NewReader(auth.Data))
	for k, v := range auth.Headers {
		req.Header.Add(k, v)
	}
	req.Header.Add("date", time.Now().Format(time.RFC1123))
	req.Header.Add("content-md5", auth.Content_Md5)
	req.Header.Add("content-type", auth.Content_Type)

	signature, err := Signature(c.App_secret, req.Method, urlStr, req.Header)
	if err != nil {
		return nil, err
	}

	req.Header.Add("authorization", fmt.Sprintf("Galaxy-V2 %s:%s", c.App_key, signature))
	res, err := client.Do(req)
	return res, err
}

func (c *FDSClient) Is_Bucket_Exists(bucketname string) (bool, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "HEAD",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, errors.New(string(body))
	}
}

func (c *FDSClient) List_Bucket() ([]string, error) {
	bucketlist := []string{}
	url := DEFAULT_FDS_SERVICE_BASE_URI
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return bucketlist, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return bucketlist, err
	}
	if res.StatusCode == 200 {
		sj, err := sJson.NewJson(body)
		if err != nil {
			return bucketlist, err
		}
		buckets, _ := sj.Get("buckets").Array()
		for _, bucket := range buckets {
			// fmt.Printf("%#v\n", bucket.(map[string]interface{})["name"])
			bucket = bucket.(map[string]interface{})["name"]
			bucketlist = append(bucketlist, bucket.(string))
		}
		return bucketlist, nil
	} else {
		return bucketlist, errors.New(string(body))
	}
}

func (c *FDSClient) Create_Bucket(bucketname string) (bool, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, errors.New(string(body))
	}
}

func (c *FDSClient) Delete_Bucket(bucketname string) (bool, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "DELETE",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, errors.New(string(body))
	}
}

func (c *FDSClient) Is_Object_Exists(bucketname, objectname string) (bool, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname + DELIMITER + objectname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "HEAD",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else if res.StatusCode == 404 {
		return false, nil
	} else {
		return false, errors.New(string(body))
	}
}

func (c *FDSClient) Get_Object(bucketname, objectname string, position int64, size int64) (*Model.FDSObject, error) {
	if position < 0 {
		err := errors.New("Seek position should be no less than 0")
		return nil, err
	}
	url := DEFAULT_CDN_SERVICE_URI + bucketname + DELIMITER + objectname
	headers := map[string]string{}
	if position > 0 && size < 0 {
		headers["range"] = fmt.Sprintf("bytes=%d-", position)
	} else if position > 0 && size >= 0 {
		headers["range"] = fmt.Sprintf("bytes=%d-%d", position, position + size - 1)
	}
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      headers,
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusPartialContent {
		return &Model.FDSObject{
			BucketName: bucketname,
			ObjectName: objectname,
			Metadata:   *Model.NewFDSMetaData(res.Header),
			ObjectContent: body,
		}, nil
	} else {
		return nil, errors.New(string(body))
	}
}

// prefix需要改进
func (c *FDSClient) List_Object(bucketname, prefix, delimiter string, maxKeys int) (*Model.FDSObjectListing, error) {
	urlStr := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketname
	auth := FDSAuth{
		UrlBase:          urlStr,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
		Params:       map[string]string{
			"prefix": prefix,
			"delimiter": delimiter,
			"maxKeys": strconv.Itoa(maxKeys),
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode == 200 {
		return Model.NewFDSObjectListing(body)
	} else {
		return nil, errors.New(string(body))
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
	url := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketName

	auth := FDSAuth{
		UrlBase:      url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
		Params:       map[string]string {
			"prefix": prefix,
			"delimiter": delimiter,
			"maxKeys": strconv.Itoa(maxKeys),
			"marker": marker,
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode == 200 {
		return Model.NewFDSObjectListing(body)
	} else {
		return nil, errors.New(string(body))
	}
}

// v1类型
func (c *FDSClient) Post_Object(bucketname string, data []byte, filetype string) (string, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketname + DELIMITER
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
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return "", err
	}
	if res.StatusCode == 200 {
		sj, err := sJson.NewJson(body)
		if err != nil {
			return "", err
		}
		objectname, _ := sj.Get("objectName").String()
		return objectname, nil
	} else {
		return "", errors.New(string(body))
	}
}

// v2类型  自定义文件名 如果object已存在，将会覆盖
func (c *FDSClient) Put_Object(bucketname string, objectname string,
                               data []byte, contentType string) (*Model.PutObjectResult, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketname + DELIMITER + objectname
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	md5sum := fmt.Sprintf("%x", md5.Sum(data))
	auth := FDSAuth{
		UrlBase:       url,
		Method:       "PUT",
		Data:         data,
		Content_Md5:  md5sum,
		Content_Type: contentType,
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode == 200 {
		return Model.NewPutObjectResult(body)
	} else {
		return nil, errors.New(string(body))
	}
}

func (c *FDSClient) Delete_Object(bucketname, objectname string) (bool, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname + DELIMITER + objectname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "DELETE",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, errors.New(string(body))
	}
}

func (c *FDSClient) Rename_Object(bucketname, src_objectname, dst_objectname string) (bool, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname + DELIMITER + src_objectname +
		"?renameTo=" + dst_objectname
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, errors.New(string(body))
	}
}

func (c *FDSClient) Prefetch_Object(bucketname, objectname string) (bool, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname + DELIMITER + objectname + "?prefetch"
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, errors.New(string(body))
	}
}

func (c *FDSClient) Refresh_Object(bucketname, objectname string) (bool, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname + DELIMITER + objectname + "?refresh"
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         nil,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, errors.New(string(body))
	}
}

func (c *FDSClient) Set_Object_Acl(bucketname, objectname string, acl map[string]interface{}) (bool, error) {
	acp := make(map[string]interface{})
	acp["owner"] = map[string]string{"id": c.App_key}
	acp["accessControlList"] = []interface{}{acl}
	jsonString, _ := json.Marshal(acp)
	url := DEFAULT_FDS_SERVICE_BASE_URI + bucketname + DELIMITER + objectname + "?acl"
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         jsonString,
		Content_Md5:  "",
		Content_Type: "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return false, err
	}
	if res.StatusCode == 200 {
		return true, nil
	} else {
		return false, errors.New(string(body))
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
		return false, err
	}
	if !disable_prefetch {
		_, err := c.Prefetch_Object(bucketname, objectname)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (c *FDSClient) Init_MultiPart_Upload(bucketname, objectname string, contentType string) (*Model.InitMultipartUploadResult, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketname + DELIMITER + objectname
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
		Headers:      map[string]string{"x-xiaomi-estimated-object-size": "1000000000"},
		Params:       map[string]string {
			"uploads": "",
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}

	return Model.NewInitMultipartUploadResult(body)
}

func (c *FDSClient) Upload_Part(initUploadPartResult *Model.InitMultipartUploadResult, partnumber int, data []byte) (*Model.UploadPartResult, error) {
	bucketname := initUploadPartResult.BucketName
	objectname := initUploadPartResult.ObjectName
	uploadId   := initUploadPartResult.UploadId
	url := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketname + DELIMITER + objectname
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "PUT",
		Data:         data,
		Content_Md5:  "",
		Headers:      map[string]string{},
		Params:       map[string]string {
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
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}
	return Model.NewUploadPartResult(body)
}

func (c *FDSClient) Complete_Multipart_Upload(initPartuploadResult *Model.InitMultipartUploadResult, uploadPartResultList Model.UploadPartList) (*Model.PutObjectResult, error) {
	bucketName := initPartuploadResult.BucketName
	objectName := initPartuploadResult.ObjectName
	uploadId := initPartuploadResult.UploadId
	url := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketName + DELIMITER + objectName
	uploadPartResultListByteArray, err := json.Marshal(uploadPartResultList)
	if err != nil {
		return nil, err
	}
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "PUT",
		Data:         uploadPartResultListByteArray,
		Content_Md5:  "",
		Headers:      map[string]string{},
		Params:       map[string]string {
			"uploadId": uploadId,
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}
	return Model.NewPutObjectResult(body)
}


func (c *FDSClient) Get_Object_Meta(bucketname, objectname string) (*Model.FDSMetaData, error) {
	url := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketname +
	DELIMITER + objectname + "?metadata"
	auth := FDSAuth{
		UrlBase:          url,
		Method:       "GET",
		Data:         nil,
		Content_Md5:  "",
		Headers:      map[string]string{},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}
	return Model.NewFDSMetaData(res.Header), nil
}

func (c *FDSClient) Generate_Presigned_URI(bucketname, objectname, method string,
expiration int64, headers map[string][]string) (string, error) {
	urlStr := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketname + DELIMITER +
	objectname

	urlParsed, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}
	params := url.Values{}
	params.Add(GALAXY_ACCESS_KEY_ID, c.App_key)
	params.Add(EXPIRES, fmt.Sprintf("%d", expiration))
	urlParsed.RawQuery = params.Encode()
	signature, err := Signature(c.App_secret, method, urlParsed.String(), headers)
	if err != nil {
		return "", err
	}

	//params.Add(SIGNATURE, signature)
	//urlParsed.RawQuery = params.Encode()
	return urlParsed.String() + "&" + SIGNATURE + "=" + signature, nil
}

func (c *FDSClient) Delete_Objects(bucketname string, prefix []string) error {
	url := DEFAULT_FDS_SERVICE_BASE_URI_HTTPS + bucketname
	prefixJson, err := json.Marshal(prefix)
	if err != nil {
		return err
	}
	auth := FDSAuth{
		UrlBase:      url,
		Method:       "PUT",
		Data:         prefixJson,
		Content_Md5:  "",
		Headers:      map[string]string{},
		Params:       map[string]string {
			"deleteObjects": "",
		},
	}
	res, err := c.Auth(auth)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New(string(body))
	}
	return nil
}

func (c *FDSClient) Delete_Objects_With_Prefix(bucketname, prefix string) error {
	listObjectResult, err := c.List_Object(bucketname, prefix, "", DEFAULT_LIST_MAX_KEYS)
	if err != nil {
		return err
	}

	for true {
		prefixArray := []string{}
		for _, k := range(listObjectResult.ObjectSummaries) {
			prefixArray = append(prefixArray, k.ObjectName)
		}

		err = c.Delete_Objects(bucketname, prefixArray)
		if err != nil {
			return err
		}

		if !listObjectResult.Truncated {
			break
		}
		listObjectResult, err = c.List_Next_Batch_Of_Objects(listObjectResult)
		if err != nil {
			return err
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


