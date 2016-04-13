package galaxy_fds_sdk_golang

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/url"
	"strings"
	"time"
)

func getDateFromUrl(urlStr string) string {
	date := time.Now().Format(time.RFC1123)
	queryParams, err := url.ParseQuery(urlStr)
	if err != nil {
		return date
	}
	d, ok := queryParams["Expires"]
	if !ok {
		return date
	}
	if len(d) == 0 {
		return ""
	}
	return d[0]
}

func Signature(app_secret, method, u, content_md5, content_type string) (string) {
	var string_to_sign string
	var uri string
	date := getDateFromUrl(u)
	string_to_sign += method + "\n"
	string_to_sign += content_md5 + "\n"
	string_to_sign += content_type + "\n"
	string_to_sign += date + "\n"
	url_str, _ := url.ParseRequestURI(u)
	if strings.Contains(url_str.RequestURI(), "?") {
		uri_list := strings.Split(url_str.RequestURI(), "?")
		if uri_list[1] != "acl" {
			uri = uri_list[0]
		} else {
			uri = url_str.RequestURI()
		}
	} else {
		uri = url_str.RequestURI()
	}
	string_to_sign += uri
	// fmt.Println(string_to_sign)
	h := hmac.New(sha1.New, []byte(app_secret))
	h.Write([]byte(string_to_sign))
	b := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return b
}
