# galaxy-fds-sdk-golang
Golang SDK for Xiaomi File Data Storage.
# install
```
go get github.com/Shenjiaqi/galaxy-fds-sdk-golang
```
#example
```
package main

import (
	"fmt"
	"github.com/Shenjiaqi/galaxy-fds-sdk-golang"
)

const (
  BUCKET_NAME="test-bucket"
  REGION_NAME=""
  APP_KEY="YOUR_APP_KEY"
  SECRET_KEY="YOUR_SECRET_KEY"
)

func main() {
  objectName = "test_object"

	client = galaxy_fds_sdk_golang.NEWFDSClient(APP_KEY, SECRET_KEY,
		REGION_NAME, false, false)
	client.Create_Bucket(BUCKET_NAME)
	content := []byte("object content data")
	_, err := client.Put_Object(BUCKET_NAME, objectName, content, "", nil)
	fdsObject, err := client.Get_Object(BUCKET_NAME, objectName, 0, -1)
}

```
