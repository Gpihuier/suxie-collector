package openapi

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func (o *OpenAPI) generateSign(params map[string]interface{}) (sign string, err error) {
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var qStrList []string
	for _, key := range keys {
		switch v := params[key].(type) {
		case string:
			qStrList = append(qStrList, fmt.Sprintf("%s=%s", key, v))
		default:
			var jsonV []byte
			jsonV, err = json.Marshal(v)
			if err != nil {
				return
			}
			qStrList = append(qStrList, fmt.Sprintf("%s=%s", key, string(jsonV)))
		}
	}

	md5Str := strings.ToUpper(fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(qStrList, "&")))))
	key := o.appId
	aesTool := NewAesTool([]byte(key), len(key))
	aesEncrypted, err := aesTool.ECBEncrypt([]byte(md5Str))
	if err != nil {
		return
	}

	sign = base64.StdEncoding.EncodeToString(aesEncrypted)
	return
}
