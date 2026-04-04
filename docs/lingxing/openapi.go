package openapi

import (
	"encoding/json"
	"net/url"
	"strconv"
	"time"
)

type OpenAPI struct {
	host        string
	appId       string
	appSecret   string
	AccessToken string
}

type AccessTokenDto struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

func NewClient(host, appId, appSecret string) OpenAPI {
	return OpenAPI{
		host:      host,
		appId:     appId,
		appSecret: appSecret,
	}
}

func (o *OpenAPI) Request(routeName, method string, params map[string]interface{}) (res ResponseResult, err error) {
	u, err := url.Parse(routeName)
	if err != nil {
		return
	}
	q := u.Query()
	u.Path = "/erp" + u.Path

	// TODO 组装签名需要的参数
	qParams := make(map[string]interface{})
	qParams["access_token"] = o.AccessToken
	qParams["timestamp"] = time.Now().Unix()
	qParams["app_key"] = o.appId

	for k, v := range qParams {
		var value string
		switch x := v.(type) {
		case string:
			value = x
		case int:
			value = strconv.Itoa(x)
		case int64:
			value = strconv.Itoa(int(x))
		}
		if value != "" {
			q.Add(k, value)
		}
		params[k] = v
	}

	sign, err := o.generateSign(params)
	if err != nil {
		return
	}
	q.Add("sign", sign)
	u.RawQuery = q.Encode()

	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"

	resp, err := o.request(method, u.String(), params, headers)
	if err != nil {
		return
	}

	respJSONB, err := json.Marshal(resp)
	if err != nil {
		return
	}

	err = json.Unmarshal(respJSONB, &res)
	return
}

func (o *OpenAPI) GenerateAccessToken() (ato AccessTokenDto, err error) {
	path := "/api/auth-server/oauth/access-token"
	u, err := url.Parse(path)
	if err != nil {
		return
	}

	params := make(map[string]interface{})
	params["appId"] = o.appId
	params["appSecret"] = o.appSecret

	q := u.Query()
	for key, value := range params {
		q.Add(key, value.(string))
	}
	u.RawQuery = q.Encode()

	b, err := o.request("POST", u.String(), params, nil)
	if err != nil {
		return
	}

	// TODO 判断响应结果是否合法

	jsonByte, err := json.Marshal(b["data"])
	if err != nil {
		return
	}

	err = json.Unmarshal(jsonByte, &ato)
	return
}

func (o *OpenAPI) RefreshToken(refreshToken string) (ato AccessTokenDto, err error) {
	path := "/api/auth-server/oauth/refresh"
	u, err := url.Parse(path)
	if err != nil {
		return
	}

	params := make(map[string]interface{})
	params["appId"] = o.appId
	params["refreshToken"] = refreshToken

	q := u.Query()
	for key, value := range params {
		q.Add(key, value.(string))
	}
	u.RawQuery = q.Encode()

	b, err := o.request("POST", u.String(), params, nil)
	if err != nil {
		return
	}

	// TODO 判断响应结果是否合法

	jsonByte, err := json.Marshal(b["data"])
	if err != nil {
		return
	}

	err = json.Unmarshal(jsonByte, &ato)
	return
}
