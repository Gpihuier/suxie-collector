package openapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

type ResponseResult struct {
	Code         int         `json:"code"`
	Message      string      `json:"message"`
	ErrorDetails interface{} `json:"error_details"`
	RequestId    string      `json:"request_id"`
	ResponseTime string      `json:"response_time"`
	Data         interface{} `json:"data"`
	Total        int         `json:"total"`
}

// request 发起HTTP请求
func (o *OpenAPI) request(method, path string, params interface{}, headers map[string]string) (res map[string]interface{}, err error) {
	jsonByte, err := json.Marshal(params)
	if err != nil {
		err = fmt.Errorf("JSON marshal error: %v\n", err)
		return
	}

	client := &http.Client{}
	req, err := http.NewRequest(method, o.host+path, bytes.NewReader(jsonByte))
	if err != nil {
		err = fmt.Errorf("NewRequest error: %v\n", err)
		return
	}

	// set HTTP request headers
	for name, value := range headers {
		if req.Header.Get(name) == "" {
			req.Header.Add(name, value)
		} else {
			req.Header.Set(name, value)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("Response error: %v\n", err)
		return
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("close body reader error: %v", err)
		}
	}(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("ReadAll error: %v\n", err)
		return
	}

	if resp.StatusCode != 200 {
		err = fmt.Errorf("Response error, status code: %d, body: %s\n", resp.StatusCode, string(body))
		return
	}

	if !json.Valid(body) {
		err = errors.New("Response error, response body not a valid json\n")
		return
	}

	err = json.Unmarshal(body, &res)
	return
}
