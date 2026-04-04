# Asinking OpenAPI Golang SDK

## 基本使用

```go
package main

import (
	"fmt"
	"log"
	"openapi"
)

func main() {
	o := openapi.NewClient("host", "appId", "appSecret")
	// 生成AccessToken，每次都会产生一次请求，所以请自行保存生成的AccessToken
	ato, err := o.GenerateAccessToken()
	if err != nil {
		log.Fatalf("generate access token error: %v", err)
	}
	
	// 可以自行将AccessToken保存到缓存中
	fmt.Println(ato.AccessToken)
	// RefreshToken用于续费AccessToken，只能使用一次
	fmt.Println(ato.RefreshToken)
	// AccessToken的有效期，TTL
	fmt.Println(ato.ExpiresIn)
	
	// 刷新AccessToken
	ato, err = o.RefreshToken(ato.RefreshToken)
    if err != nil {
    	log.Fatalf("refresh token error: %v", err)
    }

	// 手动设置AccessToken
	o.AccessToken = ato.AccessToken
	
	// 发起OpenAPI的请求
	params := make(map[string]interface{})
	params["foo"] = params["boo"]
	res, err := o.Request("routeName", "method like GET, POST, etc", params)
	if err != nil {
		log.Fatalf("request error: %v", err)
	}
	
	fmt.Println(res.Code)
	fmt.Println(res.Data)
	fmt.Println(res.Total)
	fmt.Println(res.Message)
	fmt.Println(res.RequestId)
	fmt.Println(res.ResponseTime)
	fmt.Println(res.ErrorDetails)
}
```