package lingxing

import (
	"bytes"
	"crypto/aes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func GenerateSign(appID string, params map[string]any) (string, error) {
	if appID == "" {
		return "", fmt.Errorf("appID is empty")
	}

	keys := make([]string, 0, len(params))
	for k := range params {
		if strings.EqualFold(k, "sign") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		value, err := normalizeSignValue(params[key])
		if err != nil {
			return "", err
		}
		pairs = append(pairs, fmt.Sprintf("%s=%s", key, value))
	}

	raw := strings.Join(pairs, "&")
	md5HexUpper := strings.ToUpper(fmt.Sprintf("%x", md5.Sum([]byte(raw))))

	encrypted, err := aesECBEncryptPKCS7([]byte(md5HexUpper), []byte(appID))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(encrypted), nil
}

func normalizeSignValue(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case fmt.Stringer:
		return x.String(), nil
	case nil:
		return "", nil
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return "", fmt.Errorf("marshal sign value: %w", err)
		}
		return string(b), nil
	}
}

func aesECBEncryptPKCS7(src, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}

	src = pkcs7Padding(src, block.BlockSize())
	out := make([]byte, len(src))
	for bs, be := 0, block.BlockSize(); bs < len(src); bs, be = bs+block.BlockSize(), be+block.BlockSize() {
		block.Encrypt(out[bs:be], src[bs:be])
	}
	return out, nil
}

func pkcs7Padding(src []byte, blockSize int) []byte {
	padding := blockSize - len(src)%blockSize
	pad := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(src, pad...)
}
