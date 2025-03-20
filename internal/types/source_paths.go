package types

import (
	"encoding/base64"
	"fmt"
	"strings"
	"unicode/utf8"
)

func ParseSourcePaths(str string) ([]string, error) {
	if str == "" {
		return nil, fmt.Errorf("got \"\" as source paths")
	}
	codedSlice := strings.Split(str, ",")
	hasEncoded, hasPlain := false, false
	slice := make([]string, len(codedSlice))
	for i, s := range codedSlice {
		data, err := base64.URLEncoding.DecodeString(s)
		if err != nil {
			slice[i] = s
			hasPlain = true
		} else {
			if utf8.ValidString(string(data)) {
				slice[i] = string(data)
				hasEncoded = true
			} else {
				slice[i] = s
				hasPlain = true
			}
		}
	}
	if hasEncoded && hasPlain {
		return nil, fmt.Errorf("got both encoded and plain source paths: \"%s\"", str)
	}
	return slice, nil
}

func SerializeSourcePaths(slice []string) string {
	codedSlice := make([]string, len(slice))
	for i, s := range slice {
		codedSlice[i] = base64.StdEncoding.EncodeToString([]byte(s))
	}
	return strings.Join(codedSlice, ",")
}
