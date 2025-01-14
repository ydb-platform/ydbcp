package types

import (
	"encoding/base64"
	"fmt"
	"strings"
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
			slice[i] = string(data)
			hasEncoded = true
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
