package types

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestParseSourcePaths(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
		message  string
	}{
		{
			name:     "Parse base64",
			input:    "L3Rlc3RpbmctZ2xvYmFsL2lhbQ==,L3BhdGgsd2l0aCxjb21tYXM=",
			expected: []string{"/testing-global/iam", "/path,with,commas"},
		},
		{
			name:     "Parse plaintext",
			input:    "hello,world",
			expected: []string{"hello", "world"},
		},
		{
			name:     "Empty input string",
			input:    "",
			expected: nil,
			message:  "got \"\" as source paths",
		},
		{
			name:     "mixed",
			input:    "L3Rlc3RpbmctZ2xvYmFsL2lhbQ==,aboba",
			expected: nil,
			message:  "got both encoded and plain source paths: \"L3Rlc3RpbmctZ2xvYmFsL2lhbQ==,aboba\"",
		},
		{
			name:     "plaintext",
			input:    "backup_table",
			expected: []string{"backup_table"},
		},
		{
			name:     "timezones",
			input:    "string,/dev/ydbcp/Timezones",
			expected: []string{"string", "/dev/ydbcp/Timezones"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSourcePaths(tt.input)
			if tt.expected != nil {
				if !reflect.DeepEqual(result, tt.expected) {
					t.Errorf("expected %v, got %v", tt.expected, result)
				}
			} else {
				assert.Equal(t, err.Error(), tt.message)
			}
		})
	}
}

func TestSerializeSourcePaths(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "Serialize base64",
			input:    []string{"hello", "world"},
			expected: "aGVsbG8=,d29ybGQ=",
		},
		{
			name:     "Serialize plaintext",
			input:    []string{"hello"},
			expected: "aGVsbG8=",
		},
		{
			name:     "Serialize empty slice",
			input:    []string{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SerializeSourcePaths(tt.input)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestSymmetry(t *testing.T) {
	tests := []struct {
		input []string
	}{
		{
			input: []string{"/testing-global/iam", "idk,,,strangepath"},
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			serialized := SerializeSourcePaths(tt.input)
			parsed, err := ParseSourcePaths(serialized)
			assert.Nil(t, err)
			if !reflect.DeepEqual(parsed, tt.input) {
				t.Errorf("expected %v, got %v", tt.input, parsed)
			}
		})
	}
}
