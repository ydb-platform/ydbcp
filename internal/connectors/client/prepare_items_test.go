package client

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Import"
	"testing"
	"ydbcp/internal/connectors/s3"
	"ydbcp/internal/types"
)

func deref(items []*Ydb_Import.ImportFromS3Settings_Item) []Ydb_Import.ImportFromS3Settings_Item {
	result := make([]Ydb_Import.ImportFromS3Settings_Item, len(items))
	for i, ptr := range items {
		result[i] = *ptr
	}
	return result
}

func TestPrepareItemsForImport(t *testing.T) {
	s3ObjectsMap := make(map[string]s3.Bucket)
	s3ObjectsMap["bucket"] = s3.Bucket{
		"local/table_1/scheme.pb": {
			Key: aws.String("local/table_1/scheme.pb"),
		},
		"local/table_2/scheme.pb": {
			Key: aws.String("local/table_2/scheme.pb"),
		},
		"local/folder/table_3/scheme.pb": {
			Key: aws.String("local/folder/table_3/scheme.pb"),
		},
	}

	s3Settings := types.ImportSettings{
		Bucket:       "bucket",
		BucketDbRoot: "local",
		SourcePaths:  nil,
	}

	s3Client := s3.NewMockS3Connector(s3ObjectsMap)

	items, err := prepareItemsForImport("/cluster/local", s3Client, s3Settings)

	assert.NoError(t, err)
	expected := []Ydb_Import.ImportFromS3Settings_Item{
		{
			Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
				SourcePrefix: "local/table_1/",
			},
			DestinationPath: "/cluster/local/table_1",
		},
		{
			Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
				SourcePrefix: "local/table_2/",
			},
			DestinationPath: "/cluster/local/table_2",
		},
		{
			Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
				SourcePrefix: "local/folder/table_3/",
			},
			DestinationPath: "/cluster/local/folder/table_3",
		},
	}
	assert.ElementsMatch(t, deref(items), expected)

	s3Settings.DestinationPath = "prefix"
	items, err = prepareItemsForImport("/cluster/local", s3Client, s3Settings)
	assert.NoError(t, err)

	expected = []Ydb_Import.ImportFromS3Settings_Item{
		{
			Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
				SourcePrefix: "local/table_1/",
			},
			DestinationPath: "/cluster/local/prefix/table_1",
		},
		{
			Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
				SourcePrefix: "local/table_2/",
			},
			DestinationPath: "/cluster/local/prefix/table_2",
		},
		{
			Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
				SourcePrefix: "local/folder/table_3/",
			},
			DestinationPath: "/cluster/local/prefix/folder/table_3",
		},
	}
	assert.ElementsMatch(t, deref(items), expected)

	s3Settings.SourcePaths = map[string]bool{"local/table_1": true, "local/folder/table_3": true}
	items, err = prepareItemsForImport("/cluster/local", s3Client, s3Settings)
	assert.NoError(t, err)

	expected = []Ydb_Import.ImportFromS3Settings_Item{
		{
			Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
				SourcePrefix: "local/table_1/",
			},
			DestinationPath: "/cluster/local/prefix/table_1",
		},
		{
			Source: &Ydb_Import.ImportFromS3Settings_Item_SourcePrefix{
				SourcePrefix: "local/folder/table_3/",
			},
			DestinationPath: "/cluster/local/prefix/folder/table_3",
		},
	}
	assert.ElementsMatch(t, deref(items), expected)

}
