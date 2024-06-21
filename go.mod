module ydbcp

go 1.22

toolchain go1.22.1

require (
	github.com/google/uuid v1.6.0
	github.com/ydb-platform/ydb-go-sdk/v3 v3.74.2
	go.uber.org/zap v1.27.0
	google.golang.org/genproto v0.0.0-20240528184218-531527333157
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/golang-jwt/jwt/v4 v4.4.1 // indirect
	github.com/jonboulle/clockwork v0.3.0 // indirect
	github.com/ydb-platform/ydb-go-genproto v0.0.0-20240528144234-5d5a685e41f7 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sync v0.6.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
)
