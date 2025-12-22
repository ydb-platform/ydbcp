package common

import (
	"context"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func CreateGRPCClient(endpoint string) *grpc.ClientConn {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for range 5 {
		conn, err := grpc.NewClient(endpoint, opts...)
		if err == nil {
			return conn
		}
		time.Sleep(time.Second) // Wait before retrying
	}
	log.Panicln("failed to dial")
	return nil
}

func CreateGRPCClientWithOpts(endpoint string, opts []grpc.DialOption) *grpc.ClientConn {
	for range 5 {
		conn, err := grpc.NewClient(endpoint, opts...)
		if err == nil {
			return conn
		}
		time.Sleep(time.Second) // Wait before retrying
	}
	log.Panicln("failed to dial")
	return nil
}

func OpenYdb(endpoint string, database string) *ydb.Driver {
	dialTimeout := time.Second * 5
	opts := []ydb.Option{
		ydb.WithDialTimeout(dialTimeout),
		ydb.WithTLSSInsecureSkipVerify(),
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithAnonymousCredentials(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	driver, err := ydb.Open(ctx, endpoint+"/"+database, opts...)
	cancel()
	if err != nil {
		log.Panicf("failed to open database: %v", err)
	}
	return driver
}
