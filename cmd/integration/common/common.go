package common

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"time"
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
