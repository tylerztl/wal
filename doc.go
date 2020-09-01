//go:generate protoc --proto_path=./pb --gofast_out=plugins=grpc,paths=source_relative:./pb wal.proto

package wal
