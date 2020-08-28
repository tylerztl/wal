//go:generate protoc --proto_path=. --gofast_out=plugins=grpc,paths=source_relative:. wal.proto

package wal
