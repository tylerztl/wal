//go:generate protoc --proto_path=./log/walpb --gofast_out=plugins=grpc,paths=source_relative:./log/walpb wal.proto
//go:generate protoc --proto_path=./snap/snappb --gofast_out=plugins=grpc,paths=source_relative:./snap/snappb snap.proto

package wal
