module github.com/BeDreamCoder/wal

go 1.14

replace (
	github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)

require (
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dustin/go-humanize v0.0.0-20171111073723-bb3d318650d4
	github.com/golang/protobuf v1.4.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.13.0 // indirect
	github.com/stretchr/testify v1.4.0
	go.etcd.io/etcd v3.3.25+incompatible
	go.uber.org/zap v1.15.0
	golang.org/x/mod v0.1.1-0.20191107180719-034126e5016b // indirect
	golang.org/x/sys v0.0.0-20200826173525-f9321e4c35a6 // indirect
	golang.org/x/tools v0.0.0-20200207183749-b753a1ba74fa // indirect
	google.golang.org/protobuf v1.25.0 // indirect
)
