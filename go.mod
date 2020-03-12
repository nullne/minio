module github.com/minio/minio

require (
	cloud.google.com/go v0.46.3
	github.com/Azure/azure-sdk-for-go v27.0.0+incompatible
	github.com/Azure/go-autorest v11.7.0+incompatible
	github.com/alecthomas/participle v0.2.1
	github.com/aliyun/aliyun-oss-go-sdk v0.0.0-20190307165228-86c17b95fcd5
	github.com/bcicen/jstream v0.0.0-20190220045926-16c1f8af81c2
	github.com/cheggaaa/pb v1.0.28
	github.com/coredns/coredns v1.4.0
	github.com/coreos/etcd v3.3.12+incompatible
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/djherbis/atime v1.0.0
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.1.2-0.20190322152051-20337d8c3947
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/fatih/color v1.7.0
	github.com/fatih/structs v1.1.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.1
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/gorilla/handlers v1.4.0
	github.com/gorilla/mux v1.7.0
	github.com/gorilla/rpc v1.2.0+incompatible
	github.com/hashicorp/go-version v1.1.0
	github.com/hashicorp/vault v1.1.0
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/jmhodges/levigo v1.0.0
	github.com/klauspost/pgzip v1.2.1
	github.com/klauspost/reedsolomon v1.9.1
	github.com/lib/pq v1.0.0
	github.com/mattn/go-isatty v0.0.7
	github.com/miekg/dns v1.1.8
	github.com/minio/blazer v0.0.0-20171126203752-2081f5bf0465
	github.com/minio/cli v1.21.0
	github.com/minio/dsync v1.0.0
	github.com/minio/highwayhash v1.0.0
	github.com/minio/lsync v1.0.1
	github.com/minio/mc v0.0.0-20190529152718-f4bb0b8850cb
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/minio/parquet-go v0.0.0-20190318185229-9d767baf1679
	github.com/minio/sha256-simd v0.1.1
	github.com/minio/sio v0.2.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/nats-io/go-nats-streaming v0.4.4
	github.com/nats-io/nats v1.7.2
	github.com/nsqio/go-nsq v1.0.7
	github.com/pingcap/go-ycsb v0.0.0-00010101000000-000000000000
	github.com/pkg/profile v1.3.0
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/rjeczalik/notify v0.9.2
	github.com/rs/cors v1.6.0
	github.com/segmentio/go-prompt v1.2.1-0.20161017233205-f0d19b6901ad
	github.com/skyrings/skyring-common v0.0.0-20160929130248-d1c0bb1cbd5e
	github.com/streadway/amqp v0.0.0-20190402114354-16ed540749f6
	github.com/syndtr/goleveldb v1.0.0
	github.com/tecbot/gorocksdb v0.0.0-20190519120508-025c3cf4ffb4
	github.com/tidwall/gjson v1.2.1
	github.com/tidwall/sjson v1.0.4
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	go.uber.org/atomic v1.3.2
	go.uber.org/multierr v1.1.0
	golang.org/x/crypto v0.0.0-20190820162420-60c769a6c586
	golang.org/x/net v0.0.0-20190827160401-ba9fcec4b297
	golang.org/x/sys v0.0.0-20190826190057-c7b8b68b1456
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/api v0.10.0
	gopkg.in/Shopify/sarama.v1 v1.20.0
	gopkg.in/bufio.v1 v1.0.0-20140618132640-567b2bfa514e
	gopkg.in/olivere/elastic.v5 v5.0.80
	gopkg.in/yaml.v2 v2.2.2
)

go 1.13

replace github.com/pingcap/go-ycsb => github.com/nullne/go-ycsb v0.0.0-20191104065842-7af12b260f54
