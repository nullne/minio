
##### Instructions

1. install the rocksdb locally
2. run `make rocksdb-file-volume`  which generates two bins: `minio` and `rocksdb-file-volume.so`
3. follow the [distributed-minio-quickstart-guide](https://docs.min.io/docs/distributed-minio-quickstart-guide.html) to setup a minio cluster, make sure export following envirenment variables before starting the minio server:
   1. set envirenment variable `MINIO_FILE_VOLUME` to `on`
   2. set envirenment variable `MINIO_FILE_VOLUME_PLUGIN_PATH` to where you place `rocksdb-file-volume.so` on the node
   3. [option] set envirenment variable `MINIO_FILE_VOLUME_INDEX_ROOT` to a ssd path which will place db on it


