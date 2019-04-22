package volume

import "github.com/prometheus/client_golang/prometheus"

var (
	DiskOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "minio_disk_operation_duration_seconds",
			Help:    "Time taken by requests served by current Minio server instance",
			Buckets: []float64{.001, .003, .005, 0.02, .1, .5, 1},
		},
		[]string{"operation_type"},
	)
)

// func init() {
// 	prometheus.MustRegister(diskOperationDuration)
// }
