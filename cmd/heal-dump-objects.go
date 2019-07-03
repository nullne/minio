package cmd

import (
	"fmt"
	"io/ioutil"
	"path"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/volume"
)

var healDumpObjectsFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "index-root",
		Value: "/index",
		Usage: "root of the index",
	},
}

var healDumpObjectsCmd = cli.Command{
	Name:        "dump-objects",
	Usage:       "dump objects from rocksdb",
	Flags:       healDumpObjectsFlags,
	Action:      mainHealDumpObjects,
	Subcommands: []cli.Command{},
	CustomHelpTemplate: `NAME:
   {{.HelpName}} - {{.Usage}}

USAGE:
   {{.HelpName}}{{if .VisibleFlags}} [FLAGS]{{end}}
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
   1. restore the index from backup on the specified drives (same as the volume path when start minio server):
      $ {{.HelpName}} /data{1..12}
`,
}

func mainHealDumpObjects(ctx *cli.Context) {
	// only support one disk dump
	if len(ctx.Args()) != 1 {
		cli.ShowCommandHelpAndExit(ctx, "dump-objects", 1)
	}

	for _, drive := range ctx.Args() {
		indexRoot := ctx.String("index-root")
		dir := path.Join(indexRoot, drive)
		fileInfos, err := ioutil.ReadDir(dir)
		if err != nil {
			logger.Info("failed to read dir %s: %v", dir, err)
			continue
		}
		for _, info := range fileInfos {
			if !info.IsDir() {
				continue
			}
			bucket := info.Name()
			if isMinioMetaBucketName(bucket) {
				continue
			}
			ch, err := volume.DumpObjectsFromRocksDB(path.Join(dir, bucket))
			if err != nil {
				logger.Fatal(err, "failed to dump objects")
			}

			for key := range ch {
				// output to stdout
				fmt.Println(strings.Join([]string{bucket, key}, ","))
			}
		}
	}
}
