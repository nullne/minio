package cmd

import (
	"io/ioutil"
	"path"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/minio/cmd/logger"
	fv "github.com/minio/minio/cmd/volume"
)

var healRestoreFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "backup-root",
		Value: "/",
		Usage: "root of the backup index",
	},
	cli.StringFlag{
		Name:  "root",
		Value: "/index",
		Usage: "root of the index",
	},
}

var healRestoreIndexCmd = cli.Command{
	Name:        "restore-index",
	Usage:       "restore the index(rocksdb) from the backup",
	Flags:       healRestoreFlags,
	Action:      mainHealRestoreIndex,
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

func mainHealRestoreIndex(ctx *cli.Context) {
	if len(ctx.Args()) == 0 {
		cli.ShowCommandHelpAndExit(ctx, "restore-index", 1)
	}
	backupRoot := ctx.String("backup-root")
	root := ctx.String("root")
	if root == "" || backupRoot == "" {
	}
	// fmt.Println(ctx.Args(), len(ctx.Args()))
	// make sure the minio service is stopped
	for _, drive := range ctx.Args() {
		backupDir := path.Join(backupRoot, drive)
		// dir := path.Join(root, s)
		fileInfos, err := ioutil.ReadDir(backupDir)
		if err != nil {
			logger.Info("failed to read dir %s: %v", backupDir, err)
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
			if err := fv.CheckRocksDB(path.Join(root, drive, bucket)); err == nil {
				logger.Info("no need to restore %s\n", path.Join(root, drive, bucket))
				continue
			} else if strings.Contains(err.Error(), "Resource temporarily unavailable") {
				logger.Fatal(err, "rocksdb is running, please stop first")
			} else {
				logger.Info("rocksdb %s  got error: %v, is going to restore from backup\n", path.Join(root, drive, bucket), err)
			}
			if err := fv.RestoreRocksDBFromBackup(path.Join(backupDir, bucket), path.Join(root, drive, bucket)); err != nil {
				logger.Info("%+v", err)
			}
		}
	}
}
