package cmd

import (
	"github.com/minio/cli"
)

var healFlags = []cli.Flag{}

var healCmd = cli.Command{
	Name:   "heal",
	Usage:  "heal the single minio node",
	Flags:  healFlags,
	Action: mainHeal,
	Subcommands: []cli.Command{
		healRestoreIndexCmd,
	},
	CustomHelpTemplate: `NAME:
   {{.HelpName}} - {{.Usage}}

USAGE:
   {{.HelpName}}{{if .VisibleFlags}} [FLAGS]{{end}}
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
`,
}

func mainHeal(ctx *cli.Context) error {
	cli.ShowCommandHelp(ctx, ctx.Args().First())
	return nil
}
