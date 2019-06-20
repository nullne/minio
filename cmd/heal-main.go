package cmd

import (
	"fmt"

	"github.com/minio/cli"
)

var healCmd = cli.Command{
	Name:   "heal",
	Usage:  "heal the single minio node",
	Action: mainHeal,
	CustomHelpTemplate: `NAME:
   {{.HelpName}} - {{.Usage}}

USAGE:
   {{.HelpName}}{{if .VisibleFlags}} [FLAGS]{{end}}
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
   1. Heal the whole cluster:
      $ {{.HelpName}} /data{1..12}
`,
}

func mainHeal(ctx *cli.Context) {
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "heal", 1)
	}
	fmt.Println(ctx.Args(), len(ctx.Args()))
	for _, s := range ctx.Args() {
		p, err := newPosix(s)
		if err != nil {
			panic(err)
		}
		if err := p.RestoreIndex(); err != nil {
			panic(err)
		}
		p.Close()
	}
}
