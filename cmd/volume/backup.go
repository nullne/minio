package volume

import "fmt"

var backupCh = make(chan func() error)

func init() {
	for fn := range backupCh {
		if err := fn(); err != nil {
			fmt.Println(err)
		}
	}
}
