package volume

import "fmt"

var globalBackupQueue = make(chan func() error)

func init() {
	go func() {
		for fn := range globalBackupQueue {
			if err := fn(); err != nil {
				fmt.Println(err)
			}
		}
	}()
}
