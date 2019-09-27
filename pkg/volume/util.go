package volume

import "os"

func MkdirIfNotExist(p string) error {
	if _, err := os.Stat(p); os.IsNotExist(err) {
		if err := os.MkdirAll(p, 0755); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}
