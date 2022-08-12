package bigtable

import (
	"context"
	"fmt"
)

func bigTableInstanceName(projectID, instance string) string {
	return fmt.Sprintf("%s.%s", projectID, instance)
}

// Singleton return instance of BigTable for current project id and instance.
func Singleton(projectID, instance string, credentialFile ...string) *BigTable {
	Error = nil
	mutex.Lock()
	defer mutex.Unlock()

	bigTableInstance := bigTableInstanceName(projectID, instance)
	bt, exists := t[bigTableInstance]
	if exists && bt != nil {
		return bt
	}

	bt, err := NewBigTable(context.Background(), projectID, instance, credentialFile...)
	if err != nil {
		Error = err
		return nil
	}

	t[bigTableInstance] = bt
	return bt
}

// Close client connection to Google BigTable API.
func Close() {
	if t != nil && len(t) > 0 {
		for _, bt := range t {
			bt.Close()
		}
	}
}
