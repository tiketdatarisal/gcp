package bigquery

import "context"

// Singleton return instance of BigQuery for current project id.
func Singleton(projectID string, credentialFile ...string) *BigQuery {
	Error = nil
	mutex.Lock()
	defer mutex.Unlock()

	bq, exists := q[projectID]
	if exists && bq != nil {
		return bq
	}

	bq, err := NewBigQuery(context.Background(), projectID, credentialFile...)
	if err != nil {
		Error = err
		return nil
	}

	q[projectID] = bq
	return bq
}

// Close client connection to Google BigQuery API.
func Close() {
	if q != nil && len(q) > 0 {
		for _, bq := range q {
			bq.Close()
		}
	}
}
