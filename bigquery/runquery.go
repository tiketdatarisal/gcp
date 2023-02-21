package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/tiketdatarisal/gcp/bigquery/config"
	"time"
)

// RunQueryToCSV query and store the result to CSV file.
// Use wildcard (*) when you want to save to multiple files.
// For example: gcsURI = "gs://bucket/sample-*.csv" will save to "sample-000000000000.csv",
// "sample-000000000001.csv", etc.
func (q BigQuery) RunQueryToCSV(query, gcsURI string, cfg ...config.RunQueryConfig) error {
	if query == "" || gcsURI == "" {
		return nil
	}

	// Get config from parameter
	c := config.InitRunQueryConfig(cfg...)

	// Initialize context with timeout when possible
	ctx := q.ctx
	var cancel context.CancelFunc
	if c.Timeout > 0 {
		ctx, cancel = context.WithTimeout(q.ctx, c.Timeout)
		defer func() {
			if cancel != nil {
				cancel()
			}
		}()
	}

	// Initialize task with labels when possible
	task := q.client.Query(query)
	if c.Labels != nil {
		task.Labels = c.Labels
	}

	// Run the query job and wait for result
	result, err := task.Run(ctx)
	if err != nil {
		return err
	}

	status, err := result.Wait(ctx)
	if err != nil {
		return err
	} else if err := status.Err(); err != nil {
		return err
	}

	// Get temporary table from the result
	resConfig, err := result.Config()
	if err != nil {
		return err
	}

	var tmpTable *bigquery.Table
	if queryConfig, ok := resConfig.(*bigquery.QueryConfig); ok {
		tmpTable = queryConfig.Dst
	}

	if tmpTable == nil {
		return ErrTemporaryTableNotFound
	}

	// Prepare to export, initialize table extractor
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.DestinationFormat = bigquery.CSV
	gcsRef.FieldDelimiter = c.Delimiter
	if c.Compressed {
		gcsRef.Compression = bigquery.Gzip
	}

	extractor := tmpTable.ExtractorTo(gcsRef)
	extractor.DisableHeader = c.DisableHeader
	if c.Labels != nil {
		extractor.Labels = c.Labels
	}

	retry := c.Retry
	var extractErr error = nil
	for {
		extractErr := func() error {
			result, err := extractor.Run(ctx)
			if err != nil {
				return err
			}

			status, err := result.Wait(ctx)
			if err != nil {
				return err
			} else if err := status.Err(); err != nil {
				return err
			}

			return nil
		}()

		if extractErr != nil && retry > 0 {
			time.Sleep(c.Delay)
			retry--
		} else {
			break
		}
	}

	return extractErr
}

// RunQueryToJSON query and store the result to JSON file.
// Use wildcard (*) when you want to save to multiple files.
// For example: gcsURI = "gs://bucket/sample-*.json" will save to "sample-000000000000.json",
// "sample-000000000001.json", etc.
func (q BigQuery) RunQueryToJSON(query, gcsURI string, cfg ...config.RunQueryConfig) error {
	if query == "" || gcsURI == "" {
		return nil
	}

	// Get config from parameter
	c := config.InitRunQueryConfig(cfg...)

	// Initialize context with timeout when possible
	ctx := q.ctx
	var cancel context.CancelFunc
	if c.Timeout > 0 {
		ctx, cancel = context.WithTimeout(q.ctx, c.Timeout)
		defer func() {
			if cancel != nil {
				cancel()
			}
		}()
	}

	// Initialize task with labels when possible
	task := q.client.Query(query)
	if c.Labels != nil {
		task.Labels = c.Labels
	}

	// Run the query job and wait for result
	result, err := task.Run(ctx)
	if err != nil {
		return err
	}

	status, err := result.Wait(ctx)
	if err != nil {
		return err
	} else if err := status.Err(); err != nil {
		return err
	}

	// Get temporary table from the result
	resConfig, err := result.Config()
	if err != nil {
		return err
	}

	var tmpTable *bigquery.Table
	if queryConfig, ok := resConfig.(*bigquery.QueryConfig); ok {
		tmpTable = queryConfig.Dst
	}

	if tmpTable == nil {
		return ErrTemporaryTableNotFound
	}

	// Prepare to export, initialize table extractor
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.DestinationFormat = bigquery.JSON
	if c.Compressed {
		gcsRef.Compression = bigquery.Gzip
	}

	extractor := tmpTable.ExtractorTo(gcsRef)
	if c.Labels != nil {
		extractor.Labels = c.Labels
	}

	retry := c.Retry
	var extractErr error = nil
	for {
		extractErr := func() error {
			result, err := extractor.Run(ctx)
			if err != nil {
				return err
			}

			status, err := result.Wait(ctx)
			if err != nil {
				return err
			} else if err := status.Err(); err != nil {
				return err
			}

			return nil
		}()

		if extractErr != nil && retry > 0 {
			time.Sleep(c.Delay)
			retry--
		} else {
			break
		}
	}

	return extractErr
}
