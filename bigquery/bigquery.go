package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/tiketdatarisal/gcp/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"time"

	bq "google.golang.org/api/bigquery/v2"
)

type BigQuery struct {
	ctx     context.Context
	client  *bigquery.Client
	service *bq.Service
}

// NewBigQuery return a new BigQuery client.
func NewBigQuery(ctx context.Context, projectID string, credentialFile ...string) (*BigQuery, error) {
	var err error
	var client *bigquery.Client
	if len(credentialFile) > 0 {
		client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credentialFile[0]))
	} else {
		client, err = bigquery.NewClient(ctx, projectID)
	}
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrInitBigQueryClientFailed, err)
	}

	var service *bq.Service
	if len(credentialFile) > 0 {
		service, err = bq.NewService(ctx, option.WithCredentialsFile(credentialFile[0]))
	} else {
		service, err = bq.NewService(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrInitBigQueryClientFailed, err)
	}

	return &BigQuery{
		ctx:     ctx,
		client:  client,
		service: service,
	}, nil
}

// Close closes BigQuery client.
func (q BigQuery) Close() {
	if q.client != nil {
		_ = q.client.Close()
	}
}

// GetProjectNames return a list of project names.
func (q BigQuery) GetProjectNames() (shared.StringSlice, error) {
	var projectNames shared.StringSlice

	t := ""
	for {
		res, err := q.service.Projects.List().PageToken(t).Do()
		if err != nil {
			return nil, fmt.Errorf(errorWrapper, ErrGetProjectNamesFailed, err)
		}

		for _, p := range res.Projects {
			projectNames = append(projectNames, p.Id)
		}

		t = res.NextPageToken
		if t == "" {
			break
		}
	}

	return projectNames, nil
}

// GetDatasetNames return a list of dataset names.
func (q BigQuery) GetDatasetNames() (shared.StringSlice, error) {
	ctx, cancel := context.WithTimeout(q.ctx, timeoutDuration)
	defer cancel()

	datasetIterator := q.client.Datasets(ctx)
	var datasetNames shared.StringSlice
	for {
		dataset, err := datasetIterator.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, fmt.Errorf(errorWrapper, ErrGetDatasetNamesFailed, err)
		}

		datasetNames = append(datasetNames, dataset.DatasetID)
	}

	return datasetNames, nil
}

// GetTableNames return a list of table names.
func (q BigQuery) GetTableNames(datasetID string) (shared.StringSlice, error) {
	ctx, cancel := context.WithTimeout(q.ctx, timeoutDuration)
	defer cancel()

	tableIterator := q.client.Dataset(datasetID).Tables(ctx)
	var tableNames shared.StringSlice
	for {
		table, err := tableIterator.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, fmt.Errorf(errorWrapper, ErrGetTableNamesFailed, err)
		}

		tableNames = append(tableNames, table.TableID)
	}

	return tableNames, nil
}

// CreateTable create a new table with a schema.
func (q BigQuery) CreateTable(datasetID, tableID string, schema *bigquery.Schema) error {
	table := q.client.Dataset(datasetID).Table(tableID)
	err := table.Create(q.ctx, &bigquery.TableMetadata{
		Schema: *schema,
	})
	if err != nil {
		return fmt.Errorf(errorWrapper, ErrCreateTableFailed, err)
	}

	return nil
}

// DeleteTable delete an existing table.
func (q BigQuery) DeleteTable(datasetID, tableID string) error {
	table := q.client.Dataset(datasetID).Table(tableID)
	err := table.Delete(q.ctx)
	if err != nil {
		return fmt.Errorf(errorWrapper, ErrDeleteTableFailed, err)
	}

	return nil
}

// GetTableSchema return a schema from an existing table.
func (q BigQuery) GetTableSchema(datasetID, tableID string) (bigquery.Schema, error) {
	table := q.client.Dataset(datasetID).Table(tableID)
	meta, err := table.Metadata(q.ctx)
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrGetTableSchemaFailed, err)
	}

	return meta.Schema, nil
}

// InsertRows insert a new row to a table.
func (q BigQuery) InsertRows(datasetID, tableID string, items ...bigquery.ValueSaver) error {
	inserter := q.client.Dataset(datasetID).Table(tableID).Inserter()
	if err := inserter.Put(q.ctx, items); err != nil {
		return fmt.Errorf(errorWrapper, ErrInsertRowFailed, err)
	}

	return nil
}

// GetColumnMetadata returns columns metadata.
func (q BigQuery) GetColumnMetadata(datasetID, tableID string) (Columns, error) {
	table := q.client.Dataset(datasetID).Table(tableID)
	meta, err := table.Metadata(q.ctx)
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrGetColumnMetadataFailed, err)
	}

	var columns Columns
	for _, col := range meta.Schema {
		columns = append(columns, Column{ColumnName: col.Name, DataType: string(col.Type)})
	}

	return columns, nil
}

// DryRunQuery return number of bytes processed when succeeded.
func (q BigQuery) DryRunQuery(query string, timeout ...time.Duration) (int64, error) {
	if query == "" {
		return -1, nil
	}

	ctx := q.ctx
	var cancel context.CancelFunc
	if len(timeout) > 0 && timeout[0] > 0 {
		ctx, cancel = context.WithTimeout(q.ctx, timeout[0])
		defer func() {
			if cancel != nil {
				cancel()
			}
		}()
	}

	task := q.client.Query(query)
	task.DryRun = true

	job, err := task.Run(ctx)
	if err != nil {
		return -1, fmt.Errorf(errorWrapper, ErrDryRunQueryFailed, err)
	}

	if err = job.LastStatus().Err(); err != nil {
		return -1, fmt.Errorf(errorWrapper, ErrDryRunQueryFailed, err)
	}

	return job.LastStatus().Statistics.TotalBytesProcessed, nil
}

// RunQuery return query result when succeeded.
func (q BigQuery) RunQuery(query string, timeout ...time.Duration) (any, error) {
	if query == "" {
		return -1, nil
	}

	ctx := q.ctx
	var cancel context.CancelFunc
	if len(timeout) > 0 && timeout[0] > 0 {
		ctx, cancel = context.WithTimeout(q.ctx, timeout[0])
		defer func() {
			if cancel != nil {
				cancel()
			}
		}()
	}

	task := q.client.Query(query)
	queryIterator, err := task.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrRunQueryFailed, err)
	}

	type row = map[string]bigquery.Value
	var result []row
	for {
		var r row
		err = queryIterator.Next(&r)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return nil, fmt.Errorf(errorWrapper, ErrRunQueryFailed, err)
		}

		result = append(result, r)
	}

	return result, nil
}

// RunQueryFunc query and process the query result in func.
func (q BigQuery) RunQueryFunc(query string, f func(row map[string]bigquery.Value) error, timeout ...time.Duration) error {
	if query == "" || f == nil {
		return nil
	}

	ctx := q.ctx
	var cancel context.CancelFunc
	if len(timeout) > 0 && timeout[0] > 0 {
		ctx, cancel = context.WithTimeout(q.ctx, timeout[0])
		defer func() {
			if cancel != nil {
				cancel()
			}
		}()
	}

	task := q.client.Query(query)
	queryIterator, err := task.Read(ctx)
	if err != nil {
		return fmt.Errorf(errorWrapper, ErrRunQueryFailed, err)
	}

	for {
		// Get next row, when possible
		var r map[string]bigquery.Value
		err = queryIterator.Next(&r)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return fmt.Errorf(errorWrapper, ErrRunQueryFailed, err)
		}

		// Break the loop when the function return iterator.Done
		err = f(r)
		if err == iterator.Done {
			break
		}

		if err != nil {
			return fmt.Errorf(errorWrapper, ErrRunQueryFailed, err)
		}
	}

	return nil
}

func (q BigQuery) ExportToCsv(query, gcsURI string, retry int, delay time.Duration, timeout ...time.Duration) error {
	if query == "" {
		return nil
	}

	ctx := q.ctx
	var cancel context.CancelFunc
	if len(timeout) > 0 && timeout[0] > 0 {
		ctx, cancel = context.WithTimeout(q.ctx, timeout[0])
		defer func() {
			if cancel != nil {
				cancel()
			}
		}()
	}

	task := q.client.Query(query)
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

	config, err := result.Config()
	if err != nil {
		return err
	}

	var tmpTable *bigquery.Table
	if queryConfig, ok := config.(*bigquery.QueryConfig); ok {
		tmpTable = queryConfig.Dst
	}

	if tmpTable == nil {
		return ErrTemporaryTableNotFound
	}

	ref := bigquery.NewGCSReference(gcsURI)
	ref.DestinationFormat = bigquery.CSV
	ref.FieldDelimiter = commaDelimiter

	extractor := tmpTable.ExtractorTo(ref)
	extractor.DisableHeader = false

	var exportErr error
	for {
		exportErr := func() error {
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

		if exportErr != nil && retry > 0 {
			time.Sleep(delay)
			retry--
		} else {
			break
		}
	}

	return exportErr
}
