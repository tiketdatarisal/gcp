package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/tiketdatarisal/gcp/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

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
