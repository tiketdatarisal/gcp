package bigquery

import (
	"errors"
	"sync"
	"time"
)

const (
	timeoutDuration = 30 * time.Second
	errorWrapper    = "%w: %v"
	commaDelimiter  = ","
)

var (
	q     = map[string]*BigQuery{}
	mutex sync.Mutex
	Error error

	ErrInitBigQueryClientFailed = errors.New("could not initialize BigQuery client")
	ErrGetProjectNamesFailed    = errors.New("could not get BigQuery project names")
	ErrGetDatasetNamesFailed    = errors.New("could not get BigQuery dataset names")
	ErrGetTableNamesFailed      = errors.New("could not get BigQuery table names")
	ErrGetColumnMetadataFailed  = errors.New("could not get BigQuery column metadata")
	ErrTemporaryTableNotFound   = errors.New("could not found temporary table")
	ErrCreateTableFailed        = errors.New("could not create BigQuery table")
	ErrDeleteTableFailed        = errors.New("could not delete BigQuery table")
	ErrGetTableSchemaFailed     = errors.New("could not get BigQuery table schema")
	ErrInsertRowFailed          = errors.New("could not insert new row to BigQuery table")
	ErrDryRunQueryFailed        = errors.New("could not dry run query")
	ErrRunQueryFailed           = errors.New("could not run query")
)
