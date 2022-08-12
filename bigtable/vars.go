package bigtable

import (
	"errors"
	"sync"
)

const (
	errorWrapper = "%w: %v"
)

var (
	t     = map[string]*BigTable{}
	mutex sync.Mutex
	Error error

	ErrInitBigTableAdminClientFailed = errors.New("could not initialize Bigtable admin client")
	ErrInitBigTableClientFailed      = errors.New("could not initialize Bigtable client")
	ErrGetTableNamesFailed           = errors.New("could not get Bigtable table names")
	ErrCreateTableFailed             = errors.New("could not create Bigtable table")
	ErrDeleteTableFailed             = errors.New("could not delete Bigtable table")
	ErrGetFamilyNamesFailed          = errors.New("could not get Bigtable column family names")
	ErrCreateFamilyNameFailed        = errors.New("could not create Bigtable column family name")
	ErrAddRowFailed                  = errors.New("could not add a new Bigtable row")
	ErrReadRowByKeyFailed            = errors.New("could not read Bigtable row by its key")
	ErrReadRowsByKeysFailed          = errors.New("could not read Bigtable rows by its keys")
	ErrReadRowsByKeyPrefixFailed     = errors.New("could not read Bigtable rows by its key prefix")
	ErrReadRowsByKeyRangeFailed      = errors.New("could not read Bigtable rows by its key range")
)
