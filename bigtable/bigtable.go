package bigtable

import (
	"cloud.google.com/go/bigtable"
	"context"
	"fmt"
	"github.com/tiketdatarisal/gcp/shared"
	"google.golang.org/api/option"
	"math"
)

type BigTable struct {
	ctx         context.Context
	adminClient *bigtable.AdminClient
	client      *bigtable.Client
}

// NewBigTable return a new BigTable client.
func NewBigTable(ctx context.Context, projectID, instance string, credentialFile ...string) (*BigTable, error) {
	var err error
	var adminClient *bigtable.AdminClient
	if len(credentialFile) > 0 {
		adminClient, err = bigtable.NewAdminClient(ctx, projectID, instance, option.WithCredentialsFile(credentialFile[0]))
	} else {
		adminClient, err = bigtable.NewAdminClient(ctx, projectID, instance)
	}

	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrInitBigTableAdminClientFailed, err)
	}

	var client *bigtable.Client
	if len(credentialFile) > 0 {
		client, err = bigtable.NewClient(ctx, projectID, instance, option.WithCredentialsFile(credentialFile[0]))
	} else {
		client, err = bigtable.NewClient(ctx, projectID, instance)
	}

	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrInitBigTableClientFailed, err)
	}

	return &BigTable{
		ctx:         ctx,
		adminClient: adminClient,
		client:      client,
	}, nil
}

// Close closes the BigTable client.
func (t BigTable) Close() {
	if t.client != nil {
		_ = t.client.Close()
	}

	if t.adminClient != nil {
		_ = t.adminClient.Close()
	}
}

// GetTableNames return a list of table names.
func (t BigTable) GetTableNames() (shared.StringSlice, error) {
	tableNames, err := t.adminClient.Tables(t.ctx)
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrGetTableNamesFailed, err)
	}

	return tableNames, nil
}

// CreateTable create a new table.
func (t BigTable) CreateTable(tableName string) error {
	tableNames, err := t.GetTableNames()
	if err != nil {
		return err
	} else if tableNames.Contains(tableName) {
		return nil
	}

	err = t.adminClient.CreateTable(t.ctx, tableName)
	if err != nil {
		return fmt.Errorf(errorWrapper, ErrCreateTableFailed, err)
	}

	return nil
}

// DeleteTable delete an existing table.
func (t BigTable) DeleteTable(tableName string) error {
	tableNames, err := t.GetTableNames()
	if err != nil {
		return err
	} else if !tableNames.Contains(tableName) {
		return nil
	}

	err = t.adminClient.DeleteTable(t.ctx, tableName)
	if err != nil {
		return fmt.Errorf(errorWrapper, ErrDeleteTableFailed, err)
	}

	return nil
}

// GetColumnFamilies return a list of column families from table.
func (t BigTable) GetColumnFamilies(tableName string) (shared.StringSlice, error) {
	tableInfo, err := t.adminClient.TableInfo(t.ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrGetFamilyNamesFailed, err)
	}

	var families shared.StringSlice
	for _, family := range tableInfo.FamilyInfos {
		families = append(families, family.Name)
	}

	return families, nil
}

// CreateColumnFamily create a new column family name.
func (t BigTable) CreateColumnFamily(tableName, columnFamilyName string) error {
	columnFamilies, err := t.GetColumnFamilies(tableName)
	if err != nil {
		return err
	} else if columnFamilies.Contains(columnFamilyName) {
		return nil
	}

	err = t.adminClient.CreateColumnFamily(t.ctx, tableName, columnFamilyName)
	if err != nil {
		return fmt.Errorf(errorWrapper, ErrCreateFamilyNameFailed, err)
	}

	return nil
}

// AddRow add a new row to a table.
func (t BigTable) AddRow(tableName, rowKey, columnFamily string, columns ColumnValueMap) (err error) {
	table := t.client.Open(tableName)
	mutation := bigtable.NewMutation()
	for columnName, value := range columns {
		mutation.Set(columnFamily, columnName, bigtable.Now(), value)
	}

	err = table.Apply(t.ctx, rowKey, mutation)
	if err != nil {
		return fmt.Errorf(errorWrapper, ErrAddRowFailed, err)
	}

	return nil
}

// ReadRow read a row from a table.
func (t BigTable) ReadRow(tableName, rowKey string, filters ...bigtable.Filter) (*bigtable.Row, error) {
	table := t.client.Open(tableName)

	var row bigtable.Row
	var err error
	if len(filters) > 0 {
		var opts []bigtable.ReadOption
		for _, filter := range filters {
			opts = append(opts, bigtable.RowFilter(filter))
		}

		row, err = table.ReadRow(t.ctx, rowKey, opts...)
	} else {
		row, err = table.ReadRow(t.ctx, rowKey)
	}

	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrReadRowByKeyFailed, err)
	}

	return &row, nil
}

// ReadRowsByKeys read a group of rows by its keys.
func (t BigTable) ReadRowsByKeys(tableName string, rowKeys []string, filters ...bigtable.Filter) ([]bigtable.Row, error) {
	table := t.client.Open(tableName)

	var rows []bigtable.Row
	var err error
	if len(filters) > 0 {
		var opts []bigtable.ReadOption
		for _, filter := range filters {
			opts = append(opts, bigtable.RowFilter(filter))
		}

		err = table.ReadRows(t.ctx, bigtable.RowList(rowKeys), func(row bigtable.Row) bool {
			rows = append(rows, row)
			return true
		}, opts...)
	} else {
		err = table.ReadRows(t.ctx, bigtable.RowList(rowKeys), func(row bigtable.Row) bool {
			rows = append(rows, row)
			return true
		})
	}

	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrReadRowsByKeysFailed, err)
	}

	return rows, nil
}

// ReadRowsByKeyPrefix read a group of rows by its key prefix.
func (t BigTable) ReadRowsByKeyPrefix(tableName string, keyPrefix string, filters ...bigtable.Filter) ([]bigtable.Row, error) {
	table := t.client.Open(tableName)

	var rows []bigtable.Row
	var err error
	if len(filters) > 0 {
		var opts []bigtable.ReadOption
		for _, filter := range filters {
			opts = append(opts, bigtable.RowFilter(filter))
		}

		err = table.ReadRows(t.ctx, bigtable.PrefixRange(keyPrefix), func(row bigtable.Row) bool {
			rows = append(rows, row)
			return true
		}, opts...)
	} else {
		err = table.ReadRows(t.ctx, bigtable.PrefixRange(keyPrefix), func(row bigtable.Row) bool {
			rows = append(rows, row)
			return true
		})
	}

	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrReadRowsByKeyPrefixFailed, err)
	}

	return rows, nil
}

// ReadRowsByKeyRange read a group if rows by its key range.
func (t BigTable) ReadRowsByKeyRange(tableName string, startKey, endKey string, filters ...bigtable.Filter) ([]bigtable.Row, error) {
	table := t.client.Open(tableName)

	var rows []bigtable.Row
	var err error
	if len(filters) > 0 {
		var opts []bigtable.ReadOption
		for _, filter := range filters {
			opts = append(opts, bigtable.RowFilter(filter))
		}

		err = table.ReadRows(t.ctx, bigtable.NewRange(startKey, endKey), func(row bigtable.Row) bool {
			rows = append(rows, row)
			return true
		}, opts...)
	} else {
		err = table.ReadRows(t.ctx, bigtable.NewRange(startKey, endKey), func(row bigtable.Row) bool {
			rows = append(rows, row)
			return true
		})
	}

	if err != nil {
		return nil, fmt.Errorf(errorWrapper, ErrReadRowsByKeyRangeFailed, err)
	}

	return rows, nil
}

// ReadRows read a number of rows from table.
func (t BigTable) ReadRows(tableName string, f func(row bigtable.Row), count int, rowSetOpt bigtable.RowSet, filters ...bigtable.Filter) error {
	max := math.MaxInt
	if count > 0 {
		max = count
	}

	if rowSetOpt == nil {
		rowSetOpt = bigtable.RowRange{}
	}

	current := 0
	table := t.client.Open(tableName)
	iterator := func(row bigtable.Row) bool {
		f(row)

		current = current + 1
		if current >= max {
			return false
		}

		return true
	}

	if len(filters) > 0 {
		var opts []bigtable.ReadOption
		for _, filter := range filters {
			opts = append(opts, bigtable.RowFilter(filter))
		}

		return table.ReadRows(t.ctx, rowSetOpt, iterator, opts...)
	} else {
		return table.ReadRows(t.ctx, rowSetOpt, iterator)
	}
}
