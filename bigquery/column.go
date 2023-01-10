package bigquery

import (
	"fmt"
	"strings"
)

type Column struct {
	ColumnName string `json:"columnName,omitempty" bigquery:"column_name"`
	DataType   string `json:"dataType,omitempty"   bigquery:"data_type"`
}

type Columns []Column

func (c Columns) Len() int { return len(c) }

func (c Columns) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

func (c Columns) Less(i, j int) bool { return c[i].ColumnName < c[j].ColumnName }

func (c Columns) String() string {
	var cols []string
	for _, col := range c {
		cols = append(cols, fmt.Sprintf("%s(%s)", col.ColumnName, col.DataType))
	}

	return strings.Join(cols, ", ")
}
