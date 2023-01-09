package bigquery

import "strings"

type Column struct {
	ColumnName string `json:"columnName,omitempty" bigquery:"column_name"`
	DataType   string `json:"dataType,omitempty"   bigquery:"data_type"`
}

type Columns []Column

func (c *Columns) Sort() {
	count := len(*c)
	for i := 0; i < count-1; i++ {
		for j := 1; j < count; j++ {
			if strings.Compare((*c)[i].ColumnName, (*c)[j].ColumnName) > 0 {
				(*c)[i], (*c)[j] = (*c)[j], (*c)[i]
			}
		}
	}
}
