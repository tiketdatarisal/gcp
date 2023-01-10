package main

import (
	"fmt"
	"github.com/tiketdatarisal/gcp/bigquery"
	"sort"
)

func main() {
	client := bigquery.Singleton("tiket-0818", `path-to-credential`)
	if bigquery.Error != nil {
		panic(bigquery.Error)
	}

	if projects, err := client.GetProjectNames(); err != nil {
		panic(err)
	} else {
		fmt.Println("Project IDs:", projects)
		fmt.Println()
	}

	if datasets, err := client.GetDatasetNames(); err != nil {
		panic(err)
	} else {
		fmt.Println("Dataset IDs:", datasets)
		fmt.Println()
	}

	if tables, err := client.GetTableNames("galaxy_dwh"); err != nil {
		panic(err)
	} else {
		sort.Strings(tables)
		fmt.Println("Table Names:", tables)
		fmt.Println()
	}

	if columns, err := client.GetColumnMetadata("galaxy_dwh", "verified_active_user"); err != nil {
		panic(err)
	} else {
		fmt.Println("Columns Metadata:", columns)
		fmt.Println()
	}

	if result, err := client.RunQuery("SELECT * FROM `tiket-0818.galaxy_dwh.verified_active_user` LIMIT 10"); err != nil {
		panic(err)
	} else {
		fmt.Println("Query Result:", result)
	}
}
