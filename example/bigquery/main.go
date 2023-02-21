package main

import (
	"fmt"
	"github.com/tiketdatarisal/gcp/bigquery"
	"sort"
	"time"
)

func main() {
	client := bigquery.Singleton("tiket-0818", `path-to-credentials`)
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

	if columns, err := client.GetColumnMetadata("galaxy_dwh", "ancillary_flight_order"); err != nil {
		panic(err)
	} else {
		fmt.Println("Columns Metadata:", columns)
		fmt.Println()
	}

	if result, err := client.RunQuery("SELECT * FROM `tiket-0818.galaxy_dwh.ancillary_flight_order` LIMIT 10", nil); err != nil {
		panic(err)
	} else {
		fmt.Println("Query Result:", result)
	}

	if err := client.ExportToCsv("SELECT * FROM `tiket-0818.galaxy_dwh.ancillary_flight_order` LIMIT 10", nil, "gs://data_risal/sample.csv", 3, 2*time.Second); err != nil {
		panic(err)
	}

	if err := client.RunQueryToCSV("SELECT * FROM `tiket-0818.galaxy_dwh.ancillary_flight_order`", "gs://data_risal/exported/sample-*.csv"); err != nil {
		panic(err)
	}
}
