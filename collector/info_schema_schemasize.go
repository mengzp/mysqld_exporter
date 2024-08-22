// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `information_schema.tables`.

package collector

import (
	"context"
	"fmt"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	tableSchemaSizeQuery = `
		SELECT
		    TABLE_SCHEMA,
			sum(table_rows) as SCHEMA_ROWS,
			sum(DATA_LENGTH) as SCHEMA_DATA_LENGTH,
			sum(INDEX_LENGTH) as SCHEMA_INDEX_LENGTH
		  FROM information_schema.tables
		  WHERE TABLE_SCHEMA = '%s' group by table_schema
		`
)

// Tunable flags.
var (
	tableSchemaSizeDatabases = kingpin.Flag(
		"collect.info_schema.schemasize.databases",
		"The list of databases to collect table stats for, or '*' for all",
	).Default("*").String()
)

// Metric descriptors.
var (
	infoSchemaSchemaSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "schema_size"),
		"The size of the schema from information_schema.tables",
		[]string{"schema", "component"}, nil,
	)
)

// ScrapeTableSchema collects from `information_schema.tables`.
type ScrapeSchemaSizeSchema struct{}

// Name of the Scraper. Should be unique.
func (ScrapeSchemaSizeSchema) Name() string {
	return informationSchema + ".schemasize"

}

// Help describes the role of the Scraper.
func (ScrapeSchemaSizeSchema) Help() string {
	return "Collect metrics from information_schema.schemasize"
}

// Version of MySQL from which scraper is available.
func (ScrapeSchemaSizeSchema) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeSchemaSizeSchema) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger log.Logger) error {
	var dbList []string
	db := instance.getDB()
	if *tableSchemaSizeDatabases == "*" {
		dbListRows, err := db.QueryContext(ctx, dbListQuery)
		if err != nil {
			return err
		}
		defer dbListRows.Close()

		var database string

		for dbListRows.Next() {
			if err := dbListRows.Scan(
				&database,
			); err != nil {
				return err
			}
			dbList = append(dbList, database)
		}
	} else {
		dbList = strings.Split(*tableSchemaSizeDatabases, ",")
	}

	for _, database := range dbList {
		tableSchemaRows, err := db.QueryContext(ctx, fmt.Sprintf(tableSchemaSizeQuery, database))
		if err != nil {
			return err
		}
		defer tableSchemaRows.Close()

		var (
			tableSchema string
			tableRows   uint64
			dataLength  uint64
			indexLength uint64
		)

		for tableSchemaRows.Next() {
			err = tableSchemaRows.Scan(
				&tableSchema,
				&tableRows,
				&dataLength,
				&indexLength,
			)
			if err != nil {
				return err
			}

			ch <- prometheus.MustNewConstMetric(
				infoSchemaSchemaSizeDesc, prometheus.GaugeValue, float64(tableRows),
				tableSchema, "table_rows",
			)
			ch <- prometheus.MustNewConstMetric(
				infoSchemaSchemaSizeDesc, prometheus.GaugeValue, float64(dataLength),
				tableSchema, "data_length",
			)
			ch <- prometheus.MustNewConstMetric(
				infoSchemaSchemaSizeDesc, prometheus.GaugeValue, float64(indexLength),
				tableSchema, "index_length",
			)
		}
	}

	return nil
}

// check interface
var _ Scraper = ScrapeSchemaSizeSchema{}
