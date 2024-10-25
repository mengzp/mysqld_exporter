// Copyright 2020 The Prometheus Authors
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

package collector

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const perfReplicationGroupMembersNodeQuery = `
  select member_state from performance_schema.replication_group_members where member_host=@@hostname;
 	`

// ScrapeReplicationGroupMembers collects from `performance_schema.replication_group_members`.
type ScrapePerfReplicationGroupNodeMembers struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfReplicationGroupNodeMembers) Name() string {
	return performanceSchema + ".replication_group_members_node"
}

// Help describes the role of the Scraper.
func (ScrapePerfReplicationGroupNodeMembers) Help() string {
	return "Collect metrics Node stafrom performance_schema.replication_group_members"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfReplicationGroupNodeMembers) Version() float64 {
	return 5.7
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfReplicationGroupNodeMembers) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger log.Logger) error {
	db := instance.getDB()
	perfReplicationGroupMembersRows, err := db.QueryContext(ctx, perfReplicationGroupMembersNodeQuery)
	if err != nil {
		return err
	}
	defer perfReplicationGroupMembersRows.Close()

	var (
		memberState    string
		memberStateInt float64
	)

	for perfReplicationGroupMembersRows.Next() {
		err = perfReplicationGroupMembersRows.Scan(
			&memberState,
		)
		if err != nil {
			return err
		}

		switch memberState {
		case "ONLINE":
			memberStateInt = 1
		case "RECOVERING":
			memberStateInt = 2
		case "OFFLINE":
			memberStateInt = 3
		case "ERROR":
			memberStateInt = 4
		case "UNREACHABLE":
			memberStateInt = 5
		default:
			memberStateInt = -1
		}
		var performanceSchemaReplicationGroupMembersMemberDesc = prometheus.NewDesc(
			prometheus.BuildFQName(namespace, performanceSchema, "replication_group_member_info_Node"),
			"Information about the replication group member node status",
			[]string{"MEMBER_STATE"}, nil,
		)

		ch <- prometheus.MustNewConstMetric(performanceSchemaReplicationGroupMembersMemberDesc,
			prometheus.GaugeValue, memberStateInt, "MEMBER_STATE")
	}
	return nil

}

// check interface
var _ Scraper = ScrapePerfReplicationGroupNodeMembers{}
