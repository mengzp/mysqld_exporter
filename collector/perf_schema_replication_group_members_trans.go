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
	"strings"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const perfReplicationGroupMembersTransQuery = `
        SELECT member_role, count_transactions_in_queue, COUNT_TRANSACTIONS_CHECKED, COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE
		FROM performance_schema.replication_group_member_stats a, performance_schema.replication_group_members b
		WHERE a.member_id = b.member_id and b.MEMBER_HOST = @@hostname
 	`

// ScrapeReplicationGroupMembers collects from `performance_schema.replication_group_members`.
type ScrapePerfReplicationGroupTrans struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfReplicationGroupTrans) Name() string {
	return performanceSchema + ".replication_group_trans"
}

// Help describes the role of the Scraper.
func (ScrapePerfReplicationGroupTrans) Help() string {
	return "Collect metrics trans from performance_schema.replication_group_members"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfReplicationGroupTrans) Version() float64 {
	return 5.7
}

func parseMemberRole(data string) float64 {
	dataString := strings.ToLower(data)
	switch dataString {
	case "secondary":
		return 2
	case "primary":
		return 1
	default:
		return 0
	}
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfReplicationGroupTrans) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger log.Logger) error {
	db := instance.getDB()
	perfReplicationGroupMembersRows, err := db.QueryContext(ctx, perfReplicationGroupMembersTransQuery)
	if err != nil {
		return err
	}
	defer perfReplicationGroupMembersRows.Close()

	var (
		memberRole                           string
		countTransactionsInQueue             float64
		countTransactionsChecked             float64
		countTransactionRemoteInApplierQueue float64
	)

	if perfReplicationGroupMembersRows.Next() {
		err = perfReplicationGroupMembersRows.Scan(
			&memberRole,
			&countTransactionsInQueue,
			&countTransactionsChecked,
			&countTransactionRemoteInApplierQueue,
		)
		if err != nil {
			return err
		}

		var performanceSchemaReplicationGroupMembersMemberDesc = prometheus.NewDesc(
			prometheus.BuildFQName(namespace, performanceSchema, "replication_group_trans"),
			"Information about the replication group member trans",
			[]string{"component"}, nil,
		)
		memberRoleInt := parseMemberRole(memberRole)

		ch <- prometheus.MustNewConstMetric(performanceSchemaReplicationGroupMembersMemberDesc,
			prometheus.GaugeValue, memberRoleInt, "member_role")

		ch <- prometheus.MustNewConstMetric(performanceSchemaReplicationGroupMembersMemberDesc,
			prometheus.GaugeValue, countTransactionsInQueue, "count_transactions_in_queue")
		ch <- prometheus.MustNewConstMetric(performanceSchemaReplicationGroupMembersMemberDesc,
			prometheus.GaugeValue, countTransactionsChecked, "count_transactions_checked")
		ch <- prometheus.MustNewConstMetric(performanceSchemaReplicationGroupMembersMemberDesc,
			prometheus.GaugeValue, countTransactionRemoteInApplierQueue, "count_transactions_remote_in_applier_queue")
	}
	return nil

}

// check interface
var _ Scraper = ScrapePerfReplicationGroupTrans{}
