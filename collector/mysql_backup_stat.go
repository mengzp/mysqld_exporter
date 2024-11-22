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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	tableBackupStatQuery = `
		select UNIX_TIMESTAMP(start_time) as start_timestamp,UNIX_TIMESTAMP(end_time) as end_timestamp, backup_type , backup_destination , 
		exit_state, lock_time  from mysql.backup_history where start_time>='%s' 
		order by start_time desc limit 1
		`
	read_fromdb_interval = 600
	tmp_path             = "/tmp/"
	backup_config_dir    = "/dbbkup/data/meb/backup/"
	backup_config_file   = "meb_backup_result.json"
)

// Metric descriptors.
var (
	mysqlBackupDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, mysqlSubsystem, "backup_stat"),
		"The stat of the backup from meb_backup_result.json",
		[]string{"info"}, nil,
	)
	// mysqlBackupSizeDesc = prometheus.NewDesc(
	// 	prometheus.BuildFQName(namespace, mysqlSubsystem, "backup_size"),
	// 	"The size of the backup from disk information",
	// 	nil, nil,
	// )
	// mysqlBackupTimeDesc = prometheus.NewDesc(
	// 	prometheus.BuildFQName(namespace, mysqlSubsystem, "backup_time"),
	// 	"The time of the backup spent",
	// 	[]string{"time_info"}, nil,
	//)
	// //"start_time", "end_time", "time_len", , "lock_time", "all_backup_size", "cur_backup_size"
)

// ScrapeTableSchema collects from `information_schema.tables`.
type ScrapeBackupStatSchema struct{}

// Name of the Scraper. Should be unique.
func (ScrapeBackupStatSchema) Name() string {
	return mysqlSubsystem + ".backupstat"

}

// Help describes the role of the Scraper.
func (ScrapeBackupStatSchema) Help() string {
	return "Collect metrics from mysql.backup_history"
}

// Version of MySQL from which scraper is available.
func (ScrapeBackupStatSchema) Version() float64 {
	return 5.1
}

func get_config_name(curdate string) (config_name string) {
	config_name = fmt.Sprintf("%s_tmp_meb_%s.json", tmp_path, curdate)
	return
}
func read_backupinfo(configfilename string) (map[string]interface{}, error) {
	file, _ := os.OpenFile(configfilename, os.O_CREATE|os.O_RDONLY, 0666)
	defer file.Close()
	//创建map，用于接收解码好的数据
	backup_info := make(map[string]interface{})
	//创建文件的解码器
	decoder := json.NewDecoder(file)
	//解码文件中的数据，丢入dataMap所在的内存
	err8 := decoder.Decode(&backup_info)
	if err8 == nil {
		fmt.Println(err8)
		return backup_info, err8
	}
	return nil, err8
}

func write_backupinfo(configfilename string, _backup_info map[string]interface{}) {
	backup_info := make(map[string]interface{})
	//将数据写入map
	backup_info["curdate"] = time.Now().Unix()
	backup_info["start_time"] = _backup_info["start_time"]
	backup_info["end_time"] = _backup_info["end_time"]
	backup_info["full_backup_size"] = _backup_info["full_backup_size"]
	backup_info["cur_backup_size"] = _backup_info["cur_backup_size"]
	backup_info["backup_type"] = _backup_info["backup_type"]
	backup_info["backup_status"] = _backup_info["backup_status"]
	backup_info["lock_time"] = _backup_info["lock_time"]
	file, _ := os.OpenFile(configfilename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	defer file.Close()
	//创建encoder 数据输出到file中
	encoder := json.NewEncoder(file)
	//把dataMap的数据encode到file中
	err := encoder.Encode(backup_info)
	//异常处理
	if err != nil {
		fmt.Println(err)
		return
	}
}

// func read_backup_fromdb(curdate string, ctx context.Context, instance *instance) (backup_info map[string]interface{}, err error) {

// 	//连接数据集
// 	db := instance.getDB()

// 	tableSchemaRows, err := db.QueryContext(ctx, fmt.Sprintf(tableBackupStatQuery, curdate))
// 	if err != nil {
// 		return
// 	}
// 	defer tableSchemaRows.Close()

// 	var (
// 		start_timestamp    int64
// 		end_timestamp      int64
// 		backup_type        string
// 		backup_destination string
// 		exit_state         string
// 		lock_time          float64
// 	)

//		if tableSchemaRows.Next() {
//			err = tableSchemaRows.Scan(
//				&start_timestamp,
//				&end_timestamp,
//				&backup_type,
//				&backup_destination,
//				&exit_state,
//				&lock_time,
//			)
//			if err != nil {
//				return
//			}
//			backup_info = make(map[string]interface{})
//			backup_info["start_time"] = start_timestamp
//			backup_info["end_time"] = end_timestamp
//			backup_info["backup_type"] = backup_type
//			backup_info["backup_destination"] = backup_destination
//			backup_info["exit_state"] = exit_state
//			backup_info["lock_time"] = lock_time
//			return
//		}
//		err = errors.New("null")
//		return
//	}
func read_backup_fromfile() (backup_info map[string]interface{}, err error) {

	backup_filename := fmt.Sprintf("%s%s", backup_config_dir, backup_config_file)
	if _, err := os.Stat(backup_filename); err != nil {
		fmt.Println("state")
		fmt.Println(err)
		return nil, err
	}
	file, err := os.OpenFile(backup_filename, os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println("openfile")
		fmt.Println(err)
		return nil, err
	}
	defer file.Close()
	//创建map，用于接收解码好的数据
	//backup_info = make(map[string]interface{})
	//创建文件的解码器
	decoder := json.NewDecoder(file)
	//解码文件中的数据，丢入dataMap所在的内存
	err8 := decoder.Decode(&backup_info)
	if err8 == nil {
		return backup_info, err8
	}
	fmt.Println("decode")
	return nil, err8
}

func pathsize(target_path string) (totalSize int64, err error) {
	err = filepath.Walk(target_path, func(path string, info os.FileInfo, err error) error {
		if info != nil && !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	return
}
func get_backup_size(path string) (cur_backup_size, full_backup_size int64, err error) {
	cur_backup_path := filepath.Dir(path)
	full_backup_path := filepath.Dir(cur_backup_path)
	cur_backup_size, err = pathsize(cur_backup_path)
	full_backup_size, err = pathsize(full_backup_path)
	return
}

func getTimestampdiff(timestamp1, timestamp2 int64) int {
	time1 := time.Unix(int64(timestamp1), 0)
	time2 := time.Unix(int64(timestamp2), 0)
	duration := time2.Sub(time1)
	return int(duration.Seconds())
}

func process_write_info(backup_info *map[string]interface{}, config_name string) {
	var err error
	*backup_info, err = read_backup_fromfile() //(curdate_t, ctx, instance)
	if err == nil {
		if *backup_info != nil && get_backup_item_str(*backup_info, "backup_status") == "1" {
			if _, ok := (*backup_info)["backup_destination"]; ok {
				cur_backup_size, full_backup_size, err := get_backup_size((*backup_info)["backup_destination"].(string))
				if err == nil {
					(*backup_info)["cur_backup_size"] = cur_backup_size
					(*backup_info)["full_backup_size"] = full_backup_size
				}
			}
			write_backupinfo(config_name, *backup_info)
		}
	} else {
		fmt.Println(err)
	}
}

func get_backup_item_str(backup_info map[string]interface{}, itemname string) string {
	if backup_info == nil {
		return ""
	}
	if backup_info[itemname] == nil {
		return ""
	}

	switch backup_info[itemname].(type) {
	case string:
		return backup_info[itemname].(string)
	case float64:
		return fmt.Sprintf("%d", (int64)(backup_info[itemname].(float64)))
		//return backup_info["start_time"].(float64)
	}
	return ""
}

func get_backup_item_float(backup_info map[string]interface{}, itemname string) float64 {
	if backup_info == nil {
		return 0
	}
	if backup_info[itemname] == nil {
		return 0
	}

	switch backup_info[itemname].(type) {

	case float64:
		return backup_info[itemname].(float64)
		//return backup_info["start_time"].(float64)
	}
	return 0
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeBackupStatSchema) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger log.Logger) error {
	year, month, day := time.Now().Date()
	curdate := fmt.Sprintf("%d%02d%02d", year, month, day)
	//var cur_time_from_zero time.Time
	//curdate_t := fmt.Sprintf("%d-%d-%d", year, month, day)
	config_name := get_config_name(curdate)
	backup_info, err := read_backupinfo(config_name)
	if err != nil || backup_info == nil {
		process_write_info(&backup_info, config_name)
	} else {
		last_backup_time := int64(backup_info["curdate"].(float64))
		currentTime := time.Now()
		time_diff := getTimestampdiff(last_backup_time, currentTime.Unix())
		if time_diff > read_fromdb_interval {
			process_write_info(&backup_info, config_name)
		}
	}
	var backup_stat float64
	var backup_type string
	var start_timestamp, end_timestamp, time_len, lock_time, backup_size float64
	backup_stat = 0 //失败
	backup_type = "0"

	if backup_info != nil {
		if get_backup_item_str(backup_info, "backup_status") == "1" {
			backup_stat = 1 //成功
		}
		if get_backup_item_str(backup_info, "backup_status") == "2" {
			backup_stat = 2 //失败
		}
		if backup_stat == 1 {
			start_timestamp = get_backup_item_float(backup_info, "start_time")

			start_time := time.Unix(int64(start_timestamp/1000)+24*60*60, 0)
			if time.Now().After(start_time) {
				//if curtime-int64(start_timestamp) >= 60*60*24 {
				backup_stat = 0
				start_timestamp = 0
			} else {
				end_timestamp = get_backup_item_float(backup_info, "end_time")

				time_len = end_timestamp - start_timestamp
				if get_backup_item_str(backup_info, "backup_type") == "1" {
					backup_type = "1" //1:全备 0:增备
				}
				lock_time = get_backup_item_float(backup_info, "lock_time")
				// cur_backup_size = get_backup_item_float(backup_info, "cur_backup_size")
				// full_backup_size = get_backup_item_float(backup_info, "full_backup_size")
				if backup_type == "1" {
					backup_size = get_backup_item_float(backup_info, "full_backup_size")
				} else {
					backup_size = get_backup_item_float(backup_info, "cur_backup_size")
				}
			}
		}
	}

	//"start_time", "end_time", "backup_type", "lock_time", "all_backup_size", "cur_backup_size"}

	// ch <- prometheus.MustNewConstMetric(
	// 	mysqlBackupStatDesc, prometheus.GaugeValue, float64(backup_stat),
	// 	start_timestamp, end_timestamp, time_len, backup_type, lock_time, full_backup_size, cur_backup_size,
	// )
	ch <- prometheus.MustNewConstMetric(
		mysqlBackupDesc, prometheus.GaugeValue, float64(backup_stat), "backup_state")
	ch <- prometheus.MustNewConstMetric(
		mysqlBackupDesc, prometheus.GaugeValue, float64(backup_size), "backup_size")

	// ch <- prometheus.MustNewConstMetric(
	// 	mysqlBackupStatDesc, prometheus.GaugeValue, float64(backup_stat), backup_type)
	// ch <- prometheus.MustNewConstMetric(
	// 	mysqlBackupSizeDesc, prometheus.GaugeValue, float64(full_backup_size), "full")
	// ch <- prometheus.MustNewConstMetric(
	// 	mysqlBackupSizeDesc, prometheus.GaugeValue, float64(cur_backup_size), "incr")
	ch <- prometheus.MustNewConstMetric(
		mysqlBackupDesc, prometheus.GaugeValue, float64(start_timestamp), "start_time")
	ch <- prometheus.MustNewConstMetric(
		mysqlBackupDesc, prometheus.GaugeValue, float64(end_timestamp), "end_time")
	ch <- prometheus.MustNewConstMetric(
		mysqlBackupDesc, prometheus.GaugeValue, float64(time_len), "time_len")
	ch <- prometheus.MustNewConstMetric(
		mysqlBackupDesc, prometheus.GaugeValue, float64(lock_time), "lock_time")

	return nil
}

// check interface
var _ Scraper = ScrapeBackupStatSchema{}
