package main

import (
	"github.com/zorkian/go-datadog-api"
	"fmt"
	"os"
	"log"
	"time"
)

func metricPayload(metricGroup tableMetrics, timestamp float64) (payloads []datadog.Metric) {
	tableMetrics := metricGroup.metrics
	for _, tableMetric := range tableMetrics {
		payloads = append(payloads, datadog.Metric{
			Metric: fmt.Sprintf("rds.db.table_metrics.%s", tableMetric.name),
			Points: []datadog.DataPoint{datadog.DataPoint{timestamp, tableMetric.value}},
			Host:   os.Getenv("DB_HOSTNAME"),
			Tags:   metricTags(&metricGroup),
		})
	}
	return
}

func metricTags(metricGroup *tableMetrics) []string {
	return []string{
		fmt.Sprintf("schema_name:%s", metricGroup.schemaName),
		fmt.Sprintf("table_name:%s", metricGroup.tableName),
		fmt.Sprintf("environment:%s", os.Getenv("ENVIRONMENT")),
		fmt.Sprintf("db_hostname:%s", os.Getenv("DB_HOSTNAME")),
		//fmt.Sprintf("aws_account", AWS_ACCT),
	}
}

func postTableMetrics(metricList []tableMetrics) {
	if len(metricList) < 1 {
		log.Print("No records returned")
		return
	}
	ddClient := datadog.NewClient(os.Getenv("DD_API_KEY"), os.Getenv("DD_APP_KEY"))
	timestamp := float64(time.Now().Unix())
	for _, metricGroup := range metricList {
		ddClient.PostMetrics(metricPayload(metricGroup, timestamp))
	}

}