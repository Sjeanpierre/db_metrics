package main

import "database/sql"
import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/zorkian/go-datadog-api"
	"os"
	"fmt"
	"log"
	"time"
)

type tableMetrics struct {
	schemaName  string
	tableName string
	rowCount    float64
	dataSize    float64
	indexSize   float64
	totalSizeMB float64
}

type simpleMetric struct {
	name  string
	value float64
}

func establishDBConnection() sql.DB {
	user := "root"
	pass := os.Getenv("MYSQL_ROOT_PW")
	host := "localhost"
	port := "3306"
	mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/information_schema",user,pass,host,port)
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatalf("Could not establish DB connection, %s", err)
		os.Exit(1)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalf("Could not establish DB connection, %s", err)
	}
	return *db
}

func (metrics tableMetrics) reportableValues() []simpleMetric {
	v1 := simpleMetric{"row_count", metrics.rowCount}
	v2 := simpleMetric{"data_size", metrics.dataSize}
	v3 := simpleMetric{"index_size", metrics.indexSize}
	v4 := simpleMetric{"total_size", metrics.totalSizeMB}
	return []simpleMetric{v1, v2, v3, v4}
}

func gatherTableMetrics(databaseName string, mysql sql.DB) (metricList []tableMetrics) {
	metrics := tableMetrics{}
	rows, err := mysql.Query(
		"select table_schema,table_name,table_rows,data_length,index_length from tables where table_schema = ?",
		databaseName)
	if err != nil {
		log.Fatalf("Encountered error performing query, %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&metrics.schemaName, &metrics.tableName, &metrics.rowCount, &metrics.dataSize, &metrics.indexSize)
		metrics.totalSizeMB = float64(float64(metrics.dataSize + metrics.indexSize) / 1024 / 1024)
		metricList = append(metricList, metrics)
		fmt.Printf("schema: %s, name: %s, rows: %v, datassize: %.0f, indexsize: %v, totalsize: %.2f MB\n",
			metrics.schemaName, metrics.tableName,
			metrics.rowCount, metrics.dataSize, metrics.indexSize, metrics.totalSizeMB)
	}
	return
}

func metricPayload(metricGroup tableMetrics, timestamp float64) (payloads []datadog.Metric) {
	tableMetrics := metricGroup.reportableValues()
	for _, tableMetric := range tableMetrics {
		payloads = append(payloads, datadog.Metric{
			Metric: fmt.Sprintf("rds.db.table_metrics.%s", tableMetric.name),
			Points: []datadog.DataPoint{datadog.DataPoint{timestamp, tableMetric.value}},
			Host: "sjp.db.local",
			Tags: MetricTags(&metricGroup),
		})
	}
	return
}

func MetricTags(metricGroup *tableMetrics) []string {
	return []string{
		fmt.Sprintf("schema_name:%s", metricGroup.schemaName),
		fmt.Sprintf("table_name:%s", metricGroup.tableName),
		fmt.Sprintf("environment:%s","dev"),
		//fmt.Sprintf("db_hostname:%s", DB_HOST),
		//fmt.Sprintf("aws_account", AWS_ACCT),
	}
}

func reportTableMetrics(metricList []tableMetrics) {
	ddClient := datadog.NewClient(os.Getenv("DD_API_KEY"), os.Getenv("DD_APP_KEY"))
	timestamp := float64(time.Now().Unix())
	for _, metricGroup := range metricList {
		ddClient.PostMetrics(metricPayload(metricGroup,timestamp))
	}

}

func main() {
	mysqlConnection := establishDBConnection()
	metricList := gatherTableMetrics("counter_db", mysqlConnection)
	reportTableMetrics(metricList)
}