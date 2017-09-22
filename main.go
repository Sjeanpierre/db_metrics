package main

import "database/sql"
import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/zorkian/go-datadog-api"
	"log"
	"os"
	"time"
)

//Table Metrics is a container around metrics, a Schema Name and Table name has multiple metrics
type tableMetrics struct {
	schemaName, tableName string
	metrics               []Metric
}

//Metric is an individual metric point from a table
type Metric struct {
	name  string
	value float64
}

//Connection Params are used to store information needed to form MYSql DSN
type connectionParams struct {
	user, password, hostName, port, defaultDB string
}

//MySql DNS function takes a pointer to a connectionParams struct and returns a formatted DSN for MySQL
func (con *connectionParams) mysqlDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		con.user,
		con.password,
		con.hostName,
		con.port,
		con.defaultDB)
}

// Establish DB connection takes connection parameters and establishes a connection to MySQl
// The connection is then validated using the Ping function from the MySQL lib
func establishDBConnection(connectionDetails connectionParams) sql.DB {
	db, err := sql.Open("mysql", connectionDetails.mysqlDSN())
	if err != nil {
		log.Fatalf("Could not establish DB connection, %s", err) //todo, return error message up to caller
		os.Exit(1)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalf("Could not establish DB connection, %s", err) //todo, same as above
	}
	return *db
}

func gatherTableMetrics(databaseName string, mysql sql.DB) (metricList []tableMetrics) {
	tblMetrics := tableMetrics{}
	var (
		rowCount, dataSize, totalSizeMB, indexSize float64
	)
	log.Printf("Gathering metrics for database: %s", databaseName)
	rows, err := mysql.Query(
		"select table_schema,table_name,table_rows,data_length,index_length from tables where table_schema = ?",
		databaseName)
	if err != nil {
		log.Fatalf("Encountered error performing query, %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&tblMetrics.schemaName, &tblMetrics.tableName, &rowCount, &dataSize, &indexSize)
		totalSizeMB = float64(float64(dataSize+indexSize) / 1024 / 1024)
		tblMetrics.metrics = []Metric{
			{"row_count", rowCount},
			{"data_size", dataSize},
			{"index_size", indexSize},
			{"total_size", totalSizeMB},
		}
		metricList = append(metricList, tblMetrics)
		fmt.Printf("schema: %s, name: %s, rows: %v, datassize: %.0f, indexsize: %v, totalsize: %.2f MB\n",
			tblMetrics.schemaName, tblMetrics.tableName,
			rowCount, dataSize, indexSize, totalSizeMB)
	}
	return
}

func metricPayload(metricGroup tableMetrics, timestamp float64) (payloads []datadog.Metric) {
	tableMetrics := metricGroup.metrics
	for _, tableMetric := range tableMetrics {
		payloads = append(payloads, datadog.Metric{
			Metric: fmt.Sprintf("rds.db.table_metrics.%s", tableMetric.name),
			Points: []datadog.DataPoint{datadog.DataPoint{timestamp, tableMetric.value}},
			Host:   "sjp.db.local", //todo, add correct var
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
		//fmt.Sprintf("db_hostname:%s", DB_HOST),
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

func configCheck() {
	envs := []string{"DB_USER", "MYSQL_ROOT_PW", "DB_HOSTNAME", "ENVIRONMENT", "DD_API_KEY", "DD_APP_KEY"}
	for _, env := range envs {
		if os.Getenv(env) == "" {
			log.Fatalf("Required Environment variable %s is not set", env)
		}
	}
}

func main() {
	configCheck()
	con := connectionParams{
		user:      os.Getenv("DB_USER"),
		password:  os.Getenv("MYSQL_ROOT_PW"),
		hostName:  os.Getenv("DB_HOSTNAME"),
		port:      "3306",
		defaultDB: "information_schema",
	}
	mysqlConnection := establishDBConnection(con)
	metricList := gatherTableMetrics("counter_db", mysqlConnection)
	_ = metricList
	//postTableMetrics(metricList)
}
