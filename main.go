package main

import "database/sql"
import (
	"encoding/json"
	"fmt"
	"github.com/eawsy/aws-lambda-go-core/service/lambda/runtime"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	DD_ENABLED = false
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

func gatherTableMetrics(databaseName string, mysql sql.DB) (metricList []tableMetrics) { //todo, return error
	tblMetrics := tableMetrics{}
	var (
		rowCount, dataSize, totalSize, indexSize float64
	)
	log.Printf("Gathering metrics for database: %s", databaseName)
	rows, err := mysql.Query(
		"select table_schema,table_name,table_rows,data_length,index_length from tables where table_schema = ?",
		databaseName)
	if err != nil {
		log.Printf("Encountered error performing query, %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&tblMetrics.schemaName, &tblMetrics.tableName, &rowCount, &dataSize, &indexSize)
		totalSize = dataSize + indexSize
		tblMetrics.metrics = []Metric{
			{"row_count", rowCount},
			{"data_size", dataSize},
			{"index_size", indexSize},
			{"total_size", totalSize},
		}
		metricList = append(metricList, tblMetrics)
		log.Printf("schema: %s, name: %s, rows: %v, datassize: %.0f, indexsize: %v, totalsize: %.0f\n",
			tblMetrics.schemaName, tblMetrics.tableName,
			rowCount, dataSize, indexSize, totalSize)
	}
	return
}

func configCheck() {
	envs := []string{"DB_USER", "MYSQL_ROOT_PW", "DB_HOSTNAME", "ENVIRONMENT",
		"DD_API_KEY", "DD_APP_KEY", "DB_LIST", "DATADOG_ENABLED"}

	for _, env := range envs {
		if os.Getenv(env) == "" {
			log.Fatalf("Required Environment variable %s is not set", env)
		}
	}

	ddEnabled, err := strconv.ParseBool(os.Getenv("DATADOG_ENABLED"))
	if err != nil {
		log.Fatal("Could not parse value for DATADOG_ENABLED environment varibable")
	}
	DD_ENABLED = ddEnabled
	log.Printf("DATADOG_ENABLED set to %v", DD_ENABLED)
}

func process() {
	configCheck()
	con := connectionParams{
		user:      os.Getenv("DB_USER"),
		password:  os.Getenv("MYSQL_ROOT_PW"),
		hostName:  os.Getenv("DB_HOSTNAME"),
		port:      "3306",
		defaultDB: "information_schema",
	}
	mysqlConnection := establishDBConnection(con)
	dbNames := strings.Split(os.Getenv("DB_LIST"), ",")
	for _, dbName := range dbNames {
		metricList := gatherTableMetrics(dbName, mysqlConnection)
		if DD_ENABLED {
			postTableMetrics(metricList)
		}
	}
}

func Handle(evt json.RawMessage, ctx *runtime.Context) (interface{}, error) {
	process()
	return "done", nil
}

func main() {
	var evt = json.RawMessage{}
	var ctx = runtime.Context{}
	Handle(evt, &ctx)
}
