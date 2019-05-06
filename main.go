package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sjeanpierre/SJP_Go_Packages/lib/sumologic"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	sumoSourceCatFormat = "lsm/global/db-metrics/table_size_tracking/%s"
	sumoNameFormat      = "%s/%s" //Name={DB Instance Identifier}/{Table Name}
	sumoCollectorURL    string
	dbConnectionTimeout = time.Second * 5
	DEBUG, _            = strconv.ParseBool(os.Getenv("DEBUG"))
)

type credentialSet struct {
	user     string
	password string
}

//Table Metrics is a container around metrics, a Schema Name and Table name has multiple metrics
type tableMetrics struct {
	schemaName, tableName string
	metrics               []metric
}

//metric is an individual data point from a table
type metric struct {
	name  string
	value float64
}

//Connection Params are used to store information needed to form MYSql DSN
type connectionParams struct {
	user, password, hostName, port, defaultDB string
}

//takes data_length in bytes and returns data_length in MB rounded to the nearest 100th
func roundedMB(x float64) float64 {
	return round(byte2MB(x))
}

//takes size representation in bytes and returns size in MBs
func byte2MB(b float64) float64 {
	return b / (1024 * 1024)
}

//rounds x to the nearest 100th
func round(x float64) float64 {
	return math.Round(x/0.02) * 0.02
}

//checks and sets config for app to run, including validating env vars
func setConfig() {
	collectorURL, ok := os.LookupEnv("SUMO_HOSTED_COLLECTOR_URL")
	if !ok || collectorURL == "" || collectorURL == " " {
		log.Fatalln("SUMO_HOSTED_COLLECTOR_URL env var is not set")
	}
	sumoCollectorURL = collectorURL
}

//sends table metrics to sumo with custom headers
func sendMetrics2Sumo(metrics tableMetrics, instance rDSInstance) {
	headers := prepSumoHeaders(metrics.schemaName, instance)
	c := sumologic.NewUploaderWithHeaders(sumoCollectorURL)
	data := prepDataForSumo(metrics, instance.name)
	if DEBUG {
		log.Printf("%+v", data)
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("could not marshall data for %s into JSON, error : %s", instance.name, err)
	}
	err = c.SendWithHeaders(jsonData, "", headers)
	if err != nil {
		log.Printf("[Error] - encountered issue sending metrics to" +
			" Sumo for schema %s on host %s, %s",metrics.schemaName,instance.name,err)
	}
}

func prepDataForSumo(metrics tableMetrics, instanceName string) map[string]string {
	x := make(map[string]string)
	x["table_name"] = metrics.tableName
	x["schema_name"] = metrics.schemaName
	x["instance_name"] = instanceName
	for _, metric := range metrics.metrics {
		x[metric.name] = fmt.Sprintf("%0.0f", metric.value)
	}
	return x
}

func prepSumoHeaders(schemaName string, instance rDSInstance) sumologic.HeaderSet {
	var h sumologic.HeaderSet
	h.Headers = make(map[string]string)
	h.Headers["X-Sumo-Name"] = fmt.Sprintf(sumoNameFormat, instance.name, schemaName)
	h.Headers["X-Sumo-Host"] = *instance.Endpoint.Address
	h.Headers["X-Sumo-Category"] = fmt.Sprintf(sumoSourceCatFormat, instance.name)
	return h
}

//MySql DNS function takes a pointer to a connectionParams struct and returns a formatted DSN for MySQL
func (con *connectionParams) mysqlDSN() string {
	c := mysql.Config{
		User:    con.user,
		Passwd:  con.password,
		DBName:  con.defaultDB,
		Addr:    fmt.Sprintf("%s:%s", con.hostName, con.port),
		Net:     "tcp",
		Timeout: dbConnectionTimeout,
	}
	return c.FormatDSN()
}

// Establish DB connection takes connection parameters and establishes a connection to MySQl
// The connection is then validated using the Ping function from the MySQL lib
func connect(connectionDetails connectionParams) (sql.DB, error) {
	db, err := sql.Open("mysql", connectionDetails.mysqlDSN())
	if err != nil {
		return sql.DB{}, fmt.Errorf("could not establish DB connection, %s", err)
	}
	log.Print("Pinging DB to check connection details")
	err = db.Ping()
	if err != nil {
		return sql.DB{}, fmt.Errorf("could not establish DB connection, %s", err)
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return *db, nil
}

//performs query against performance_schema table for given schemaName for key statistics
//logs retrieved data and returns tableMetrics struct
func fetchMetrics(schemaName string, mysql sql.DB) (metricList []tableMetrics) { //todo, return error
	tblMetrics := tableMetrics{}
	var (
		rowCount, dataSize, totalSize, indexSize float64
	)
	log.Printf("Gathering metrics for database: %s", schemaName)
	rows, err := mysql.Query(
		"select table_schema,table_name,table_rows,data_length,index_length from tables where table_schema = ?",
		schemaName)
	if err != nil {
		log.Printf("Encountered error performing query, %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&tblMetrics.schemaName, &tblMetrics.tableName, &rowCount, &dataSize, &indexSize)
		totalSize = dataSize + indexSize
		tblMetrics.metrics = []metric{
			{"row_count", rowCount},
			{"data_size", roundedMB(dataSize)},
			{"index_size", roundedMB(indexSize)},
			{"total_size", roundedMB(totalSize)},
		}
		metricList = append(metricList, tblMetrics)
		if DEBUG {
			log.Printf("schema: %s, name: %s, rows: %0.0f, datassize: %0.2f MB, indexsize: %0.2f MB, totalsize: %0.2f MB\n",
				tblMetrics.schemaName, tblMetrics.tableName,
				rowCount, roundedMB(dataSize), roundedMB(indexSize), roundedMB(totalSize))
		}
	}
	return
}

//list RDS instances with tag named audit_growth if set to true
func listAuditableRDSInstances() rDSInstances {
	p := ListRDSInstanceInput{filter: true, key: "audit_growth", value: "true"}
	instances := ListRDSInstances(p)
	return instances
}

//retrieves credentials for RDS instance by reading system
//manager param store path from RDS Tag and loading json body
//contained within securestring object
// cred_path follows AWS_REGION_FOR_PARAM:PARAM_NAME format
func retrieveCredentials(instance rDSInstance) credentialSet {
	//todo,error handling
	credPath := instance.TagValue("cred_path")
	s := strings.Split(credPath, ":")
	region, paramPath := s[0], s[1]
	cred := GetParamAtPath(paramPath, region)
	var v map[string]string
	err := json.Unmarshal([]byte(cred), &v)
	if err != nil {
		log.Fatalf("Could not unmarshall param json to needed values for %s, %s", credPath, err)
	}
	var c credentialSet
	c.user = v["mysql_username"]
	c.password = v["mysql_password"]
	if c.user == "" || c.password == "" {
		log.Fatal("Credentials from Param store invalid or missing")
	}
	return c
}

//builds connection options into struct which can be converted to a MySQL DSN
//makes use of the credential set provided to extract username and password information
func buildConnectionOptions(instance rDSInstance, creds credentialSet) connectionParams {
	co := connectionParams{
		user:      creds.user,
		password:  creds.password,
		port:      "3306",
		defaultDB: "information_schema",
		hostName:  *instance.Endpoint.Address,
	}
	return co
}

//drive audit process per instance provided
//schemaList contains list of schema names to audit on the Instance
func AuditGrowth(instance rDSInstance, c connectionParams, schemaList []string) {
	//connection, err := connect(c)
	//if err != nil {
	//	log.Fatalf("Could not perform audit action for instance %s, error: %s", instance.identifier, err)
	//}
	var wg sync.WaitGroup
	for _, schemaName := range schemaList {
		wg.Add(1)
		go func(schema string, i rDSInstance, xwg *sync.WaitGroup) {
			defer wg.Done()
			con, err := connect(c)
			if err != nil {
				log.Fatalf("Could not perform audit action for instance %s, error: %s", instance.identifier, err)
			}
			tableStats := fetchMetrics(schema, con)
			var innerWg sync.WaitGroup
			log.Printf("Sending metrics to Sumo for %s-%s", i.identifier, schema)
			for _, stats := range tableStats {
				innerWg.Add(1)
				go func(s tableMetrics,r rDSInstance, wg2 *sync.WaitGroup) {
					defer wg2.Done()
					sendMetrics2Sumo(s, r)
				}(stats, instance,&innerWg)
			}
			innerWg.Wait()
			log.Printf("Done sending metrics to Sumo for %s-%s", i.identifier, schema)
		}(schemaName, instance, &wg)
	}
	wg.Wait()
}

//retrieves list of schemas to audit
//Tag: schemas_to_audit should be comma(,) separated list of schema names
func schemaList(instance rDSInstance) []string {
	s := instance.TagValue("schemas_to_audit")
	if s == "" {
		log.Fatalf("schemas_to_audit tag missing on %s", instance.name)
	}
	return strings.Split(s, ":")
}

//main driver function which brings all the needed pieces together for the audit process
//loops through list of auditable instances and performs the audits
func perform() Response {
	log.Println("Auditing RDS Instances")
	auditableInstances := listAuditableRDSInstances() //todo, make region look up parallel
	log.Printf("Found %v Instances with the correct tags", len(auditableInstances))
	var wg sync.WaitGroup
	for _, instance := range auditableInstances {
		wg.Add(1)
		go func(i rDSInstance, x *sync.WaitGroup) {
			defer x.Done()
			log.Println("Attempting to establish connection to perform audit for RDS instance", i.identifier)
			creds := retrieveCredentials(i)
			connection := buildConnectionOptions(i, creds)
			schemasToAudit := schemaList(i)
			AuditGrowth(i, connection, schemasToAudit)
		}(instance, &wg)
	}
	wg.Wait()
	return Response{Message: "tracking run completed"}
}

func Handler() {
	setConfig()
	perform()
}

type Response struct {
	Message string `json:"message"`
}

func main() {
	lambda.Start(Handler)
	//Handler()
}
