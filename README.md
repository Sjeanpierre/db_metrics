# DB metric auditor
Go utility which tracks table growth using MYSQL information schema

The following information is pushed into Datadog and reported to stdout
* Number of rows in table (Count)
* Size of table data (Bytes)
* Size of indexes (Bytes)
* Total size of table (Bytes)

Required environment variables for configuration
* DB_USER
    * User name of MySQL user with information_schema read access
* MYSQL_ROOT_PW
    * Password for user defined by DB_USER
* DB_HOSTNAME
    * FQDN of MySQL server, without port
* ENVIRONMENT
    * string defining operating environment, e.g prod, staging, etc
* DD_API_KEY
    * DataDog API Key
* DD_APP_KEY
    * DataDog APP Key
* DB_LIST
    * Comma delimited list of schema names to report on
* DATADOG_ENABLED
    * Boolean which determines if utility should try to post to Data Dog or not
