# Example Application for getting started with YugabyteDB and Datastax cassandra 4.x driver

# Environment Setup

## Start the YugabyteDB cluster

You can do so using following command from YugabyteDB installation directory,


```
$ ./bin/yb-ctl destroy && ./bin/yb-ctl --rf 3 create --tserver_flags="cql_nodelist_refresh_interval_secs=10" --master_flags="tserver_unresponsive_timeout_ms=10000"
```

## Build and Run the application

```
mvn exec:java -D"exec.mainClass"="com.yugabytedb.samples.SampleCode4x_CRUD_00_GettingStarted"
```
# ycql-4.x-tests
