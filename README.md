# YCQL 4.x Java Driver 4.x tests

# Environment Setup

## Start the YugabyteDB cluster

You can do so using following command from YugabyteDB installation directory,


```
$ ./bin/yb-ctl destroy && ./bin/yb-ctl --rf 3 create --tserver_flags="cql_nodelist_refresh_interval_secs=10" --master_flags="tserver_unresponsive_timeout_ms=10000"
```

## Run Tests

```
mvn test
```
