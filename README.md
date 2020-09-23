# Agens Spark Modules 

- Agens Spark Cypher
- Agens Spark Connector
- Agens Livy Jobs
- Agens Hive Storage Handler

## build

```shell script
mvn clean install
```

## run

- run batch by Spark Submit
- run interactive by Spark Shell with Zeppelin
- create table through hive-storage-handler, livy 

```shell script
## for TEST
spark-submit --executor-memory 1g \
    --master spark://minmac:7077 \
    --class net.bitnine.agens.spark.Agens \
    target/agens-spark-connector-1.0-dev.jar

## for IMPORT
```
