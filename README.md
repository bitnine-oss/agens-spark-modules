# Agens Spark Modules 

- Agens Spark Cypher
- Agens Spark Connector
- Agens Livy Jobs
- Agens Hive Storage Handler

## build

```shell script
cd agens-spark-modules

mvn clean install
```

## pre-requirement

- java 1.8
- scala 2.11
  - **important**: es-hadoop cannot support scala 2.12
- elasticsearch 7.9.0 (> 7.0.0)
  - vertexIndex
  - edgeIndex
- hadoop 2.10.0
  - dfs
  - yarn 
  - mkdir /user/agens, /user/agens/temp
  - copy 'default.avsc' to hdfs//<host>:<ip>/user/agens
- spark 2.4.6 (< 3.0.0)
  - master
  - slaves 
- hive 2.3.7 (< 3.0.0)
  - hiveserver2
  - metastore
- livy 0.7.0
  - livy-server
- zeppelin 0.8.2
  - zeppelin-daemon

## deploy

```shell script
# for spark
cp spark-connector/target/agens-spark-connector-1.0-dev.jar ${extraJars}
cp livy-jobs/target/agens-livy-jobs-1.0-dev.jar ${extraJars}

# for hive
cp hive-storage-handler/target/agens-hive-storage-handler-1.0-dev.jar ${extraJars}
```

## config

### spark

spark/conf/spark-defaults.conf
```shell script
## spark.agens.* properties for AgensConf
spark.agens.host            ${esHost}
spark.agens.port            9200
spark.agens.vertexIndex     agensvertex
spark.agens.edgeIndex       agensedge
spark.agens.tempPath        /user/agens/temp
spark.agens.user            elastic
spark.agens.password        bitnine

spark.driver.extraClassPath     ${extraJars}/agens-spark-connector-1.0-dev.jar:${extraJars}/agens-livy-jobs-1.0-dev.jar
spark.executor.extraClassPath   ${extraJars}/agens-spark-connector-1.0-dev.jar:${extraJars}/agens-livy-jobs-1.0-dev.jar
```

### hive

hive/conf/hive-site.xml
```xml
<property>
  <name>agens.spark.path</name>
  <value>/user/agens/temp</value>
</property>
<property>
  <name>agens.spark.livy</name>
  <value>http://minmac:8998</value>
</property>
```

hive/conf/hive-env.sh
```shell script
export HIVE_AUX_JARS_PATH=${extraJars}/agens-hive-storage-handler-1.0-dev.jar
```

### zeppelin

zeppelin/conf/zeppelin-env.sh
```shell script
export SPARK_SUBMIT_OPTIONS="--jars ${extraJars}/agens-spark-connector-1.0-dev.jar"
```

## run

- run batch by Spark Submit
- run interactive by Spark Shell with Zeppelin
- create table through hive-storage-handler, livy 

run batch
```shell script
## for TEST
spark-submit --executor-memory 1g \
    --master spark://minmac:7077 \
    --class net.bitnine.agens.spark.Agens \
    target/agens-spark-connector-1.0-dev.jar

## for IMPORT
```

run interactive (zeppelin)
```scala
import net.bitnine.agens.spark.Agens.ResultsAsDF
import net.bitnine.agens.spark.AgensBuilder

val agens = AgensBuilder(spark)
        .host("minmac")
        .port("29200")
        .user("elastic")
        .password("bitnine")
        .vertexIndex("agensvertex")
        .edgeIndex("agensedge")
        .build
val datasource = "modern"

//////////////////////////////////

val graphModern = agens.graph(datasource)

agens.catalog.store("modern", graphModern)

val result1 = agens.cypher("FROM GRAPH session.modern MATCH (n) RETURN n")
result1.show
val result2 = agens.cypher("FROM GRAPH session.modern MATCH ()-[e]-() RETURN e")
result2.show

//////////////////////////////////

val result3 = graphModern.cypher("""|MATCH (a:person)-[r:knows]->(b)
                                    |RETURN a.name, b.name, r.weight
                                    |ORDER BY a.name""".stripMargin)
result3.show

// Spark SQL: temp Table => managed Table
val df3 = result3.asDataFrame
val tblName = "modern_temp3"

df3.createOrReplaceTempView(tblName)
spark.sql(s"create table avro_${tblName} stored as avro as select * from ${tblName}")
spark.sql(s"select * from avro_${tblName}").show

agens.catalog.dropGraph(datasource)
```

run hive
```sql
CREATE external TABLE modern_test1
STORED BY 'net.bitnine.agens.hive.AgensHiveStorageHandler'
TBLPROPERTIES(
'avro.schema.url'='hdfs://minmac:9000/user/agens/default.avsc',
'agens.spark.datasource'='modern',
'agens.spark.query'='match (a:person)-[b]-(c:person) return distinct a.id_, a.name, a.age, a.country, b.label, c.name'
);
```