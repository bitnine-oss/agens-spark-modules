package net.bitnine.agens.spark.examples

import net.bitnine.agens.spark.AgensBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SaveToHiveExample extends App {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	// given after spark start
	val JOB_NAME: String = "SaveToHiveExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	//////////////////////////////////

	// create agens session
	val agens = AgensBuilder(spark).build
//			.host("minmac")
//			.port("29200")
//			.user("elastic")
//			.password("bitnine")
//			.vertexIndex("agensvertex")
//			.edgeIndex("agensedge")
//			.build

	// set datasource value
	val datasource = "modern"

	//////////////////////////////////

	// for hive connected with spark
	val dbPath = "agens"
	agens.sql(s"DROP DATABASE IF EXISTS $dbPath CASCADE")
	agens.sql("show databases").show

	//////////////////////////////////

	val graphModern = agens.graph(datasource)

	agens.saveGraphToHive(graphModern, datasource, dbPath)

	agens.sql(s"show tables in $dbPath").show
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.spark.examples.SaveToHiveExample \
	target/agens-spark-connector-1.0-dev.jar

*/


/*
hive> show tables in modern;
OK
modern_node_person
modern_node_software
modern_relationship_created
modern_relationship_knows
==>

Table(
	tableName:modern_node_person,
	dbName:modern,
	owner:bgmin,
	createTime:1600081874, lastAccessTime:0, retention:0,
	sd:StorageDescriptor(
		cols:[
			FieldSchema(name:id, type:binary, comment:null),
			FieldSchema(name:property_age@0024, type:int, comment:null),
			FieldSchema(name:property_country@0024, type:string, comment:null),
			FieldSchema(name:property_datasource, type:string, comment:null),
			FieldSchema(name:property_label, type:string, comment:null),
			FieldSchema(name:property_name@0024, type:string, comment:null),
			FieldSchema(name:property_timestamp, type:timestamp, comment:null)
		],
		location:hdfs://minmac:9000/user/hive/warehouse/modern.db/modern_node_person,
		inputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat,
		outputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat,
		compressed:false, numBuckets:-1,
		serdeInfo:SerDeInfo(
			name:null,
			serializationLib:org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe,
			parameters:{
				serialization.format=1,
				path=hdfs://minmac:9000/user/hive/warehouse/modern.db/modern_node_person
			}
		),
		bucketCols:[], sortCols:[], parameters:{},
		skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}),
		storedAsSubDirectories:false
	),
	partitionKeys:[],
	parameters:{
		totalSize=9194, numFiles=5, transient_lastDdlTime=1600081874,
		spark.sql.sources.schema.part.0={
			"type":"struct",
			"fields":[
				{"name":"id","type":"binary","nullable":true,"metadata":{}},
				{"name":"property_age@0024","type":"integer","nullable":true,"metadata":{}},
				{"name":"property_country@0024","type":"string","nullable":true,"metadata":{}},
				{"name":"property_datasource","type":"string","nullable":true,"metadata":{}},
				{"name":"property_label","type":"string","nullable":true,"metadata":{}},
				{"name":"property_name@0024","type":"string","nullable":true,"metadata":{}},
				{"name":"property_timestamp","type":"timestamp","nullable":true,"metadata":{}}
			]
		},
		spark.sql.sources.schema.numParts=1,
		spark.sql.sources.provider=parquet,
		spark.sql.create.version=2.4.6
	},
	viewOriginalText:null, viewExpandedText:null, tableType:MANAGED_TABLE, rewriteEnabled:false
)

 */