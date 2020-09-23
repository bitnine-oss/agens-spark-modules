package net.bitnine.agens.livy.job

import net.bitnine.agens.livy.util.SchemaConverters
import net.bitnine.agens.spark.Agens.ResultsAsDF
import net.bitnine.agens.spark.{Agens, AgensBuilder, AgensHelper}
import org.apache.avro.SchemaBuilder
import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.PropertyGraph

//import org.slf4j.{Logger,LoggerFactory}

class CypherJob(
		val datasource: java.lang.String,
		val name: java.lang.String,
		val query: java.lang.String
) extends Job[java.lang.String] {

//val LOG: Logger = LoggerFactory.getLogger(getClass)

	override def call(jc: JobContext): java.lang.String = {
		val spark: SparkSession = jc.sparkSession()
		callScala(spark)
	}

	def callScala(spark: SparkSession): java.lang.String = {
		println("** parameters")
		println(s"- datasource: $datasource")
		println(s"- name: $name")
		println(s"- query: $query")

// **NOTE: logger impl not found and raise java.lang.NullPointerException
//LOG.info("** parameters")
//LOG.info(s"- datasource: $datasource")
//LOG.info(s"- name: $name")
//LOG.info(s"- query: $query")

		//////////////////////////////////
		// 1) spark-connector : connect to elasticsearch

		// **NOTE: AgensConf must be set by spark-default.conf
		println("1) Agens connect to ES")
		val agens:Agens = AgensBuilder(spark).build
//				.host("minmac")
//				.port("29200")
//				.user("elastic")
//				.password("bitnine")
//				.vertexIndex("agensvertex")
//				.edgeIndex("agensedge")
//				.build

		//////////////////////////////////
		// 2) spark-cypher : run query

		println("2) load graph from datasource: "+datasource)
		val graph:PropertyGraph = agens.graph(datasource)

		// query: {name, age}, {name, country}
		println("\n** query => "+query)
		println("3) run query using spark-cypher: "+query)
		val result = graph.cypher(query)
		result.show

		// save to '/user/agens/temp' as avro
		println("\n** tempPath ==> "+ agens.conf.tempPath)
		println("4) save results as avro: "+name)
		agens.saveResultAsAvro(result, name)

		//////////////////////////////////
		// 3) convert schema of result to avro schema

		println("5) convert to DF and get Avro Schema")
		val df = result.asDataFrame
		val build = SchemaBuilder.record(name).namespace(SchemaConverters.AGENS_AVRO_NAMESPACE)
		val avroSchema = SchemaConverters.convertStructToAvro(df.schema, build, SchemaConverters.AGENS_AVRO_NAMESPACE)

		avroSchema.toString
	}

}

/*
spark-shell --master spark://minmac:7077 \
--jars hdfs://minmac:9000/user/agens/lib/agens-livy-jobs-1.0-dev.jar,hdfs://minmac:9000/user/agens/lib/agens-spark-connector-1.0-dev.jar

///////////////////////////////////////////////////

import net.bitnine.agens.livy.util.SchemaConverters
import net.bitnine.agens.spark.Agens.ResultsAsDF
import net.bitnine.agens.spark.{Agens, AgensBuilder}
import org.apache.avro.SchemaBuilder
import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.PropertyGraph

val datasource = "modern"
val name = "test_query02"
val query = "match (a:person) return a.id_, a.name, a.age, a.country"

val agens:Agens = AgensBuilder(spark).host("minmac").port("29200").
user("elastic").password("bitnine").vertexIndex("agensvertex").edgeIndex("agensedge").build

val graph:PropertyGraph = agens.graph(datasource)
// <== java.lang.ClassNotFoundException: Failed to find data source: es. Please find packages at http://spark.apache.org/third-party-projects.html

val result = graph.cypher(query)
result.show

agens.saveResultAsAvro(result, name)

val df = result.asDataFrame
val build = SchemaBuilder.record(name).namespace(SchemaConverters.AGENS_AVRO_NAMESPACE)
val avroSchema = SchemaConverters.convertStructToAvro(df.schema, build, SchemaConverters.AGENS_AVRO_NAMESPACE)
avroSchema.toString(true)

 */