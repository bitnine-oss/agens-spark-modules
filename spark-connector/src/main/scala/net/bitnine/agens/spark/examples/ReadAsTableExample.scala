package net.bitnine.agens.spark.examples

import net.bitnine.agens.spark.Agens.ResultsAsDF
import net.bitnine.agens.spark.AgensBuilder
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object ReadAsTableExample extends App {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "ReadAsTableExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	//////////////////////////////////

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
	println("\n** schema: "+graphModern.schema.toString)

	agens.catalog.store("modern", graphModern)
	println("\n** [start] graphs: "+agens.catalog.graphNames.map(_.graphName).mkString("[","],[","]"))
	// start ==> graphs: [emptyGraph],[modern]

	val result1 = agens.cypher("FROM GRAPH session.modern MATCH (n) RETURN n")
	result1.show
	val result2 = agens.cypher("FROM GRAPH session.modern MATCH ()-[e]-() RETURN e")
	result2.show

	val result3 = graphModern.cypher("""|MATCH (a:person)-[r:knows]->(b)
										|RETURN a.name, b.name, r.weight
										|ORDER BY a.name""".stripMargin)
	result3.show

	// Spark SQL: temp Table => managed Table
	val df3 = result3.asDataFrame
	val tblName = "modern_temp3"
	// deprecated: registerTempTable
	df3.createOrReplaceTempView(tblName)
	// create managed table using avro
	spark.sql(s"create table avro_${tblName} stored as avro as select * from ${tblName}")
	spark.sql(s"select * from avro_${tblName}").show

	val result4 = graphModern.cypher("MATCH (a)-[r:knows]->(b) RETURN a, b")
	result4.show

	agens.catalog.dropGraph(datasource)
	println("\n** [end] graphs: "+agens.catalog.graphNames.map(_.graphName).mkString("[","],[","]"))
	// end ==> graphs: [tmp2],[tmp3],[emptyGraph],[tmp4],[tmp1]
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.spark.examples.ReadAsTableExample \
	target/agens-spark-connector-1.0-dev.jar

*/

/*
** schema: SchemaImpl(
Map(
	Set(person) -> Map(
		timestamp -> LOCALDATETIME,
		age$ -> INTEGER?,
		label -> STRING,
		country$ -> STRING?,
		name$ -> STRING?,
		datasource -> STRING
	),
	Set(software) -> Map(
		timestamp -> LOCALDATETIME,
		lang$ -> STRING?,
		label -> STRING,
		name$ -> STRING?,
		datasource -> STRING
	)
),
Map(
	knows -> Map(
		timestamp -> LOCALDATETIME,
		datasource -> STRING,
		label -> STRING,
		weight$ -> FLOAT?
	),
	created -> Map(
		timestamp -> LOCALDATETIME,
		datasource -> STRING,
		label -> STRING,
		weight$ -> FLOAT?
	)
),
Set(),
Map(),
Map()
)
 */