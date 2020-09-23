package net.bitnine.agens.spark.examples

import net.bitnine.agens.cypher.api.GraphSources
import net.bitnine.agens.cypher.impl.CAPSRecords
import net.bitnine.agens.spark.Agens.ResultsAsDF
import net.bitnine.agens.spark.AgensBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.{CypherResult, GraphName, Namespace}

object MultiGraphExample extends App {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "ReadAsTableExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	//////////////////////////////////

	val agens = AgensBuilder(spark).build	//	.default(spark)
	val datasource = "modern"

	val graphModern = agens.graph(datasource)

	// default namespace: session
	agens.catalog.store("modern", graphModern)

	// for DEBUG
	// agens.cypher("FROM GRAPH modern MATCH (n) RETURN n").show

	////////////////////////////////////

	// 3) Register a file system graph source to the catalog
	// Note: if files were stored in HDFS, the file path would indicate so by starting with hdfs://
	val csvFolder = "hdfs://minmac:9000/user/agens/samples/csv"
	agens.registerSource(Namespace("purchases"), agens.fsGraphsource(rootPath = csvFolder).csv)

	println("\n** [start] graphs: "+agens.catalog.graphNames.map(_.graphName).mkString("[","],[","]"))

	// access the graph from the catalog via its qualified graph name
	val purchaseNetwork = agens.catalog.graph("purchases.products")

	val result1 = purchaseNetwork.cypher("FROM GRAPH purchases.products MATCH (c:Customer) RETURN c")
	result1.show
	val result2 = purchaseNetwork.cypher("FROM GRAPH purchases.products MATCH (p:Product) RETURN p")
	result2.show
	val result3 = purchaseNetwork.cypher("FROM GRAPH purchases.products MATCH ()-[e]-() RETURN e")
	result3.show

	////////////////////////////////////

	// 5) Create new edges between users and customers with the same name
	val recommendationGraph = agens.cypher(
		"""|FROM GRAPH modern
		   |MATCH (p:person)
		   |FROM GRAPH purchases.products
		   |MATCH (c:Customer)
		   |WHERE p.name$ = c.name
		   |CONSTRUCT ON modern, purchases.products
		   |  CREATE (p)-[:IS]->(c)
		   |RETURN GRAPH
    		""".stripMargin
	).graph

	val tmp = s"file:///tmp/bgmin/${System.currentTimeMillis()}"
	val fs = agens.fsGraphsource(tmp).parquet
	fs.store(GraphName("recommendation"), recommendationGraph)

	////////////////////////////////////

	// 6) Query for product recommendations
	val recommendations:CypherResult = recommendationGraph.cypher(
		"""|MATCH (person:person)-[:knows]-(friend:person),
		   |      (friend)-[:IS]->(customer:Customer),
		   |      (customer)-[:BOUGHT]->(product:Product)
		   |RETURN DISTINCT product.title AS recommendation, person.name$ AS for
		   |ORDER BY recommendation
    		""".stripMargin)

	recommendations.show

	// future 처리가 필요하지 않나?
	agens.saveResultAsAvro(recommendations, "recommendations")

}
/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.spark.examples.MultiGraphExample \
	target/agens-spark-connector-1.0-dev.jar

*/