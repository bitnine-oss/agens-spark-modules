package net.bitnine.agens.cypher.examples

import net.bitnine.agens.cypher.api.{CAPSSession, GraphSources}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph.Namespace


/**
 * Demonstrates multiple graph capabilities by loading a social network from case class objects and a purchase network
 * from CSV data and schema files. The example connects both networks via matching user and customer names. A Cypher
 * query is then used to compute products that friends have bought.
 */
object MultipleGraphExample {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "MultipleGraphExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	def main(args: Array[String]): Unit = {
		spark.sparkContext.setLogLevel("error")

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Load social network data via case class instances
		val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)
		session.catalog.store("socialNetwork", socialNetwork)

		// 3) Register a file system graph source to the catalog
		// Note: if files were stored in HDFS, the file path would indicate so by starting with hdfs://
		val csvFolder = getClass.getResource("/fs-graphsource/csv").getFile
		session.registerSource(Namespace("purchases"), GraphSources.fs(rootPath = csvFolder).csv)

		// access the graph from the catalog via its qualified graph name
		val purchaseNetwork = session.catalog.graph("purchases.products")

		// 5) Create new edges between users and customers with the same name
		val recommendationGraph = session.cypher(
			"""|FROM GRAPH socialNetwork
			   |MATCH (p:Person)
			   |FROM GRAPH purchases.products
			   |MATCH (c:Customer)
			   |WHERE p.name = c.name
			   |CONSTRUCT ON socialNetwork, purchases.products
			   |  CREATE (p)-[:IS]->(c)
			   |RETURN GRAPH
    		""".stripMargin
		).graph

		// 6) Query for product recommendations
		val recommendations = recommendationGraph.cypher(
			"""|MATCH (person:Person)-[:FRIEND_OF]-(friend:Person),
			   |      (friend)-[:IS]->(customer:Customer),
			   |      (customer)-[:BOUGHT]->(product:Product)
			   |RETURN DISTINCT product.title AS recommendation, person.name AS for
			   |ORDER BY recommendation
    		""".stripMargin)

		recommendations.show
	}
}

/*
spark-submit --executor-memory 2g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.cypher.examples.MultipleGraphExample \
	target/agens-spark-cypher-1.0-dev.jar

==>
GraphNotFoundException: Graph with name 'products'
*/
