package net.bitnine.agens.cypher.examples

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.CAPSSession._

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.opencypher.okapi.api.graph.CypherResult


/**
 * Shows how to access a Cypher query result as a [[DataFrame]].
 */
object DataFrameOutputExample {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "DataFrameOutputExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	def main(args: Array[String]): Unit = {
		spark.sparkContext.setLogLevel("error")

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Load social network data via case class instances
		val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

		// 3) Query graph with Cypher
		val results: CypherResult = socialNetwork.cypher(
			"""|MATCH (a:Person)-[r:FRIEND_OF]->(b)
			   |RETURN a.name, b.name, r.since""".stripMargin)

		// 4) Extract DataFrame representing the query result
		val df: DataFrame = results.records.asDataFrame

		// 5) Select specific return items from the query result
		val projection: DataFrame = df.select("a_name", "b_name")

		projection.show()
		/*
		+----------+----------+
		|a_dot_name|b_dot_name|
		+----------+----------+
		|     Alice|       Bob|
		|       Bob|     Carol|
		+----------+----------+
		 */
	}
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.opencypher.examples.DataFrameOutputExample \
	target/agens-spark-cypher-1.0-dev.jar

*/

/**
 * Alternative to accessing a Cypher query result as a [[DataFrame]].
 */
object DataFrameOutputUsingAliasExample {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "okapi examples #03-2"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	def main(args: Array[String]): Unit = {

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Load social network data via case class instances
		val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

		// 3) Query graph with Cypher
		val results = socialNetwork.cypher(
			"""|MATCH (a:Person)-[r:FRIEND_OF]->(b)
			   |RETURN a.name AS person1, b.name AS person2, r.since AS friendsSince""".stripMargin)

		// 4) Extract DataFrame representing the query result
		val df: DataFrame = results.records.asDataFrame

		// 5) Select aliased return items from the query result
		val projection: DataFrame = df
				.select("person1", "friendsSince", "person2")
				.orderBy(functions.to_date(df.col("friendsSince"), "dd/mm/yyyy"))

		projection.show()
/*
+-------+------------+-------+
|person1|friendsSince|person2|
+-------+------------+-------+
|  Alice|  23/01/1987|    Bob|
|    Bob|  12/12/2009|  Carol|
+-------+------------+-------+
 */
	}
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.spark.examples.DataFrameOutputUsingAliasExample \
	target/agens-spark-connector-full-1.0-dev.jar

*/