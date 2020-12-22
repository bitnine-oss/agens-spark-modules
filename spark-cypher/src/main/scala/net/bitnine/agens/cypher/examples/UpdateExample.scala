package net.bitnine.agens.cypher.examples

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.CAPSSession._
import net.bitnine.agens.cypher.api.value.CAPSNode
import net.bitnine.agens.cypher.impl.encoders._

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.opencypher.okapi.api.value.CypherValue.CypherMap

import scala.collection.JavaConverters._


/**
 * Demonstrates how to retrieve Cypher entities as a Dataset and update them.
 */
object UpdateExample {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "UpdateExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	def main(args: Array[String]): Unit = {
		spark.sparkContext.setLogLevel("error")

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Load social network data via case class instances
		val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

		// 3) Query graph with Cypher
		val results = socialNetwork.cypher(
			"""|MATCH (p:Person)
			   |WHERE p.age >= 18
			   |RETURN p""".stripMargin)

		// 4) Extract Dataset representing the query result
		val ds = results.records.asDataset

		// 5) Add a new label and property to the nodes
		val adults: Dataset[CAPSNode] = ds.map { record: CypherMap =>
			record("p").cast[CAPSNode].withLabel("Adult").withProperty("canVote", true)
		}

		// 6) Print updated nodes
		println("\n===============================")
		println(adults.toLocalIterator.asScala.toList.mkString("\n"))
/*
CAPSNode(id=0x01, labels=Set(Person, Adult), properties=Map(name -> Bob, age -> 20, canVote -> true))
CAPSNode(id=0x03, labels=Set(Person, Adult), properties=Map(name -> Carol's Mom, age -> 55, canVote -> true))
 */
	}
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.cypher.examples.UpdateExample \
	target/agens-spark-cypher-1.0-dev.jar

*/
