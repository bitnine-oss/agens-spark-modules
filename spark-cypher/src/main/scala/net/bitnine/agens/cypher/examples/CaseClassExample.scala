package net.bitnine.agens.cypher.examples

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.io.{Node, Relationship, RelationshipType}

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


/**
 * Demonstrates basic usage of the CAPS API by loading an example network via Scala case classes and running a Cypher
 * query on it.
 */
object CaseClassExample {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "CaseClassExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	def main(args: Array[String]): Unit = {
		spark.sparkContext.setLogLevel("error")

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Load social network data via case class instances
		val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

		// 3) Query graph with Cypher
		val results = socialNetwork.cypher(
			"""|MATCH (a:Person)-[r:FRIEND_OF]->(b)
			   |RETURN a.name, b.name, r.since
			   |ORDER BY a.name""".stripMargin
		)

		// 4) Print result to console
		results.show

// results ==>
//		Map(a.name -> Alice, b.name -> Bob, r.since -> 23/01/1987)
//		Map(a.name -> Bob, b.name -> Carol, r.since -> 12/12/2009)

		LOG.info(s"\n===========================================================")
/*
		// **NOTE: UDF 내에서는 spark 사용할 수 없다
		//   ==> Scala Object method or Java Static method 만 허용
		val morpheusUdf = functions.udf(CaseClassExample.morpheus)
		spark.udf.register("morpheusUdf", morpheusUdf)
		spark.sql("select morpheusUdf() as test").show(false)

		// ERROR: SparkException
		// Failed to execute user defined function(anonfun$morpheus$1: () => bigint)
		// ==> A master URL must be set in your configuration
 */
	}

}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.cypher.examples.CaseClassExample \
	target/agens-spark-cypher-1.0-dev.jar

*/

/**
 * Specify schema and data with case classes.
 */
object SocialNetworkData {

	case class Person(id: Long, name: String, age: Int) extends Node

	@RelationshipType("FRIEND_OF")
	case class Friend(id: Long, source: Long, target: Long, since: String) extends Relationship

	val alice = Person(0, "Alice", 10)
	val bob = Person(1, "Bob", 20)
	val carol = Person(2, "Carol", 15)
	val carolsMom = Person(3, "Carol's Mom", 55)

	val persons = List(alice, bob, carol, carolsMom)
	val friendships = List(
		Friend(0, alice.id, bob.id, "23/01/1987"),
		Friend(1, bob.id, carol.id, "12/12/2009"),
		Friend(2, carolsMom.id, carol.id, "02/04/1982"),
		Friend(3, carolsMom.id, alice.id, "22/08/1976")
	)
}