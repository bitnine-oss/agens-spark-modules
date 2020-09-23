package net.bitnine.agens.cypher.examples

import net.bitnine.agens.cypher.api.CAPSSession

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object DataSourceExample {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "DataSourceExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

	def main(args: Array[String]): Unit = {

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Load social network data via case class instances
		val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

		session.catalog.store("sn", socialNetwork)

		val result = session.cypher("FROM GRAPH session.sn MATCH (n) RETURN n")

		result.show
/*
╔═════════════════════════════════════════════════╗
║ n                                               ║
╠═════════════════════════════════════════════════╣
║ (:`Person` {`age`: 10, `name`: 'Alice'})        ║
║ (:`Person` {`age`: 20, `name`: 'Bob'})          ║
║ (:`Person` {`age`: 15, `name`: 'Carol'})        ║
║ (:`Person` {`age`: 55, `name`: 'Carol\'s Mom'}) ║
╚═════════════════════════════════════════════════╝
(4 rows)
 */
	}
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.opencypher.examples.DataSourceExample \
	target/agens-spark-cypher-1.0-dev.jar

*/
