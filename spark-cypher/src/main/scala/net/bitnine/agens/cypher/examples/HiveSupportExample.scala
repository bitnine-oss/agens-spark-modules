package net.bitnine.agens.cypher.examples

import net.bitnine.agens.cypher.api.io.util.CAPSGraphExport.CanonicalTableExport
import net.bitnine.agens.cypher.api.{CAPSSession, GraphSources}
import net.bitnine.agens.cypher.api.io.util.HiveTableName
import net.bitnine.agens.cypher.impl.CAPSConverters._

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.opencypher.okapi.api.graph.{GraphName, Node, Relationship}


object HiveSupportExample {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "HiveSupportExample"
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()
	spark.sparkContext.setLogLevel("error")

	def main(args: Array[String]): Unit = {

		// 1) Create CAPS session
		implicit val session: CAPSSession = CAPSSession.create(spark)

		// 2) Load social network data via case class instances
		val socialNetwork = session.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)
		session.catalog.store("sn", socialNetwork)

		val result1 = session.cypher("FROM GRAPH session.sn MATCH (n) RETURN n")
		result1.show
/*
╔══════════════════════════════════════════╗
║ n                                        ║
╠══════════════════════════════════════════╣
║ (:`Person` {`age`: 10, `name`: 'Alice'}) ║
║ (:`Person` {`age`: 20, `name`: 'Bob'})   ║
║ (:`Person` {`age`: 15, `name`: 'Carol'}) ║
╚══════════════════════════════════════════╝
(3 rows)
 */
		val result2 = session.cypher("FROM GRAPH session.sn MATCH ()-[e]-() RETURN e")
		result2.show
/*
╔════════════════════════════════════════╗
║ e                                      ║
╠════════════════════════════════════════╣
║ [:`FRIEND_OF` {`since`: '23/01/1987'}] ║
║ [:`FRIEND_OF` {`since`: '12/12/2009'}] ║
║ [:`FRIEND_OF` {`since`: '23/01/1987'}] ║
║ [:`FRIEND_OF` {`since`: '12/12/2009'}] ║		<== ??
╚════════════════════════════════════════╝			왜 두번씩 찍히지? 4개면 8줄이 찍힘
(4 rows)
 */
		//////////////////////////////////////

		val hiveDatabaseName = "socialNetwork"
//		session.sparkSession.sql(s"DROP DATABASE IF EXISTS $hiveDatabaseName CASCADE")
		session.sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $hiveDatabaseName")
		session.sparkSession.sql("show databases").show
/*
+-------------+
| databaseName|
+-------------+
|      default|
|      es_test|
|socialnetwork|
+-------------+
 */
		LOG.info("\n============================================")
/*
		val tmp = s"file:///tmp/bgmin/${System.currentTimeMillis()}"
		val fs = GraphSources.fs(tmp, Some(hiveDatabaseName)).parquet
		fs.store(graphName, socialNetwork)
*/
		val graphName = GraphName("sn")
		val graph = socialNetwork.asCaps

		val schema = socialNetwork.schema
		val nodeWrites = schema.labelCombinations.combos.map { combo =>
			val nodeType = combo.toList.sorted.mkString("_")		// multi-label 이라서?
			val tableName = HiveTableName(hiveDatabaseName, graphName, Node, Set(nodeType.toLowerCase))
			val df = graph.canonicalNodeTable(combo)
			df.write.mode("overwrite").saveAsTable(tableName)
			tableName
		}
		val relWrites = schema.relationshipTypes.map { relType =>
			val tableName = HiveTableName(hiveDatabaseName, graphName, Relationship, Set(relType.toLowerCase))
			val df = graph.canonicalRelationshipTable(relType)
			df.write.mode("overwrite").saveAsTable(tableName)
			tableName
		}
		println(s"** nodeWrites: $nodeWrites")
		println(s"** relWrites: $relWrites")
/*
** nodeWrites: Set(socialNetwork.sn_node_person)
** relWrites : Set(socialNetwork.sn_relationship_friend_of)
 */

		session.sparkSession.sql(s"show tables in ${hiveDatabaseName}").show
		// **SparkSQL : spark.catalog.listTables("socialnetwork").show

		// **NOTE: after overwrite, FileNotFoundException raised
		// refresh table socialnetwork.sn_node_person;
		// refresh table socialnetwork.sn_relationship_friend_of;

/*
+-------------+--------------------+-----------+
|     database|           tableName|isTemporary|
+-------------+--------------------+-----------+
|socialnetwork|      sn_node_person|      false|
|socialnetwork|sn_relationship_f...|      false|
+-------------+--------------------+-----------+
 */
		LOG.info("\n============================================")

		// socialnetwork.sn_node_person
		// socialnetwork.sn_relationship_friend_of
		val nodeTableName = HiveTableName(hiveDatabaseName, graphName, Node, Set("Person"))

		val result3 = session.sql(s"SELECT * FROM $nodeTableName WHERE property_age >= 15")
		result3.show
/*
╔═════╤══════════════╤════════════════╗
║ id  │ property_age │ property_name  ║
╠═════╪══════════════╪════════════════╣
║ [2] │ 15           │ 'Carol'        ║
║ [3] │ 55           │ 'Carol\'s Mom' ║
║ [1] │ 20           │ 'Bob'          ║
╚═════╧══════════════╧════════════════╝
(3 rows)
 */
		val edgeTableName = HiveTableName(hiveDatabaseName, graphName, Relationship, Set("FRIEND_OF".toLowerCase))
		session.sparkSession.table(edgeTableName).select(col("property_since")).show
		// **NOTE: Hive 테이블 저장시 컬럼명 변경됨 ==> "property_" + "since"
/*
+--------------+
|property_since|
+--------------+
|    02/04/1982|
|    22/08/1976|
|    23/01/1987|
|    12/12/2009|
+--------------+
 */
		session.catalog.dropGraph("sn")

//		fs.delete(graphName)
//		session.sparkSession.sql(s"DROP DATABASE IF EXISTS $hiveDatabaseName CASCADE")
	}
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.opencypher.examples.HiveSupportExample \
	target/agens-spark-cypher-1.0-dev.jar

*/

/*
** NOTE: 조인 테스트
   <== beeline 화면상에는 id 값들이 안나온다 해도 정상적으로 매치된다 (binary value??)
   <== spark 에서는 정상적으로 id 값들이 나온다

spark> spark.catalog.listColumns("socialnetwork.sn_node_person").show
+-------------+-----------+--------+--------+-----------+--------+
|         name|description|dataType|nullable|isPartition|isBucket|
+-------------+-----------+--------+--------+-----------+--------+
|           id|       null|  binary|    true|      false|   false|
| property_age|       null|     int|    true|      false|   false|
|property_name|       null|  string|    true|      false|   false|
+-------------+-----------+--------+--------+-----------+--------+

spark> spark.catalog.listTables("socialnetwork").show
+--------------------+-------------+-----------+---------+-----------+
|                name|     database|description|tableType|isTemporary|
+--------------------+-------------+-----------+---------+-----------+
|      sn_node_person|socialnetwork|       null|  MANAGED|      false|
|sn_relationship_f...|socialnetwork|       null|  MANAGED|      false|
+--------------------+-------------+-----------+---------+-----------+

spark> spark.table("socialnetwork.sn_node_person").show
+----+------------+-------------+
|  id|property_age|property_name|
+----+------------+-------------+
|[02]|          15|        Carol|
|[03]|          55|  Carol's Mom|
|[00]|          10|        Alice|
|[01]|          20|          Bob|
+----+------------+-------------+

beeline> select * from socialnetwork.sn_relationship_friend_of;
+-----+---------+---------+-----------------+--+
| id  | source  | target  | property_since  |
+-----+---------+---------+-----------------+--+
|    |        |        | 02/04/1982      |
|    |        |        | 22/08/1976      |
|    |        |        | 23/01/1987      |
|    |        |        | 12/12/2009      |
+-----+---------+---------+-----------------+--+
4 rows selected (0.084 seconds)

beeline> select a.property_name, a.property_age, b.property_since
from socialnetwork.sn_node_person a, socialnetwork.sn_relationship_friend_of b
where a.id = b.source;
+----------------+---------------+-----------------+--+
| property_name  | property_age  | property_since  |
+----------------+---------------+-----------------+--+
| Carol's Mom    | 55            | 22/08/1976      |
| Carol's Mom    | 55            | 02/04/1982      |
| Alice          | 10            | 23/01/1987      |
| Bob            | 20            | 12/12/2009      |
+----------------+---------------+-----------------+--+
4 rows selected (0.474 seconds)

val df = spark.read.format("HiveAcid").options(Map("table" -> "default.temp01")).load()

 */