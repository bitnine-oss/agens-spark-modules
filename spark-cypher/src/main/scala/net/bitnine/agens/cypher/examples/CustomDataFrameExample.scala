package net.bitnine.agens.cypher.examples

import java.sql.Date

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.io.{CAPSEntityTable, CAPSNodeTable}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}


/**
 * Demonstrates basic usage of the CAPS API by loading an example network from existing [[DataFrame]]s including
 * custom entity mappings and running a Cypher query on it.
 */
object CustomDataFrameExample extends App {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	val JOB_NAME: String = "CustomDataFrameExample"

	// 1) Create CAPS session and retrieve Spark session
	val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()
	spark.sparkContext.setLogLevel("error")

	implicit val session: CAPSSession = CAPSSession.create(spark)
	// end::create-session[]

	// 2) Generate some DataFrames that we'd like to interpret as a property graph.
	// tag::prepare-dataframes[]
	val nodeData: DataFrame = spark.createDataFrame(Seq(
		("Alice", 42L),
		("Bob", 23L),
		("Eve", 84L)
	)).toDF("FIRST_NAME", "AGE")
	val nodesDF = nodeData.withColumn("ID", nodeData.col("FIRST_NAME"))

	val relsDF: DataFrame = spark.createDataFrame(Seq(
		(0L, "Alice", "Bob", Date.valueOf("1987-01-23")),
		(1L, "Bob", "Eve", Date.valueOf("2009-12-12"))
	)).toDF("REL_ID", "SOURCE_ID", "TARGET_ID", "CONNECTED_SINCE")
	// end::prepare-dataframes[]

	// 3) Generate node- and relationship tables that wrap the DataFrames and describe their contained data.
	//    Node and relationship mappings are used to explicitly define which DataFrame column stores which specific entity
	//    component (identifiers, properties, optional labels, relationship types).

	// tag::create-node-relationship-tables[]

	val personNodeMapping = NodeMappingBuilder
			.withSourceIdKey("ID")
			.withImpliedLabel("Person")
			.withPropertyKey(propertyKey = "name", sourcePropertyKey = "FIRST_NAME")
			.withPropertyKey(propertyKey = "age", sourcePropertyKey = "AGE")
			.build

	val friendOfMapping = RelationshipMappingBuilder
			.withSourceIdKey("REL_ID")
			.withSourceStartNodeKey("SOURCE_ID")
			.withSourceEndNodeKey("TARGET_ID")
			.withRelType("FRIEND_OF")
			.withPropertyKey("since", "CONNECTED_SINCE")
			.build

	val personTable = CAPSEntityTable.create(personNodeMapping, nodesDF)
	val friendsTable = CAPSEntityTable.create(friendOfMapping, relsDF)
	// end::create-node-relationship-tables[]

	val person1Table = CAPSEntityTable.create(personNodeMapping, nodesDF)
	val person2Table = CAPSEntityTable.create(personNodeMapping, nodesDF)
	val person3Table = CAPSEntityTable.create(personNodeMapping, nodesDF)

	val friends1Table = CAPSEntityTable.create(friendOfMapping, relsDF)
	val friends2Table = CAPSEntityTable.create(friendOfMapping, relsDF)

	val nodeTables = List(personTable, person1Table, person2Table, person3Table)
	val edgeTables = List(friendsTable, friends1Table, friends2Table)

	// **NOTE: 둘다 안됨
	val graph1 = session.readFrom(nodeTables ++ edgeTables)
	val graphNodes = session.readFrom(nodeTables)
	val graphEdges = session.readFrom(edgeTables)
	val graphUnion = graph1.unionAll(graphNodes)

	// 4) Create property graph from graph scans
	// tag::create-graph[]
	val graph = session.readFrom(personTable, friendsTable)
	// end::create-graph[]

	val result0 = graph.cypher("""|MATCH (a:Person)-[r:FRIEND_OF]->(b)
								 |RETURN a.name, b.name, r.since
								 |ORDER BY a.name""".stripMargin)
	result0.show
	val result1 = graph1.cypher("""|MATCH (a:Person)-[r:FRIEND_OF]->(b)
								   |RETURN a.name, b.name, r.since
								   |ORDER BY a.name""".stripMargin)
	result1.show
	val result2 = graphUnion.cypher("""|MATCH (a:Person)-[r:FRIEND_OF]->(b)
									   |RETURN a.name, b.name, r.since
									   |ORDER BY a.name""".stripMargin)
	result2.show

	val result3 = graphUnion.cypher("MATCH (n:Person) RETURN distinct n.name")
	result3.show

	// 5) Execute Cypher query and print results
	// tag::run-query[]
	val result = graph.cypher("MATCH (n:Person) RETURN n.name")
	// end::run-query[]

	// 6) Collect results into string by selecting a specific column.
	//    This operation may be very expensive as it materializes results locally.
	// 6a) type safe version, discards values with wrong type
	// tag::collect-results-typesafe[]
	val safeNames: Set[String] = result.records.collect.flatMap(_ ("n.name").as[String]).toSet
	// end::collect-results-typesafe[]
	// 6b) unsafe version, throws an exception when value cannot be cast
	// tag::collect-results-nontypesafe[]
	val unsafeNames: Set[String] = result.records.collect.map(_ ("n.name").cast[String]).toSet
	// end::collect-results-nontypesafe[]

	LOG.info(s"\n===========================================================")
	println(safeNames)
	/*
╔═════════╗
║ n.name  ║
╠═════════╣
║ 'Alice' ║
║ 'Bob'   ║
║ 'Eve'   ║
╚═════════╝
	*/
	LOG.info("names ==> "+safeNames)
	// ==> Set(Alice, Bob, Eve)

}
// end::full-example[]

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.cypher.examples.CustomDataFrameExample \
	target/agens-spark-cypher-1.0-dev.jar

*/