package net.bitnine.agens.spark

import net.bitnine.agens.cypher.api.{CAPSSession, FSGraphSources}
import net.bitnine.agens.cypher.api.CAPSSession._
import net.bitnine.agens.cypher.api.io.util.CAPSGraphExport.CanonicalTableExport
import net.bitnine.agens.cypher.api.io.util.HiveTableName
import net.bitnine.agens.cypher.api.io.CAPSEntityTable
import net.bitnine.agens.cypher.impl.CAPSConverters.RichPropertyGraph
import net.bitnine.agens.cypher.impl.CAPSRecords
import net.bitnine.agens.spark.Agens.{schemaE, schemaV}
import net.bitnine.agens.spark.AgensHelper.{explodeEdge, explodeVertex, wrappedEdgeTable, wrappedVertexTable}

import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.opencypher.okapi.api.graph.{CypherResult, GraphName, Namespace, Node, PropertyGraph, Relationship}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult
import org.opencypher.okapi.api.value.CypherValue.CypherMap

import scala.language.implicitConversions


object Agens extends Serializable {

	val schemaP = StructType( Array(
		StructField("key", StringType, false),
		StructField("type", StringType, false),
		StructField("value", StringType, false)
	))
	// ** Equal to ==> Encoders.product[Agens.ElasticVertex].schema,
	val schemaV = StructType( Array(
		StructField("timestamp", TimestampType, false),
		StructField("datasource", StringType, false),
		StructField("id", StringType, false),
		StructField("label", StringType, false),
		StructField("properties", new ArrayType(schemaP, true), false)
	))
	// ** Equal to ==> Encoders.product[Agens.ElasticEdge].schema,
	val schemaE = schemaV.
			add(StructField("src", StringType, false)).
			add(StructField("dst", StringType, false))

	implicit class ResultsAsDF(val results: CypherResult) extends AnyVal {
		def asDataFrame: DataFrame = results.records match {
			case caps: CAPSRecords => caps.table.df
			case _ => throw UnsupportedOperationException(s"can only handle CAPS results, got ${results.records}")
		}
		def asDataset: Dataset[CypherMap] = results.records match {
			case caps: CAPSRecords => caps.toCypherMaps
			case _ => throw UnsupportedOperationException(s"can only handle CAPS results, got ${results.records}")
		}
	}

	def main(args: Array[String]): Unit = {

		var query = "FROM GRAPH modern " +
					"MATCH (p:person {country:'USA'})-[e:knows]-(t:person) " +
					"WHERE p.age < 35 " +
					"RETURN p.name, p.age, t.name, t.country, e.id_, e.src, e.dst"
		if( args.length > 0 ) query = args(0)

		var saveName = "test_query01"
		if( args.length > 1 ) saveName = args(1)

		val JOB_NAME: String = "Agens.main"
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

		// default namespace: session
		agens.catalog.store("modern", graphModern)
		println("\n** [start] graphs: "+agens.catalog.graphNames.map(_.graphName).mkString("[","],[","]"))
		// start ==> graphs: [emptyGraph],[modern]

		// vertices: id, label
		val result1 = agens.cypher("FROM GRAPH session.modern MATCH (n) RETURN n.id_, n.label")
		result1.show
		// **NOTE: edge 양방향 탐색이라 distinct 해 주어야 중복 안됨
		// edges: id, label, source, target
		val result2 = agens.cypher("FROM GRAPH session.modern MATCH ()-[e]-() RETURN distinct e.id_, e.label, e.src, e.dst")
		result2.show

		// query: {name, age}, {name, country}
		println("\n** query => "+query)
		val result3 = agens.cypher(query)
		result3.show

		// save to '/user/agens/temp' as avro
		println("\n** tempPath ==> "+ agens.conf.tempPath)
		agens.saveResultAsAvro(result3, saveName)
	}

}
/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.spark.Agens \
	target/agens-spark-connector-1.0-dev.jar

*/

class Agens(spark: SparkSession, val conf: AgensConf) extends Serializable {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)
	require(spark != null, "Spark session must be given before using AgensSparkConnector")

	implicit private val meta: AgensMeta = AgensMeta(conf)
	require(meta != null, "Fail to meta scan about AgensGraph. Check elasticsearch config")

	spark.sparkContext.setLogLevel("error")
	implicit private val session: CAPSSession = CAPSSession.create(spark)

	implicit val catalog:CypherCatalog = session.catalog

	// val emptyDf = this.spark.createDataFrame(this.spark.sparkContext.emptyRDD[Row], schemaV)
	val emptyGraph = this.catalog.graph("emptyGraph")
	require(emptyGraph != null, "Fail to initializing CAPSSession and not found emptyGraph")

	///////////////////////////////////////


	// def metaRoot = this.meta.datasources

	def count(datasource:String): Long = {
		assert(meta.datasources.contains(datasource), "wrong datasource")
		this.vertices(datasource).count() + this.edges(datasource).count()
	}

//	def graphFrame(datasource: String):GraphFrame = {
//		assert(meta.datasources.contains(datasource), "wrong datasource")
//		GraphFrame(this.vertices(datasource), this.edges(datasource))
//	}

	def graphs(): PropertyGraph = {
		val vlabels = meta.datasource().vertices.keys.toSet
		val vertexTables:List[CAPSEntityTable] = vlabels.map{ label =>
			readVertexAsTable(datasource, label)
		}.toList
		val elabels = meta.datasource(datasource).edges.keys.toSet
		val edgeTables:List[CAPSEntityTable] = elabels.map{ label =>
			readEdgeAsTable(datasource, label)
		}.toList

		session.readFrom(vertexTables ++ edgeTables)
	}

	def graph(datasource: String): PropertyGraph = {
		val vlabels = meta.datasource(datasource).vertices.keys.toSet
		val vertexTables:List[CAPSEntityTable] = vlabels.map{ label =>
			readVertexAsTable(datasource, label)
		}.toList
		val elabels = meta.datasource(datasource).edges.keys.toSet
		val edgeTables:List[CAPSEntityTable] = elabels.map{ label =>
			readEdgeAsTable(datasource, label)
		}.toList

		session.readFrom(vertexTables ++ edgeTables)
	}

	// Agens Cypher
	def cypher(datasource: String, query: String): CypherResult = {
		val graph: PropertyGraph = this.graph(datasource)
		if( graph == null ) return RelationalCypherResult.empty
		graph.cypher(query)
	}

	def cypher(query: String): CypherResult = {
		if( !query.toUpperCase.contains("FROM GRAPH") ) return RelationalCypherResult.empty
		this.session.cypher(query)
	}

	// Spark SQL
	def sql(query: String): DataFrame = {
		if( this.session == null ) null
		this.session.sparkSession.sql(query)
	}

	///////////////////////////////////////

	// for Multi-graph
	def registerSource(namespace: Namespace, dataSource: PropertyGraphDataSource): Unit =
		this.session.registerSource(namespace, dataSource)

	// for FSGraphSource
	def fsGraphsource(rootPath: String,
					  hiveDatabaseName: Option[String] = None,
					  filesPerTable: Option[Int] = Some(1)) = {
		FSGraphSources(rootPath, hiveDatabaseName, filesPerTable)
	}

	// by Parquet format
	def saveGraphToHive(graphSource: PropertyGraph, gName: String, dbPath: String="agens"): Unit = {
		this.session.sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $dbPath")

		val graphName = GraphName( gName )
		val graph = graphSource.asCaps		// RelationalCypherGraph[DataFrameTable]
		val schema = graphSource.schema		// Schema

		val nodeWrites = schema.labelCombinations.combos.map { combo =>
			val nodeType = combo.toList.sorted.mkString("_")		// multi-label 이라서?
			val tableName = HiveTableName(dbPath, graphName, Node, Set(nodeType.toLowerCase))
			val df = graph.canonicalNodeTable(combo)
			df.write.mode("overwrite").saveAsTable(tableName)
			tableName
		}
		val relWrites = schema.relationshipTypes.map { relType =>
			val tableName = HiveTableName(dbPath, graphName, Relationship, Set(relType.toLowerCase))
			val df = graph.canonicalRelationshipTable(relType)
			df.write.mode("overwrite").saveAsTable(tableName)
			tableName
		}
		// for DEBUG
		println(s"** Vertices: $nodeWrites")
		println(s"** Edges: $relWrites")
	}

	// **NOTE: 아래 쿼리가 DF로 save 되는 과정에서 오류 발생. 해결 필요!! (alias 해도 마찬가지)
	//
	// query: match (a:person)-[:knows]->(c:person) return a as a_person, c as c_person
	// ==> org.apache.avro.SchemaParseException: Illegal character in: a_person:person
	// ==> 컬럼에 대한 mapping 문제. node, edge 등 특수 경우에 대한 converter를 만들어야 할지도
	//
	//	at org.apache.avro.Schema.validateName(Schema.java:1151)
	//	at org.apache.avro.Schema.access$200(Schema.java:81)
	//	at org.apache.avro.Schema$Field.<init>(Schema.java:403)
	//	at org.apache.avro.SchemaBuilder$FieldBuilder.completeField(SchemaBuilder.java:2124)
	//	at org.apache.avro.SchemaBuilder$FieldBuilder.completeField(SchemaBuilder.java:2120)
	//	at org.apache.avro.SchemaBuilder$FieldBuilder.access$5200(SchemaBuilder.java:2034)
	//	at org.apache.avro.SchemaBuilder$GenericDefault.noDefault(SchemaBuilder.java:2417)
	//	at org.apache.spark.sql.avro.SchemaConverters$$anonfun$5.apply(SchemaConverters.scala:177)
	//	at org.apache.spark.sql.avro.SchemaConverters$$anonfun$5.apply(SchemaConverters.scala:174)

	def saveResultAsAvro(result: CypherResult, saveName: String): String = {
		val df = result.records.asDataFrame
		val savePath = AgensHelper.savePath(this.conf.tempPath, saveName)
		// DataFrameWriter.scala 230, 272 라인에서 오류
		df.write.mode(SaveMode.Overwrite).format("avro").save(savePath)
		savePath
	}

	///////////////////////////////////////

	// ** TRY: curring function ==> fail for overloaded
	// val elements = AgensHelper.elements(spark,conf.es) _

	// with datasource
	private def elements(index: String, schema: StructType, datasource: String): DataFrame = {
		val query: String = s"""{ "query": { "bool": {
			   |  "must": { "term": { "datasource": "${datasource}" } }
			   |}}}""".stripMargin.replaceAll("\n", " ")
		LOG.info(s"load Vertex Dataframe from '${datasource}'")
		spark.read.format("org.elasticsearch.spark.sql")		// == format("es")
				.options(conf.es)
				.option("es.query", query)
				.schema(schema)
				.load(index)
	}

	private def vertices(datasource: String): DataFrame = {
		LOG.info(s"load Vertex Dataframe from '${datasource}'")
		elements(conf.vertexIndex, schemaV, datasource)
	}
	private def edges(datasource: String): DataFrame = {
		LOG.info(s"load Edge Dataframe from '${datasource}'")
		elements(conf.edgeIndex, schemaE, datasource)
	}

	// with datasource, labels
	private def elements(index: String, schema: StructType, datasource: String, label: String): DataFrame = {
		assert(datasource != null && label != null)
		val query: String = s"""{ "query": { "bool": {
			   |  "must": { "term": { "datasource": "${datasource}" } },
			   |  "must": { "term": { "label": "${label}" } }
			   |}}}""".stripMargin.replaceAll("\n", " ")
		spark.read.format("org.elasticsearch.spark.sql")		// == format("es")
				.options(conf.es)
				.option("es.query", query)
				.schema(schema)
				.load(index)
	}
	private def vertices(datasource: String, label: String): DataFrame = {
		LOG.info(s"load Vertex Dataframe from '${datasource}.${label}")
		elements(conf.vertexIndex, schemaV,	datasource, label)
	}
	private def edges(datasource: String, label: String): DataFrame = {
		LOG.info(s"load Edge Dataframe from '${datasource}.${label}'")
		elements(conf.edgeIndex, schemaE, datasource, label)
	}

	///////////////////////////////////////

	def readVertexAsDf(datasource:String, label:String): DataFrame = {
		explodeVertex(vertices(datasource, label), datasource, label)
	}
	def readEdgeAsDf(datasource:String, label:String): DataFrame = {
		explodeEdge(edges(datasource, label), datasource, label)
	}

	def readVertexAsTable(datasource:String, label:String): CAPSEntityTable = {
		val df = explodeVertex(vertices(datasource, label), datasource, label)
		// df.show(false)
		wrappedVertexTable(df, label)
	}
	def readEdgeAsTable(datasource:String, label:String): CAPSEntityTable = {
		val df = explodeEdge(edges(datasource, label), datasource, label)
		// df.show(false)
		wrappedEdgeTable(df, label)
	}

	///////////////////////////////////////

}

/*
val spark = SparkSession.builder().master("local").getOrCreate()

// Do all your operations and save it on your Dataframe say (dataFrame)
dataframe.write.avro("/tmp/output")
dataframe.write.format("avro").save(outputPath)
dataframe.write.format("avro").saveAsTable(hivedb.hivetable_avro)
 */