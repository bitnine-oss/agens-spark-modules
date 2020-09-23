package net.bitnine.agens.spark

import net.bitnine.agens.cypher.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}


class AgensHelper

object AgensHelper {

	def hello = (msg:String) => s"Hello, $msg - AgensSparkConnector (since 2020-08-01)"

	// 1) ArrayType => MapType
	// 2) property.`type` => DataType
	// ==> select( col("id"), col("properties")("name").as("name!") )
	// https://medium.com/@mrpowers/working-with-spark-arraytype-and-maptype-columns-4d85f3c8b2b3

	// id, label, name$[type,valStr], age$
	// (key, type, valStr) => (key -> (type, valStr))
	// 3) df.withColumn("name",col("name$")[0].cast(col("name$")[1])	// ..cast(BooleanType))

	// **NOTE: UDF 에서는 Any 타입을 지원하지 않는다
	// https://stackoverflow.com/a/46274802

	// **ref https://mungingdata.com/apache-spark/chaining-custom-dataframe-transformations/
	def convStr2Value(colName: String, convType:DataType)(df: DataFrame): DataFrame = {
		df.withColumn( colName+"$", col(colName).cast(convType) )
				.drop( col(colName) )
	}

	// ** transform steps:
	// 0) parameters : datasource, label(vertex/edge)
	// 1) explode properties( array of property )
	// 2) for loop : keysMeta
	//   2-1) select datasource, label, id, property from DF where property.key = keyMeta
	//	 2-2) convert property to column with datatype
	//	 2-3) append column and drop old column(= property)
	//	 2-4) left outer join
	// 3) output dataframe with appended all keys as field

	// **ref https://mungingdata.com/apache-spark/chaining-custom-dataframe-transformations/
	def withConvStrValue(colName: String, convType:DataType)(df: DataFrame): DataFrame = {
		df.withColumn( colName+"$", col(colName).cast(convType) )
				.drop( col(colName) )
	}

	//////////////////////////////////////////////

	// **NOTE: convDf 의 withColumn 에서 제거 ==> 원본 컬럼 name 그대로 사용
	// val propertyIdentityChar = "_"

	def explodeVertex(df: DataFrame, datasource:String, label:String)(implicit meta: AgensMeta): DataFrame = {
		val metaLabel = meta.datasource(datasource).vlabel(label)
		require(metaLabel != null)

		// STEP0: Base Dataframe
		var baseDf = df.select(
			col("timestamp"),
			col("datasource"),
			col("label"),
			col("id"),
			col("id").as("id_")		// preservation
		)

		// STEP1: explode nested array field about properties
		val tmpDf = df.select(
			col("timestamp"),
			col("datasource"),
			col("label"),
			col("id"),
			explode(col("properties")).as("property")
		)

		// STEP2: convert property to column
		val baseCols = Seq("timestamp","datasource","label","id")
		metaLabel.properties.values.foreach { p:meta.MetaProperty =>
			val df = tmpDf.filter(col("property.key") === p.name)
					.withColumn("tmp$", col("property.value"))
			val convDf = df.withColumn( p.name, col("tmp$").cast(p.dataType()) )
					.drop( col("property") ).drop(col("tmp$"))
			baseDf = baseDf.join(convDf, baseCols,"left")
		}
		baseDf
	}

	def explodeEdge(df: DataFrame, datasource:String, label:String)(implicit meta: AgensMeta): DataFrame = {
		val metaLabel = meta.datasource(datasource).elabel(label)
		require(metaLabel != null)

		// STEP0: Base Dataframe
		var baseDf = df.select(
			col("timestamp"),
			col("datasource"),
			col("label"),
			col("id"),
			col("src").as("source"),
			col("dst").as("target"),
			col("id").as("id_"),		// preservation
			col("src"),							// preservation
			col("dst")							// preservation
		)

		// STEP1: explode nested array field about properties
		val tmpDf = df.select(
			col("timestamp"),
			col("datasource"),
			col("label"),
			col("id"),
			col("src").as("source"),
			col("dst").as("target"),
			explode(col("properties")).as("property")
		)

		// STEP2: convert property to column
		val baseCols = Seq("timestamp","datasource","label","id","source","target")
		metaLabel.properties.values.foreach { p:meta.MetaProperty =>
			val df = tmpDf.filter(col("property.key") === p.name)
					.withColumn("tmp$", col("property.value"))
			val convDf = df.withColumn( p.name, col("tmp$").cast(p.dataType()) )
					.drop( col("property") ).drop(col("tmp$"))
			baseDf = baseDf.join(convDf, baseCols,"left")
		}
		baseDf
	}

	def wrappedVertexTable(df: DataFrame, label: String): CAPSEntityTable = {
//		val mapping = NodeMappingBuilder
//				.withSourceIdKey("id")
//				.withImpliedLabel(label)
//				.build
//		CAPSEntityTable.create(mapping, df)

		CAPSNodeTable(Set(label), df)
	}

	def wrappedEdgeTable(df: DataFrame, label: String): CAPSEntityTable = {
//		val mapping = RelationshipMappingBuilder
//				.withSourceIdKey("id")
//				.withSourceStartNodeKey("src")
//				.withSourceEndNodeKey("dst")
//				.withRelType(label)
//				.build
//		CAPSEntityTable.create(mapping, df)

		CAPSRelationshipTable(label, df)
	}

	def savePath(tempPath: String, name: String): String = {
		s"$tempPath/$name.avro"
	}
}
