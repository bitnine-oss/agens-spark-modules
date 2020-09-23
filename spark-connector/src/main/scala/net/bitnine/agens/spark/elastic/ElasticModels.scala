package net.bitnine.agens.spark.elastic

import java.sql.Timestamp

import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}


object ElasticModels {

	case class ElasticProperty(key:String, `type`:String, value:String)
	case class ElasticElement(id:String, property:ElasticProperty)
	case class ElasticVertex(timestamp:Timestamp, datasource:String, id:String, label:String, properties:Array[ElasticProperty])
	case class ElasticEdge(timestamp:Timestamp, datasource:String, id:String, label:String, properties:Array[ElasticProperty], src:String, dst:String)

	val schemaProperty = new StructType(Array(
			StructField("key", StringType, false),
			StructField("type", StringType, false),
			StructField("value", StringType, false)
	))
	val schemaBase = new StructType(Array(
			StructField("id", StringType, false),
			StructField("property", schemaProperty, false)
	))
	val schemaVertex = StructType( Array(
			StructField("timestamp", TimestampType, false),
			StructField("datasource", StringType, false),
			StructField("id", StringType, false),
			StructField("label", StringType, false),
			StructField("properties", new ArrayType(schemaProperty, true), false)
	))
	val schemaEdge = schemaVertex.
			add( StructField("src", StringType, false)).
			add( StructField("dst", StringType, false))
}
