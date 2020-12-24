package net.bitnine.agens.spark

import net.bitnine.agens.spark.elastic.AgensJavaElastic
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.types.DataTypes.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import scala.collection.mutable


object AgensMeta {

	def apply(conf: AgensConf):AgensMeta = {
		scan(conf)
	}

	def scan(conf: AgensConf): AgensMeta ={
		val elastic = new AgensJavaElastic(conf)

		val dsV = elastic.datasourcesToScala(conf.vertexIndex)
		val dsE = elastic.datasourcesToScala(conf.edgeIndex)

		val meta = new AgensMeta(conf.vertexIndex, conf.edgeIndex)
		import meta._

		// datasource 로 구분된 graph 리스트: vertex 기준으로 같은 datasource 에 edge 삽입
		// all 포함 = 전체 graph (단, modern 은 제외)
		for( (k,v) <- dsV ) {
			// ** Error: return type ANY, type mismatch
			//     dsE.get(k).getOrElse[Long](0)
			// ==> https://stackoverflow.com/a/35330624
			val ds = new MetaDatasource(k, v, dsE.get(k).getOrElse(0).asInstanceOf[Long])
			meta.datasources.put(k, ds)

			// vertices
			val vlabelsTmp = elastic.labelsToScala(conf.vertexIndex, k).asInstanceOf[Map[String, Long]]
			for( (kl,vl) <- vlabelsTmp ) {
				val label = new MetaLabel(kl, vl, conf.vertexIndex)
				ds.vertices.put(kl, label)

				val keysTmp = elastic.keytypesToScala(conf.vertexIndex, k, kl)
				for( (kp, vp) <- keysTmp ){
					val prop = new MetaProperty(kp, vp._1, vp._2, vp._3)
					label.properties.put(kp, prop)
				}
			}

			// edges
			val elabelsTmp = elastic.labelsToScala(conf.edgeIndex, k).asInstanceOf[Map[String, Long]]
			for( (kl,vl) <- elabelsTmp ) {
				val label = new MetaLabel(kl, vl, conf.edgeIndex)
				ds.edges.put(kl, label)

				val keysTmp = elastic.keytypesToScala(conf.edgeIndex, k, kl)
				for( (kp, vp) <- keysTmp ){
					val prop = new MetaProperty(kp, vp._1, vp._2, vp._3)
					label.properties.put(kp, prop)
				}
			}
		}

		meta
	}

/*
	// ** Usage by curring
	// ==> AgensMeta.datasource(datasource)(agens.conf)
	//
	def datasources(datasource: String)(conf: AgensConf): (Long,Long) = {
		assert(conf != null, "need to AgensConfig with initialization")
		val elastic = new AgensJavaElastic(conf)

		val dsV = elastic.datasourcesToScala(conf.vertexIndex)
		val dsE = elastic.datasourcesToScala(conf.edgeIndex)

		(dsV.getOrElse(datasource, 0L).asInstanceOf[Long], dsE.getOrElse(datasource, 0L).asInstanceOf[Long])
	}

	def labels(datasource: String)(conf: AgensConf): (Map[String, Long],Map[String, Long]) = {
		assert(conf != null, "need to AgensConfig with initialization")
		val elastic = new AgensJavaElastic(conf)

		val vlabelTmp = elastic.labelsToScala(conf.vertexIndex, datasource).asInstanceOf[Map[String, Long]]
		val elabelTmp = elastic.labelsToScala(conf.edgeIndex, datasource).asInstanceOf[Map[String, Long]]

		(vlabelTmp, elabelTmp)
	}

	// datasource -> label -> key : (doc_count, type, agg_count)
	def keys(datasource: String, label:String)(conf: AgensConf): Map[String, (String,Long,Boolean)] = {
		assert(conf != null, "need to AgensConfig with initialization")
		val elastic = new AgensJavaElastic(conf)

		val vkeytypesTmp = elastic.keytypesToScala(conf.vertexIndex, datasource, label)

		if( vkeytypesTmp.size > 0)
			vkeytypesTmp.asInstanceOf[Map[String, (String,Long,Boolean)]]
		else
			elastic.keytypesToScala(conf.edgeIndex, datasource, label).asInstanceOf[Map[String, (String,Long,Boolean)]]
	}
*/

}


class AgensMeta(val vertexIndex:String, val edgeIndex:String){

	val schemaBase = StructType(Array(
		StructField("timestamp", TimestampType, false),
		StructField("datasource", StringType, false),
		StructField("id", StringType, false),
		StructField("label", StringType, false)
	))

	case class MetaProperty(
		   name: String, 		// property.key
		   typeStr: String, 	// property.type
		   size: Long = 0,		// property.count
		   hasNull: Boolean = false 	// doc count != prop count
	){
		def dataType():DataType =
			typeStr match {
				case "java.lang.String" => StringType
				case "java.lang.Long" => LongType
				case "java.lang.Double" => DoubleType
				case "java.lang.Float" => FloatType
				case "java.lang.Integer" => IntegerType
				case "java.lang.Short" => ShortType
				case "java.lang.Boolean" => BooleanType
				case "java.lang.Byte" => ByteType
				case "java.sql.Timestamp" => TimestampType
				case "java.sql.Date" => DateType
				case _ => StringType
			}

		// **NOTE: String 타입 parameter 는 정규식으로 입력해야 함 ==> split("[.]")
		override def toString() = s"$name<${typeStr.split('.').last}>"
	}

	case class MetaLabel(
			name: String,
			size: Long = 0,
			index: String,		// from AgensConfig
			properties: mutable.HashMap[String, MetaProperty] = new mutable.HashMap()
	){
		require(index == vertexIndex || index == edgeIndex, "INVALID VALUE: index should be one of indices of AgensConfig")

		def property(name:String): MetaProperty = properties.get(name).getOrElse(null)

		def idxType: String =
			if( index == vertexIndex ) "vertex"
			else if( index == edgeIndex ) "edge"
			else "none"

		def schema: StructType = {
			var schema = schemaBase
			for( p:MetaProperty <- properties.values ){
				schema = schema.add(StructField(p.name, p.dataType(), true))
			}
			schema
		}

		override def toString() = s"$name[${ properties.values.mkString(",") }]"
	}

	case class MetaDatasource(
			name: String,
			sizeVertex: Long = 0,
			sizeEdge: Long = 0,
			vertices: mutable.HashMap[String, MetaLabel] = new mutable.HashMap(),
			edges: mutable.HashMap[String, MetaLabel] = new mutable.HashMap()
	){
		def count = (sizeVertex, sizeEdge)
		def labels = (vertices ++ edges).toMap		// Map.++(Map)

		def label(name:String): MetaLabel = {
			if( vertices.contains(name) ) vertices.get(name).get
			else if( edges.contains(name) ) edges.get(name).get
			else null
		}
		def vlabel(name:String): MetaLabel = {
			if( vertices.contains(name) ) vertices.get(name).get
			else null
		}
		def elabel(name:String): MetaLabel = {
			if( edges.contains(name) ) edges.get(name).get
			else null
		}

		override def toString() = s"$name($sizeVertex,$sizeEdge): vertices={${ vertices.values.mkString(",") }}, edges={${ edges.values.mkString(",") }}"
	}

	val datasources: mutable.HashMap[String, MetaDatasource] = new mutable.HashMap()

	def datasource(name: String): MetaDatasource = datasources.get(name).getOrElse(null)
}

