package net.bitnine.agens.spark.elastic

import net.bitnine.agens.spark.{AgensBuilder, AgensConf}
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConversions._
import scala.collection.mutable


object AgensElastic extends Serializable {

	private val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

	def hello = (msg:String) => s"Hello, AgensElastic (since 2020-08-01) - msg: $msg"

	def apply(conf: AgensConf): AgensElastic = {
		val conf = new AgensConf(
			"minmac",
			"29200",
			"elastic",
			"bitnine",
			"agensvertex",
			"agensedge"
		)
		new AgensElastic(if( conf != null ) conf else AgensBuilder.defaultConf)
	}

	def main(args: Array[String]): Unit = {

		val elastic = new AgensElastic(AgensBuilder.defaultConf)

		LOG.info(s"\n===========================================================")

		LOG.info("")
		LOG.info("vertex: " + elastic.listDatasources(elastic.conf.vertexIndex))
		LOG.info("edge: " + elastic.listDatasources(elastic.conf.edgeIndex))
		LOG.info("")

		elastic.close
		LOG.info(hello("TEST"))

		val jes = new AgensJavaElastic(AgensBuilder.defaultConf)
		val datasources = jes.datasourcesToScala(elastic.conf.vertexIndex)
		val labels = jes.labelsToScala(elastic.conf.vertexIndex, datasources.head._1)
		val keys = jes.keysToScala(elastic.conf.vertexIndex, datasources.head._1, labels.head._1)

		LOG.info("")
		LOG.info(s"datasources(${datasources.size}): $datasources")
		LOG.info(s"labels(${labels.size}): $labels")
		LOG.info(s"keys(${keys.size}): $keys")
	}
}

/*
spark-submit --executor-memory 1g \
	--master spark://minmac:7077 \
	--class net.bitnine.agens.spark.elastic.AgensElastic \
	target/agens-spark-connector-full-1.0-dev.jar
*/

class AgensElastic(val conf: AgensConf=null) extends Serializable {

	var client: RestHighLevelClient = null;
	val AGG_BUCKET_SIZE: Int = 1000;

	def open(): RestHighLevelClient = {
		val builder = RestClient.builder(
			new HttpHost(conf.host, conf.port.toInt, "http")
		)

		if( conf.user.nonEmpty && conf.user.nonEmpty ){
			val credentialsProvider = new BasicCredentialsProvider
			credentialsProvider.setCredentials(
				AuthScope.ANY,
				new UsernamePasswordCredentials(conf.user, conf.password)
			)
			builder.setHttpClientConfigCallback(
				new RestClientBuilder.HttpClientConfigCallback() {
					override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder =
						httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
				}
			)
		}
		new RestHighLevelClient(builder)
	}

	// **NOTE: 반드시 close 해야 Application 이 종료된다 (안하면 멈춤! callback 대기상태?)
	def close() = {
		if( client != null ){
			try{ client.close() } catch{ case _: Throwable => }	// do nothing
			client = null
		}
	}

	// @throws[Exception]
	def listDatasources(index: String): Map[String,Long] = { // query : aggregation
		if( client == null ) client = open

		val searchSourceBuilder = new SearchSourceBuilder
		searchSourceBuilder
				.query(QueryBuilders.matchAllQuery)
				.aggregation(
					AggregationBuilders.terms("datasources")
							.field("datasource")
							.order(BucketOrder.key(true))
							.size(AGG_BUCKET_SIZE)
				).size(0)
		// request
		val searchRequest = new SearchRequest(index)
		searchRequest.source(searchSourceBuilder)

		val result = new mutable.HashMap[String, Long]()
		try {
			val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
			// response
			val aggregations: Aggregations = searchResponse.getAggregations
			val labels: Terms = aggregations.get("datasources")

			labels.getBuckets.foreach { (b: Terms.Bucket) =>
				result.put(b.getKeyAsString, b.getDocCount)
			}
		}
		catch { case _: Throwable => }
		finally { close }

		result.toMap
	}

	///////////////////////////////////////

	// with datasource
	def elements(spark: SparkSession, esConf: Map[String,String])(index:String, schema: StructType, datasource: String): DataFrame = {
		val query: String = s"""{ "query": { "bool": {
							   |  "must": { "term": { "datasource": "${datasource}" } }
							   |}}}""".stripMargin.replaceAll("\n", " ")
		spark.read.format("org.elasticsearch.spark.sql")		// == format("es")
				.options(esConf)
				.option("es.query", query)
				.schema(schema)
				.load(index)
	}

	// with datasource, labels
	def elements(spark: SparkSession, esConf: Map[String,String])(index: String, schema: StructType, datasource: String, labels: Array[String]): DataFrame = {
		assert(labels != null && labels.size > 0)
		val query: String = s"""{ "query": { "bool": {
							   |  "must": { "term": { "datasource": "${datasource}" } }
							   |  "filter": { "terms": { "label": ["${labels.mkString("\",\"")}"] } }
							   |}}}""".stripMargin.replaceAll("\n", " ")
		spark.read.format("org.elasticsearch.spark.sql")		// == format("es")
				.options(esConf)
				.option("es.query", query)
				.schema(schema)
				.load(index)
	}

}