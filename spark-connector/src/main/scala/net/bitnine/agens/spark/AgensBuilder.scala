package net.bitnine.agens.spark

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class AgensBuilder(
		private val sparkSession: SparkSession,
		private var paramHost: String = null,
		private var paramPort: String = null,
		private var paramUser: String = null,
		private var paramPassword: String = null,
		private var paramVertexIndex: String = null,
		private var paramEdgeIndex: String = null,
		private var paramTempPath: String = null
) {
	def host(host: String):AgensBuilder = copy(paramHost=host)
	def port(port: String):AgensBuilder = copy(paramPort=port)
	def user(user: String):AgensBuilder = copy(paramUser=user)
	def password(password: String):AgensBuilder = copy(paramPassword=password)
	def vertexIndex(vertexIndex: String):AgensBuilder = copy(paramVertexIndex=vertexIndex)
	def edgeIndex(edgeIndex: String):AgensBuilder = copy(paramEdgeIndex=edgeIndex)
	def tempPath(tempPath: String):AgensBuilder = copy(paramTempPath=tempPath)

	def build: Agens = {
		require(sparkSession != null, "Spark session must be given for creating Agens instance")

		val conf = new AgensConf()
		if( paramHost != null ) conf.host = paramHost
		if( paramPort != null ) conf.port = paramPort
		if( paramUser != null ) conf.user = paramUser
		if( paramPassword != null ) conf.password = paramPassword
		if( paramVertexIndex != null ) conf.vertexIndex = paramVertexIndex
		if( paramEdgeIndex != null ) conf.edgeIndex = paramEdgeIndex
		if( paramTempPath != null ) conf.tempPath = paramTempPath

		new Agens(sparkSession, conf)
	}
}

object AgensBuilder{

	private val prefix = "spark.agens"

	def apply(sparkSession: SparkSession): AgensBuilder = {
		val sparkConf = sparkSession.sparkContext.getConf
		sparkConf.set("spark.sql.codegen.wholeStage", "true")
		sparkConf.set("spark.sql.shuffle.partitions", "12")
		sparkConf.set("spark.default.parallelism", "8")

		val host = sparkConf.getOption(s"$prefix.host").getOrElse(null)
		val port = sparkConf.getOption(s"$prefix.port").getOrElse(null)
		val vertexIndex = sparkConf.getOption(s"$prefix.vertexIndex").getOrElse(null)
		val edgeIndex = sparkConf.getOption(s"$prefix.edgeIndex").getOrElse(null)
		val tempPath = sparkConf.getOption(s"$prefix.tempPath").getOrElse(null)

		val user = sparkConf.getOption(s"$prefix.user").getOrElse(null)
		val password = sparkConf.getOption(s"$prefix.password").getOrElse(null)

		new AgensBuilder(sparkSession, host, port, user, password, vertexIndex, edgeIndex, tempPath)
	}

	private def local(): SparkSession = {
		val sparkConf = new SparkConf(true)
		sparkConf.set("spark.sql.codegen.wholeStage", "true")
		sparkConf.set("spark.sql.shuffle.partitions", "12")
		sparkConf.set("spark.default.parallelism", "8")

		val session = SparkSession
				.builder()
				.config(sparkConf)
				.master("local[*]")
				.appName(s"agens-local-${UUID.randomUUID()}")
				.getOrCreate()

		session.sparkContext.setLogLevel("error")
		session
	}

	def defaultConf: AgensConf = new AgensConf(
		"minmac",
		"29200",
		"elastic",
		"bitnine",
		"agensvertex",
		"agensedge",
		"/user/agens/temp"
	)

	def default(sparkSession: SparkSession = null): Agens = {
		if( sparkSession == null ) new Agens(local(), defaultConf)
		else new Agens(sparkSession, defaultConf)
	}
}