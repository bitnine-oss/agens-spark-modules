package net.bitnine.agens.livy.job

import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.SparkSession

class WrappedAvroJob extends Job[java.lang.String] {
	override def call(jc: JobContext): java.lang.String = {
		val spark: SparkSession = jc.sparkSession()

		try {
//			val jarName = "hdfs://minmac:9000/user/agens/lib/agens-spark-connector-1.0-dev.jar"
//			var classLoader = new java.net.URLClassLoader(
//				Array(new URI(jarName).toURL),
//				this.getClass.getClassLoader
//			)
//			val clazz = classLoader.loadClass("AvroWriteJob")

			val clazz = Class.forName("net.bitnine.agens.spark.livy.AvroWriteJob")

			val ctor = clazz.getConstructor()
			val instance = ctor.newInstance()

			val method = clazz.getDeclaredMethod("run", classOf[org.apache.spark.sql.SparkSession])

			val result = method.invoke(instance, spark).asInstanceOf[java.lang.String]
			println("** result ==> " + result)

			result
		}
		catch{
			case _: Throwable => return null
		}
	}
}

/*
// It's trick for calling 'object' : "net.bitnine.agens.spark.livy.TestAvroJob"+"$"
// val argClasses = Array[Class[_]]()	// empty Array of Class
// verify : clazz.getDeclaredConstructors
// clazz.getConstructor(Array[Class[_]]())

// for connect remote
spark-shell --master spark://minmac:7077 --jars hdfs://minmac:9000/user/agens/lib/agens-spark-connector-1.0-dev.jar

val clazz = Class.forName("net.bitnine.agens.spark.livy.job.AvroWriteJob")
val ctor = clazz.getConstructor()
val instance = ctor.newInstance()
val method = clazz.getDeclaredMethod("run", classOf[org.apache.spark.sql.SparkSession])
val result = method.invoke(instance, spark).asInstanceOf[java.lang.String]

 */