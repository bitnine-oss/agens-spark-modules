package net.bitnine.agens.livy.job

import org.apache.livy.{Job, JobContext}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext.toSparkContext

class PiJob(val slices: java.lang.Integer) extends Job[java.lang.Double] {

	val samples = Math.min(100000L * slices, Integer.MAX_VALUE).toInt

	override def call(jc: JobContext): java.lang.Double = {
		val sc = toSparkContext(jc.sc())
		callScala(sc)
	}

	def callScala(sc: SparkContext): java.lang.Double = {
		// Pi Estimation
		// https://spark.apache.org/examples.html

		val count = sc.parallelize(1 to samples).filter { _ =>
			val x = math.random
			val y = math.random
			x*x + y*y < 1
		}.count()

		val pi = 4.0 * count / samples
		pi
	}

}
