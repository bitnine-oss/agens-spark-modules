package net.bitnine.agens.livy.job

import net.bitnine.agens.livy.util.SchemaConverters
import org.apache.avro.SchemaBuilder
import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

class AvroWriteJob(val name: java.lang.String) extends Job[java.lang.String] {

	override def call(jc: JobContext): java.lang.String = {
		val spark: SparkSession = jc.sparkSession()
		callScala(spark)
	}

	def callScala(spark: SparkSession): java.lang.String = {
		val columns = Seq("firstname", "middlename", "lastname", "dob_year", "dob_month", "gender", "salary")
		val data = Seq(("James ","","Smith",2018,1,"M",3000),
			("Michael ","Rose","",2010,3,"M",4000),
			("Robert ","","Williams",2010,3,"M",4000),
			("Maria ","Anne","Jones",2005,5,"F",4000),
			("Jen","Mary","Brown",2010,7,"",-1)
		)

		import spark.sqlContext.implicits._
		val df = data.toDF(columns:_*)

		df.write.mode(SaveMode.Overwrite)
				//.partitionBy("dob_year")
				.format("avro")
				.save(s"/user/agens/temp/$name.avro")

		val dataSchema = df.schema
		val options: Map[String, String] = Map("recordName"->s"avro_$name", "recordNamespace"->"net.bitnine.agens.hive")
		val recordName = options.getOrElse("recordName", "topLevelRecord")
		val recordNamespace = options.getOrElse("recordNamespace", "")
		val build = SchemaBuilder.record(recordName).namespace(recordNamespace)
		val outputAvroSchema = SchemaConverters.convertStructToAvro(dataSchema, build, recordNamespace)

		outputAvroSchema.toString(true)
	}

}

/*
java -cp target/agens-livy-test-1.0-dev.jar net.bitnine.agens.livytest.AvroWriteRun http://minmac:8998
==>
Uploading livy-example jar to the SparkContext...
Avro Schema ==> {
	"type":"record",
	"name":"avro_person",
	"namespace":"net.bitnine.agens.hive",
	"fields":[
		{"name":"firstname","type":["string","null"]},
		{"name":"middlename","type":["string","null"]},
		{"name":"lastname","type":["string","null"]},
		{"name":"dob_year","type":"int"},
		{"name":"dob_month","type":"int"},
		{"name":"gender","type":["string","null"]},
		{"name":"salary","type":"int"}
	]}
 */

/*

// Creating the string from an existing dataframe
val schema = df.schema
val jsonString = schema.json

// create a schema from json
import org.apache.spark.sql.types.{DataType, StructType}
val newSchema = DataType.fromJson(jsonString).asInstanceOf[StructType]

----------------------------

val schemaAvro = new Schema.Parser()
      .parse(new File("src/main/resources/person.avsc"))

val df = spark.read
              .format("avro")
              .option("avroSchema", schemaAvro.toString)
              .load("person.avro")

 */