package net.bitnine.agens.cypher.api.io.fs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import net.bitnine.agens.cypher.impl.table.SparkTable._


trait EscapeAtSymbol extends FSGraphSource {

  private val unicodeEscaping = "_specialCharacterEscape_"
  private val atSymbol = "@"

  abstract override def writeTable(path: String, table: DataFrame): Unit = {
    schemaCheck(table.schema)
    val writeMapping = encodedColumnNames(table.schema)
    val newTable = table.safeRenameColumns(writeMapping: _*)
    super.writeTable(path, newTable)
  }

  abstract override def readTable(path: String, schema: StructType): DataFrame = {
    val readMapping = encodedColumnNames(schema).toMap
    val readSchema = StructType(schema.fields.map { f =>
      f.copy(name = readMapping.getOrElse(f.name, f.name))
    })

    val outMapping = readMapping.map(_.swap).toSeq
    super.readTable(path, readSchema).safeRenameColumns(outMapping: _*)
  }

  private def encodedColumnNames(schema: StructType): Seq[(String, String)] = {
    schema.fields
      .map(f => f.name -> f.name.replaceAll(atSymbol, unicodeEscaping))
      .filterNot { case (from, to) => from == to}
  }

  private def schemaCheck(schema: StructType): Unit = {
    val invalidFields = schema.fields.filter(f => f.name.contains(unicodeEscaping)).map(_.name)
    if (invalidFields.nonEmpty) sys.error(s"Orc fields: $invalidFields cannot contain special encoding string: '$unicodeEscaping'")
  }

}
