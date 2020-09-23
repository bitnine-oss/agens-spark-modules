package net.bitnine.agens.cypher.api.io.fs

import java.net.URI

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}
import org.opencypher.okapi.api.graph.{GraphName, Node, Relationship}

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.api.io.fs.HadoopFSHelpers._
import net.bitnine.agens.cypher.api.io.json.JsonSerialization
import net.bitnine.agens.cypher.api.io.util.HiveTableName
import net.bitnine.agens.cypher.api.io.{AbstractPropertyGraphDataSource, FileFormat, StorageFormat}
import net.bitnine.agens.cypher.impl.convert.SparkConversions._
import net.bitnine.agens.cypher.impl.table.SparkTable._


/**
  * Data source implementation that handles the writing of files and tables to a filesystem.
  *
  * By default Spark is used to write tables and the Hadoop filesystem configured in Spark is used to write files.
  * The file/folder/table structure into which the graphs are stored is defined in [[DefaultGraphDirectoryStructure]].
  *
  * @param rootPath           path where the graphs are stored
  * @param tableStorageFormat Spark configuration parameter for the table format
  * @param customFileSystem   optional alternative filesystem to use for writing files
  * @param filesPerTable      optional parameter that specifies how many files a table is coalesced into, by default 1
  */
class FSGraphSource(
  val rootPath: String,
  val tableStorageFormat: StorageFormat,
  val hiveDatabaseName: Option[String] = None,
  val filesPerTable: Option[Int] = None
)(override implicit val caps: CAPSSession)
  extends AbstractPropertyGraphDataSource with JsonSerialization {

  protected val directoryStructure = DefaultGraphDirectoryStructure(rootPath)

  import directoryStructure._

  protected lazy val fileSystem: FileSystem = {
    FileSystem.get(new URI(rootPath), caps.sparkSession.sparkContext.hadoopConfiguration)
  }

  protected def listDirectories(path: String): List[String] = fileSystem.listDirectories(path)

  protected def deleteDirectory(path: String): Unit = fileSystem.deleteDirectory(path)

  protected def readFile(path: String): String = fileSystem.readFile(path)

  protected def writeFile(path: String, content: String): Unit = fileSystem.writeFile(path, content)

  protected def readTable(path: String, schema: StructType): DataFrame = {
    caps.sparkSession.read.format(tableStorageFormat.name).schema(schema).load(path)
  }

  protected def writeTable(path: String, table: DataFrame): Unit = {
    val coalescedTable = filesPerTable match {
      case None => table
      case Some(numFiles) => table.coalesce(numFiles)
    }
    coalescedTable.write.format(tableStorageFormat.name).save(path)
  }

  override protected def listGraphNames: List[String] = {
    listDirectories(rootPath)
  }

  override protected def deleteGraph(graphName: GraphName): Unit = {
    deleteDirectory(pathToGraphDirectory(graphName))
    if (hiveDatabaseName.isDefined) {
      deleteHiveDatabase(graphName)
    }
  }

  override protected def readNodeTable(
    graphName: GraphName,
    labels: Set[String],
    sparkSchema: StructType
  ): DataFrame = {
    readTable(pathToNodeTable(graphName, labels), sparkSchema)
  }

  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit = {
    writeTable(pathToNodeTable(graphName, labels), table)
    if (hiveDatabaseName.isDefined) {
      val hiveNodeTableName = HiveTableName(hiveDatabaseName.get, graphName, Node, labels)
      writeHiveTable(pathToNodeTable(graphName, labels), hiveNodeTableName, table.schema)
    }
  }

  override protected def readRelationshipTable(
    graphName: GraphName,
    relKey: String,
    sparkSchema: StructType
  ): DataFrame = {
    readTable(pathToRelationshipTable(graphName, relKey), sparkSchema)
  }

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = {
    writeTable(pathToRelationshipTable(graphName, relKey), table)
    if (hiveDatabaseName.isDefined) {
      val hiveRelationshipTableName = HiveTableName(hiveDatabaseName.get, graphName, Relationship, Set(relKey))
      writeHiveTable(pathToRelationshipTable(graphName, relKey), hiveRelationshipTableName, table.schema)
    }
  }

  private def writeHiveTable(pathToTable: String, hiveTableName: String, schema: StructType): Unit = {
    caps.sparkSession.catalog.createTable(hiveTableName, tableStorageFormat.name, schema, Map("path" -> pathToTable))
    caps.sparkSession.catalog.refreshTable(hiveTableName)
  }

  private def deleteHiveDatabase(graphName: GraphName): Unit = {
    val graphSchema = schema(graphName).get
    val labelCombinations = graphSchema.labelCombinations.combos
    val relTypes = graphSchema.relationshipTypes

    labelCombinations.foreach { combo =>
      val tableName = HiveTableName(hiveDatabaseName.get, graphName, Node, combo)
      caps.sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    relTypes.foreach { relType =>
      val tableName = HiveTableName(hiveDatabaseName.get, graphName, Relationship, Set(relType))
      caps.sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  override protected def readJsonSchema(graphName: GraphName): String = {
    readFile(pathToGraphSchema(graphName))
  }

  override protected def writeJsonSchema(graphName: GraphName, schema: String): Unit = {
    writeFile(pathToGraphSchema(graphName), schema)
  }

  override protected def readJsonCAPSGraphMetaData(graphName: GraphName): String = {
    readFile(pathToCAPSMetaData(graphName))
  }

  override protected def writeJsonCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: String): Unit = {
    writeFile(pathToCAPSMetaData(graphName), capsGraphMetaData)
  }

}

/**
  * Spark CSV does not support storing BinaryType columns by default. This data source implementation encodes BinaryType
  * columns to Hex-encoded strings and decodes such columns back to BinaryType. This feature is required because ids
  * within CAPS are stored as BinaryType.
  */
class CsvGraphSource(rootPath: String, filesPerTable: Option[Int] = None)(override implicit val caps: CAPSSession)
  extends FSGraphSource(rootPath, FileFormat.csv, None, filesPerTable) {

  override protected def writeTable(path: String, table: DataFrame): Unit =
    super.writeTable(path, table.encodeBinaryToHexString)

  protected override def readNodeTable(graphName: GraphName, labels: Set[String], sparkSchema: StructType): DataFrame =
    readEntityTable(graphName, Left(labels), sparkSchema)

  protected override def readRelationshipTable(
    graphName: GraphName,
    relKey: String,
    sparkSchema: StructType
  ): DataFrame = readEntityTable(graphName, Right(relKey), sparkSchema)

  private def readEntityTable(
    graphName: GraphName,
    labelsOrRelKey: Either[Set[String], String],
    sparkSchema: StructType
  ): DataFrame = {
    val readSchema = sparkSchema.convertTypes(BinaryType, StringType)

    val tableWithEncodedStrings = labelsOrRelKey match {
      case Left(labels) => super.readNodeTable(graphName, labels, readSchema)
      case Right(relKey) => super.readRelationshipTable(graphName, relKey, readSchema)
    }

    tableWithEncodedStrings.decodeHexStringToBinary(sparkSchema.binaryColumns)
  }
}
