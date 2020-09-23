package net.bitnine.agens.cypher.api.io.sql

import ujson.Value

import scala.util.{Failure, Success, Try}

import net.bitnine.agens.cypher.api.io.{FileFormat, HiveFormat, JdbcFormat, StorageFormat}


case class SqlDataSourceConfigException(msg: String, cause: Throwable = null) extends Throwable(msg, cause)


import org.opencypher.okapi.impl.util.JsonUtils.FlatOption._

sealed abstract class SqlDataSourceConfig(
  val format: StorageFormat,
  val options: Map[String, String]
)

object SqlDataSourceConfig {
  private implicit val jdbc: ReadWriter[Jdbc] = macroRW
  private implicit val hive: ReadWriter[Hive.type] = macroRW
  private implicit val file: ReadWriter[File] = macroRW
  private val defaultMacroRW: ReadWriter[SqlDataSourceConfig] = macroRW

  private final val UJSON_TYPE_KEY = "$type"
  private final val CAPS_TYPE_KEY = "type"

  implicit val rw: ReadWriter[SqlDataSourceConfig] = readwriter[Value].bimap[SqlDataSourceConfig](
    // Rename discriminator key from ujson default, to a more friendly version
    cfg => writeJs(cfg)(defaultMacroRW).obj.collect {
      case (UJSON_TYPE_KEY, value) => CAPS_TYPE_KEY -> value
      case other => other
    },
    // Revert name change so we can use the ujson reader
    js => read[SqlDataSourceConfig](js.obj.map {
      case (CAPS_TYPE_KEY, value) => UJSON_TYPE_KEY -> value
      case other => other
    })(defaultMacroRW)
  )

  def toJson(dataSource: SqlDataSourceConfig, indent: Int = 4): String =
    write[SqlDataSourceConfig](dataSource, indent)

  def fromJson(jsonString: String): SqlDataSourceConfig =
    read[SqlDataSourceConfig](jsonString)

  def dataSourcesFromString(jsonStr: String): Map[String, SqlDataSourceConfig] =
    Try(read[Map[String, SqlDataSourceConfig]](jsonStr)) match {
      case Success(result) => result
      case Failure(ex) =>
        throw SqlDataSourceConfigException(s"Malformed SQL configuration file: ${ex.getMessage}", ex)
    }

  /** Configures a data source that reads tables via JDBC
    *
    * @param url     the JDBC URI to use when connecting to the JDBC server
    * @param driver  class name of the JDBC driver to use for the JDBC connection
    * @param options extra options passed to Spark when configuring the reader
    */
  @upickle.implicits.key("jdbc")
  case class Jdbc(
    url: String,
    driver: String,
    override val options: Map[String, String] = Map.empty
  ) extends SqlDataSourceConfig(JdbcFormat, options)

  /** Configures a data source that reads tables from Hive
    * @note The Spark session needs to be configured with `.enableHiveSupport()`
    */
  @upickle.implicits.key("hive")
  case object Hive extends SqlDataSourceConfig(HiveFormat, Map.empty)

  /** Configures a data source that reads tables from files
    *
    * @param format   the file format passed to Spark when configuring the reader
    * @param basePath the root folder used for file based formats
    * @param options  extra options passed to Spark when configuring the reader
    */
  @upickle.implicits.key("file")
  case class File(
    override val format: FileFormat,
    basePath: Option[String] = None,
    override val options: Map[String, String] = Map.empty
  ) extends SqlDataSourceConfig(format, options)

}
