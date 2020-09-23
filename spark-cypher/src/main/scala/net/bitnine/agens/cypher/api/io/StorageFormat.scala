package net.bitnine.agens.cypher.api.io

import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.util.JsonUtils.FlatOption._
import ujson._

import net.bitnine.agens.cypher.api.io.StorageFormat.nonFileFormatNames


trait StorageFormat {
  def name: String = getClass.getSimpleName.dropRight("Format$".length).toLowerCase
}

object StorageFormat {

  val nonFileFormats: Map[String, StorageFormat] = Map(
    AvroFormat.name -> AvroFormat,
    HiveFormat.name -> HiveFormat,
    JdbcFormat.name -> JdbcFormat,
    Neo4jFormat.name -> Neo4jFormat
  )

  val nonFileFormatNames: Set[String] = nonFileFormats.keySet

  private def unexpected(name: String, available: Iterable[String]) =
    throw IllegalArgumentException(s"Supported storage format (one of ${available.mkString("[", ", ", "]")})", name)

  implicit def rwStorageFormat: ReadWriter[StorageFormat] = readwriter[Value].bimap[StorageFormat](
    (storageFormat: StorageFormat) => storageFormat.name,
    (storageFormatName: Value) => {
      val formatString = storageFormatName.str
      nonFileFormats.getOrElse(formatString, FileFormat(formatString))
    }
  )

  implicit def rwFileFormat: ReadWriter[FileFormat] = readwriter[Value].bimap[FileFormat](
    (fileFormat: FileFormat) => fileFormat.name,
    (fileFormatName: Value) => FileFormat(fileFormatName.str)
  )

}

case object AvroFormat extends StorageFormat

case object Neo4jFormat extends StorageFormat

case object HiveFormat extends StorageFormat

case object JdbcFormat extends StorageFormat

object FileFormat {
  val csv: FileFormat = FileFormat("csv")
  val orc: FileFormat = FileFormat("orc")
  val parquet: FileFormat = FileFormat("parquet")
}

case class FileFormat(override val name: String) extends StorageFormat {
  assert(!nonFileFormatNames.contains(name),
    s"Cannot create a file format with a name in ${nonFileFormatNames.mkString("[", ", ", "]")} ")
}
