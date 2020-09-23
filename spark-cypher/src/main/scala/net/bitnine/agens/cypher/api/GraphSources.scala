package net.bitnine.agens.cypher.api

import java.nio.file.Paths

import org.opencypher.graphddl.GraphDdl
import org.opencypher.okapi.api.schema.Schema

import scala.io.Source
import scala.util.Properties

import net.bitnine.agens.cypher.api.io.FileFormat
import net.bitnine.agens.cypher.api.io.fs.{CsvGraphSource, EscapeAtSymbol, FSGraphSource}
import net.bitnine.agens.cypher.api.io.sql.IdGenerationStrategy._
import net.bitnine.agens.cypher.api.io.sql.{SqlDataSourceConfig, SqlPropertyGraphDataSource}


object GraphSources {
  def fs(
    rootPath: String,
    hiveDatabaseName: Option[String] = None,
    filesPerTable: Option[Int] = Some(1)
  )(implicit session: CAPSSession) = FSGraphSources(rootPath, hiveDatabaseName, filesPerTable)

  // def cypher: CypherGraphSources.type = CypherGraphSources

  def sql(graphDdlPath: String)(implicit session: CAPSSession) = SqlGraphSources(graphDdlPath)
}

object FSGraphSources {
  def apply(
    rootPath: String,
    hiveDatabaseName: Option[String] = None,
    filesPerTable: Option[Int] = Some(1)
  )(implicit session: CAPSSession): FSGraphSourceFactory = FSGraphSourceFactory(rootPath, hiveDatabaseName, filesPerTable)

  case class FSGraphSourceFactory(
    rootPath: String,
    hiveDatabaseName: Option[String] = None,
    filesPerTable: Option[Int] = Some(1)
  )(implicit session: CAPSSession) {

    def csv: FSGraphSource = new CsvGraphSource(rootPath, filesPerTable)

    def parquet: FSGraphSource = new FSGraphSource(rootPath, FileFormat.parquet, hiveDatabaseName, filesPerTable)

    def orc: FSGraphSource = new FSGraphSource(rootPath, FileFormat.orc, hiveDatabaseName, filesPerTable) with EscapeAtSymbol
  }
}

object SqlGraphSources {

  case class SqlGraphSourceFactory(graphDdl: GraphDdl, idGenerationStrategy: IdGenerationStrategy)
    (implicit session: CAPSSession) {

    def withIdGenerationStrategy(idGenerationStrategy: IdGenerationStrategy): SqlGraphSourceFactory =
      copy(idGenerationStrategy = idGenerationStrategy)

    def withSqlDataSourceConfigs(sqlDataSourceConfigsPath: String): SqlPropertyGraphDataSource = {
      val jsonString = Source.fromFile(sqlDataSourceConfigsPath, "UTF-8").getLines().mkString(Properties.lineSeparator)
      val sqlDataSourceConfigs = SqlDataSourceConfig.dataSourcesFromString(jsonString)
      withSqlDataSourceConfigs(sqlDataSourceConfigs)
    }

    def withSqlDataSourceConfigs(sqlDataSourceConfigs: (String, SqlDataSourceConfig)*): SqlPropertyGraphDataSource =
      withSqlDataSourceConfigs(sqlDataSourceConfigs.toMap)

    def withSqlDataSourceConfigs(sqlDataSourceConfigs: Map[String, SqlDataSourceConfig]): SqlPropertyGraphDataSource =
      SqlPropertyGraphDataSource(graphDdl, sqlDataSourceConfigs, idGenerationStrategy)
  }

  def apply(graphDdlPath: String)(implicit session: CAPSSession): SqlGraphSourceFactory =
    SqlGraphSourceFactory(
      graphDdl = GraphDdl(Source.fromFile(graphDdlPath, "UTF-8").mkString),
      idGenerationStrategy = SerializedId)
}
