package net.bitnine.agens.cypher.impl

import java.util.Collections

import org.apache.spark.sql._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory}
import org.opencypher.okapi.relational.impl.table._

import scala.collection.JavaConverters._

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.impl.convert.SparkConversions._
import net.bitnine.agens.cypher.impl.convert.rowToCypherMap
import net.bitnine.agens.cypher.impl.table.SparkTable._


case class CAPSRecordsFactory(implicit caps: CAPSSession) extends RelationalCypherRecordsFactory[DataFrameTable] {

  override type Records = CAPSRecords

  override def unit(): CAPSRecords = {
    val initialDataFrame = caps.sparkSession.createDataFrame(Seq(EmptyRow()))
    CAPSRecords(RecordHeader.empty, initialDataFrame)
  }

  override def empty(initialHeader: RecordHeader = RecordHeader.empty): CAPSRecords = {
    val initialSparkStructType = initialHeader.toStructType
    val initialDataFrame = caps.sparkSession.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    CAPSRecords(initialHeader, initialDataFrame)
  }

  override def fromEntityTable(entityTable: EntityTable[DataFrameTable]): CAPSRecords = {
    val withCypherCompatibleTypes = entityTable.table.df.withCypherCompatibleTypes
    CAPSRecords(entityTable.header, withCypherCompatibleTypes)
  }

  override def from(
    header: RecordHeader,
    table: DataFrameTable,
    maybeDisplayNames: Option[Seq[String]]
  ): CAPSRecords = {
    val displayNames = maybeDisplayNames match {
      case s@Some(_) => s
      case None => Some(header.vars.map(_.withoutType).toSeq)
    }
    CAPSRecords(header, table, displayNames)
  }

  /**
    * Wraps a Spark SQL table (DataFrame) in a CAPSRecords, making it understandable by Cypher.
    *
    * @param df   table to wrap.
    * @param caps session to which the resulting CAPSRecords is tied.
    * @return a Cypher table.
    */
  private[agens] def wrap(df: DataFrame)(implicit caps: CAPSSession): CAPSRecords = {
    val compatibleDf = df.withCypherCompatibleTypes
    CAPSRecords(compatibleDf.schema.toRecordHeader, compatibleDf)
  }

  private case class EmptyRow()
}

case class CAPSRecords(
  header: RecordHeader,
  table: DataFrameTable,
  override val logicalColumns: Option[Seq[String]] = None
)(implicit session: CAPSSession) extends RelationalCypherRecords[DataFrameTable] with RecordBehaviour {
  override type Records = CAPSRecords

  def df: DataFrame = table.df

  override def cache(): CAPSRecords = {
    df.cache()
    this
  }

  override def toString: String = {
    if (header.isEmpty) {
      s"CAPSRecords.empty"
    } else {
      s"CAPSRecords(header: $header)"
    }
  }
}

trait RecordBehaviour extends RelationalCypherRecords[DataFrameTable] {

  override lazy val columnType: Map[String, CypherType] = table.df.columnType

  override def rows: Iterator[String => CypherValue] = {
    toLocalIterator.asScala.map(_.value)
  }

  override def iterator: Iterator[CypherMap] = {
    toLocalIterator.asScala
  }

  def toLocalIterator: java.util.Iterator[CypherMap] = {
    toCypherMaps.toLocalIterator()
  }

  def foreachPartition(f: Iterator[CypherMap] => Unit): Unit = {
    toCypherMaps.foreachPartition(f)
  }

  override def collect: Array[CypherMap] =
    toCypherMaps.collect()


  def toCypherMaps: Dataset[CypherMap] = {
    import encoders._
    table.df.map(rowToCypherMap(header.exprToColumn.toSeq))
  }
}
