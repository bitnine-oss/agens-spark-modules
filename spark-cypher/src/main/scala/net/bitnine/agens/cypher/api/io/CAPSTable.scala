package net.bitnine.agens.cypher.api.io

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory

import scala.reflect.runtime.universe._

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.impl.table.SparkTable
import net.bitnine.agens.cypher.impl.table.SparkTable.{DataFrameTable, _}
import net.bitnine.agens.cypher.impl.util.Annotation
import net.bitnine.agens.cypher.impl.{CAPSRecords, RecordBehaviour}


case object CAPSEntityTableFactory extends RelationalEntityTableFactory[DataFrameTable] {
  override def entityTable(
    nodeMapping: EntityMapping,
    table: DataFrameTable
  ): EntityTable[DataFrameTable] = {
    CAPSEntityTable.create(nodeMapping, table)
  }
}

case class CAPSEntityTable private[agens](
  override val mapping: EntityMapping,
  override val table: DataFrameTable
) extends EntityTable[DataFrameTable] with RecordBehaviour {

  override type Records = CAPSEntityTable

  private[agens] def records(implicit caps: CAPSSession): CAPSRecords = caps.records.fromEntityTable(entityTable = this)

  override def cache(): CAPSEntityTable = {
    table.cache()
    this
  }
}

object CAPSEntityTable {
  def create(mapping: EntityMapping, table: DataFrameTable): CAPSEntityTable = {
    val sourceIdColumns = mapping.idKeys.values.toSeq.flatten.map(_._2)
    val idCols = table.df.encodeIdColumns(sourceIdColumns: _*)
    val remainingCols = mapping.allSourceKeys.filterNot(sourceIdColumns.contains).map(table.df.col)
    val colsToSelect = idCols ++ remainingCols

    CAPSEntityTable(mapping, table.df.select(colsToSelect: _*))
  }
}

object CAPSNodeTable {

  def apply[E <: Node : TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): CAPSEntityTable = {
    val nodeLabels = Annotation.labels[E]
    val nodeDF = caps.sparkSession.createDataFrame(nodes)
    val nodeProperties = nodeDF.columns.filter(_ != GraphEntity.sourceIdKey).toSet
    val nodeMapping = NodeMappingBuilder.create(nodeIdKey = GraphEntity.sourceIdKey, impliedLabels = nodeLabels, propertyKeys = nodeProperties)
    CAPSEntityTable.create(nodeMapping, nodeDF)
  }

  /**
    * Creates a node table from the given [[DataFrame]]. By convention, there needs to be one column storing node
    * identifiers and named after [[GraphEntity.sourceIdKey]]. All remaining columns are interpreted as node property columns, the column name is used as property
    * key.
    *
    * @param impliedLabels  implied node labels
    * @param nodeDF         node data
    * @return a node table with inferred node mapping
    */
  def apply(impliedLabels: Set[String], nodeDF: DataFrame): CAPSEntityTable = {
    val propertyColumnNames = nodeDF.columns.filter(_ != GraphEntity.sourceIdKey).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = NodeMappingBuilder
      .on(GraphEntity.sourceIdKey)
      .withImpliedLabels(impliedLabels.toSeq: _*)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    CAPSEntityTable.create(mapping, nodeDF)
  }
}

object CAPSRelationshipTable {

  def apply[E <: Relationship : TypeTag](relationships: Seq[E])(implicit caps: CAPSSession): CAPSEntityTable = {
    val relationshipType: String = Annotation.relType[E]
    val relationshipDF = caps.sparkSession.createDataFrame(relationships)
    val relationshipProperties = relationshipDF.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet

    val relationshipMapping = RelationshipMappingBuilder.create(GraphEntity.sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType,
      relationshipProperties)

    CAPSEntityTable.create(relationshipMapping, relationshipDF)
  }

  /**
    * Creates a relationship table from the given [[DataFrame]]. By convention, there needs to be one column storing
    * relationship identifiers and named after [[GraphEntity.sourceIdKey]], one column storing source node identifiers
    * and named after [[Relationship.sourceStartNodeKey]] and one column storing target node identifiers and named after
    * [[Relationship.sourceEndNodeKey]]. All remaining columns are interpreted as relationship property columns, the
    * column name is used as property key.
    *
    * Column names prefixed with `property#` are decoded by [[org.opencypher.okapi.impl.util.StringEncodingUtilities]] to
    * recover the original property name.
    *
    * @param relationshipType relationship type
    * @param relationshipDF   relationship data
    * @return a relationship table with inferred relationship mapping
    */
  def apply(relationshipType: String, relationshipDF: DataFrame): CAPSEntityTable = {
    val propertyColumnNames = relationshipDF.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = RelationshipMappingBuilder
      .on(GraphEntity.sourceIdKey)
      .from(Relationship.sourceStartNodeKey)
      .to(Relationship.sourceEndNodeKey)
      .withRelType(relationshipType)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    CAPSEntityTable.create(mapping, relationshipDF)
  }
}


