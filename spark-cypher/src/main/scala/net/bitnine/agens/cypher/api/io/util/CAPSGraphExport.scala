package net.bitnine.agens.cypher.api.io.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.ir.api.expr.{Property, Var}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph

import net.bitnine.agens.cypher.api.io.{GraphEntity, Relationship}
import net.bitnine.agens.cypher.impl.convert.SparkConversions._
import net.bitnine.agens.cypher.impl.table.SparkTable.DataFrameTable


// TODO: Add documentation that describes the canonical table format
object CAPSGraphExport {

  implicit class CanonicalTableSparkSchema(val schema: Schema) extends AnyVal {

    def canonicalNodeStructType(labels: Set[String]): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, BinaryType, nullable = false)
      val properties = schema.nodePropertyKeys(labels).toSeq
        .map { case (propertyName, cypherType) => propertyName.toPropertyColumnName -> cypherType }
        .sortBy { case (propertyColumnName, _) => propertyColumnName }
        .map { case (propertyColumnName, cypherType) =>
          StructField(propertyColumnName, cypherType.getSparkType, cypherType.isNullable)
        }
      StructType(id +: properties)
    }

    def canonicalRelStructType(relType: String): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, BinaryType, nullable = false)
      val sourceId = StructField(Relationship.sourceStartNodeKey, BinaryType, nullable = false)
      val targetId = StructField(Relationship.sourceEndNodeKey, BinaryType, nullable = false)
      val properties = schema.relationshipPropertyKeys(relType).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        StructField(propertyName.toPropertyColumnName, cypherType.getSparkType, cypherType.isNullable)
      }
      StructType(id +: sourceId +: targetId +: properties)
    }
  }

  implicit class CanonicalTableExport(graph: RelationalCypherGraph[DataFrameTable]) {

    def canonicalNodeTable(labels: Set[String]): DataFrame = {
      val ct = CTNode(labels)
      val v = Var("n")(ct)
      val nodeRecords = graph.nodes(v.name, ct, exactLabelMatch = true)
      val header = nodeRecords.header

      val idRenaming = header.column(v) -> GraphEntity.sourceIdKey
      val properties: Set[Property] = header.propertiesFor(v)

      val propertyRenamings = properties.map { p =>
        header.column(p) -> p.key.name.toLowerCase
      }  // .toPropertyColumnName }     // ex) col[A] -> col[property_A]
      val selectColumns = (idRenaming :: propertyRenamings.toList.sortBy(_._2)).map {
        case (oldName, newName) => nodeRecords.table.df.col(oldName).as(newName)
      }

      nodeRecords.table.df.select(selectColumns: _*)
    }

    def canonicalRelationshipTable(relType: String): DataFrame = {
      val ct = CTRelationship(relType)
      val v = Var("r")(ct)
      val relRecords = graph.relationships(v.name, ct)
      val header = relRecords.header

      val idRenaming = header.column(v) -> GraphEntity.sourceIdKey
      val sourceIdRenaming = header.column(header.startNodeFor(v)) -> Relationship.sourceStartNodeKey
      val targetIdRenaming = header.column(header.endNodeFor(v)) -> Relationship.sourceEndNodeKey
      val properties: Set[Property] = relRecords.header.propertiesFor(v)

      val propertyRenamings = properties.map { p =>
        relRecords.header.column(p) -> p.key.name.toLowerCase
      }   // toPropertyColumnName }     // ex) col[A] -> col[property_A]
      val selectColumns = (idRenaming :: sourceIdRenaming :: targetIdRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => relRecords.table.df.col(oldName).as(newName)
      }

      relRecords.table.df.select(selectColumns: _*)
    }

  }

}
