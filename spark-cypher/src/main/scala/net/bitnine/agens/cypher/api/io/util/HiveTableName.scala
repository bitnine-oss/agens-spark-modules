package net.bitnine.agens.cypher.api.io.util

import org.opencypher.okapi.api.graph.{GraphEntityType, GraphName}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

import net.bitnine.agens.cypher.api.io.fs.DefaultGraphDirectoryStructure._


case object HiveTableName {

  def apply(databaseName: String,
    graphName: GraphName,
    entityType: GraphEntityType,
    entityIdentifiers: Set[String]): String = {

    val entityString = entityType.name.toLowerCase
            // rename : node -> vertex, relationship => edge
            match {
              case "node" => "vertex"
              case "relationship" => "edge"
            }

    val tableName = s"${graphName.path.replace('/', '_')}_${entityString}_${entityIdentifiers.toSeq.sorted.mkString("_")}".encodeSpecialCharacters
    s"$databaseName.$tableName"
  }

}
