package net.bitnine.agens.cypher.api.io.fs

import org.apache.hadoop.fs.Path
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.impl.util.StringEncodingUtilities._


trait GraphDirectoryStructure {

  def dataSourceRootPath: String

  def pathToGraphDirectory(graphName: GraphName): String

  def pathToGraphSchema(graphName: GraphName): String

  def pathToCAPSMetaData(graphName: GraphName): String

  def pathToNodeTable(graphName: GraphName, labels: Set[String]): String

  def pathToRelationshipTable(graphName: GraphName, relKey: String): String

}

object DefaultGraphDirectoryStructure {

  implicit class StringPath(val path: String) extends AnyVal {
    def /(segment: String): String = s"$path$pathSeparator$segment"
  }

  implicit class GraphPath(graphName: GraphName) {
    def path: String = graphName.value.replace(".", pathSeparator)
  }

  val pathSeparator: String = Path.SEPARATOR

  val schemaFileName: String = "propertyGraphSchema.json"

  val capsMetaDataFileName: String = "capsGraphMetaData.json"

  val nodeTablesDirectoryName = "nodes"

  val relationshipTablesDirectoryName = "relationships"

  // Because an empty path does not work, we need a special directory name for nodes without labels.
  val noLabelNodeDirectoryName: String = "__NO_LABEL__"

  def nodeTableDirectoryName(labels: Set[String]): String = concatDirectoryNames(labels.toSeq.sorted)

  def relKeyTableDirectoryName(relKey: String): String = relKey.encodeSpecialCharacters

  def concatDirectoryNames(seq: Seq[String]): String = {
    if (seq.isEmpty) {
      noLabelNodeDirectoryName
    } else {
      seq.mkString("_").encodeSpecialCharacters
    }
  }
}

case class DefaultGraphDirectoryStructure(dataSourceRootPath: String) extends GraphDirectoryStructure {

  import DefaultGraphDirectoryStructure._

  override def pathToGraphDirectory(graphName: GraphName): String = {
    dataSourceRootPath / graphName.path
  }

  override def pathToGraphSchema(graphName: GraphName): String = {
    pathToGraphDirectory(graphName) / schemaFileName
  }

  override def pathToCAPSMetaData(graphName: GraphName): String = {
    pathToGraphDirectory(graphName) / capsMetaDataFileName
  }

  override def pathToNodeTable(graphName: GraphName, labels: Set[String]): String = {
    pathToGraphDirectory(graphName) / nodeTablesDirectoryName / nodeTableDirectoryName(labels)
  }

  override def pathToRelationshipTable(graphName: GraphName, relKey: String): String = {
    pathToGraphDirectory(graphName) / relationshipTablesDirectoryName / relKeyTableDirectoryName(relKey)
  }

}
