package net.bitnine.agens.cypher.api.io.json

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema

import net.bitnine.agens.cypher.api.io.AbstractPropertyGraphDataSource
import net.bitnine.agens.cypher.api.io.metadata.CAPSGraphMetaData
import net.bitnine.agens.cypher.schema.CAPSSchema


trait JsonSerialization {
  self: AbstractPropertyGraphDataSource =>

  import CAPSSchema._

  protected def readJsonSchema(graphName: GraphName): String

  protected def writeJsonSchema(graphName: GraphName, schema: String): Unit

  protected def readJsonCAPSGraphMetaData(graphName: GraphName): String

  protected def writeJsonCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: String): Unit

  override protected def readSchema(graphName: GraphName): CAPSSchema = {
    Schema.fromJson(readJsonSchema(graphName)).asCaps
  }

  override protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit = {
    writeJsonSchema(graphName, schema.schema.toJson)
  }

  override protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData = {
    CAPSGraphMetaData.fromJson(readJsonCAPSGraphMetaData(graphName))
  }

  override protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit = {
    writeJsonCAPSGraphMetaData(graphName, capsGraphMetaData.toJson)
  }

}
