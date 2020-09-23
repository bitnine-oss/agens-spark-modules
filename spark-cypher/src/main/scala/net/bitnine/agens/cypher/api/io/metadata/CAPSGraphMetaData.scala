package net.bitnine.agens.cypher.api.io.metadata

import upickle.default._

import net.bitnine.agens.cypher.api.io.metadata.CAPSGraphMetaData._


object CAPSGraphMetaData {
  implicit def rw: ReadWriter[CAPSGraphMetaData] = macroRW

  def fromJson(jsonString: String): CAPSGraphMetaData =
    upickle.default.read[CAPSGraphMetaData](jsonString)
}

case class CAPSGraphMetaData(tableStorageFormat: String) {

  def toJson: String =
    upickle.default.write[CAPSGraphMetaData](this, indent = 4)
}
