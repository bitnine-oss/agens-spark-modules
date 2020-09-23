package net.bitnine.agens.cypher.impl.graph

import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherGraphFactory}

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.impl.table.SparkTable.DataFrameTable


case class CAPSGraphFactory(implicit val session: CAPSSession) extends RelationalCypherGraphFactory[DataFrameTable] {
  override type Graph = RelationalCypherGraph[DataFrameTable]
}
