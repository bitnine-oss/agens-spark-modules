package net.bitnine.agens.cypher.impl

import org.opencypher.okapi.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult

import scala.util.{Failure, Success, Try}

import net.bitnine.agens.cypher.api.CAPSSession
import net.bitnine.agens.cypher.impl.table.SparkTable.DataFrameTable


object CAPSConverters {

  private def unsupported(expected: String, got: Any): Nothing =
    throw UnsupportedOperationException(s"Can only handle $expected, got $got")

  implicit class RichSession(val session: CypherSession) extends AnyVal {
    def asCaps: CAPSSession = session match {
      case caps: CAPSSession => caps
      case other => unsupported("CAPS session", other)
    }
  }

  implicit class RichPropertyGraph(val graph: PropertyGraph) extends AnyVal {


    def asCaps: RelationalCypherGraph[DataFrameTable] = graph.asInstanceOf[RelationalCypherGraph[_]] match {
      case caps: RelationalCypherGraph[_] =>
        Try {
          caps.asInstanceOf[RelationalCypherGraph[DataFrameTable]]
        } match {
          case Success(value) => value
          case Failure(_) => unsupported("CAPS graphs", caps)
        }
      case other => unsupported("CAPS graphs", other)
    }
  }

  implicit class RichCypherRecords(val records: CypherRecords) extends AnyVal {
    def asCaps: CAPSRecords = records match {
      case caps: CAPSRecords => caps
      case other => unsupported("CAPS records", other)
    }
  }

  implicit class RichCypherResult(val records: CypherResult) extends AnyVal {
    def asCaps(implicit caps: CAPSSession): RelationalCypherResult[DataFrameTable] = records match {
      case relational: RelationalCypherResult[_] =>
        Try {
          relational.asInstanceOf[RelationalCypherResult[DataFrameTable]]
        } match {
          case Success(value) => value
          case Failure(_) => unsupported("CAPS results", caps)
        }
      case other => unsupported("CAPS results", other)
    }
  }

}
