package net.bitnine.agens.cypher.impl.expressions

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.opencypher.okapi.impl.exception
import net.bitnine.agens.cypher.impl.expressions.EncodeLong.encodeLong
import net.bitnine.agens.cypher.impl.expressions.Serialize._


/**
  * Encodes all children to a byte array (BinaryType).
  *
  * For each child, writes the length of the serialized child followed by the actual serialized child.
  */
case class Serialize(children: Seq[Expression]) extends Expression {

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  // TODO: Only write length if more than one column is serialized
  override def eval(input: InternalRow): Any = {
    // TODO: Reuse from a pool instead of allocating a new one for each serialization
    val out = new ByteArrayOutputStream()
    children.foreach { child =>
      child.dataType match {
        case BinaryType => write(child.eval(input).asInstanceOf[Array[Byte]], out)
        case StringType => write(child.eval(input).asInstanceOf[UTF8String], out)
        case IntegerType => write(child.eval(input).asInstanceOf[Int], out)
        case LongType => write(child.eval(input).asInstanceOf[Long], out)
        case other => throw exception.UnsupportedOperationException(s"Cannot serialize Spark data type $other.")
      }
    }
    out.toByteArray
  }

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode
  ): ExprCode = {
    ev.isNull = FalseLiteral      // more than spark 2.4.0 (<= "false")
    val out = ctx.freshName("out")
    val serializeChildren = children.map { child =>
      val childEval = child.genCode(ctx)
      s"""|${childEval.code}
          |if (!${childEval.isNull}) {
          |  ${Serialize.getClass.getName.dropRight(1)}.write(${childEval.value}, $out);
          |}""".stripMargin
    }.mkString("\n")
    val baos = classOf[ByteArrayOutputStream].getName
    ev.copy( code=                // more than spark 2.4.0 (<= Multi-lines)
      code"""|$baos $out = new $baos();
          |$serializeChildren
          |byte[] ${ev.value} = $out.toByteArray();""".stripMargin)
  }

}

object Serialize {

  val supportedTypes: Set[DataType] = Set(BinaryType, StringType, IntegerType, LongType)

  @inline final def write(value: Array[Byte], out: ByteArrayOutputStream): Unit = {
    out.write(encodeLong(value.length))
    out.write(value)
  }

  @inline final def write(
    value: Boolean,
    out: ByteArrayOutputStream
  ): Unit = write(if (value) 1.toLong else 0.toLong, out)

  @inline final def write(value: Byte, out: ByteArrayOutputStream): Unit = write(value.toLong, out)

  @inline final def write(value: Int, out: ByteArrayOutputStream): Unit = write(value.toLong, out)

  @inline final def write(value: Long, out: ByteArrayOutputStream): Unit = write(encodeLong(value), out)

  @inline final def write(value: UTF8String, out: ByteArrayOutputStream): Unit = write(value.getBytes, out)

  @inline final def write(value: String, out: ByteArrayOutputStream): Unit = write(value.getBytes, out)

}
