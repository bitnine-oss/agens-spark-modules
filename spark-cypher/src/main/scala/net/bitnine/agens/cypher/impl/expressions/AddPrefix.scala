package net.bitnine.agens.cypher.impl.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, NullIntolerant}
import org.apache.spark.sql.types.{BinaryType, ByteType, DataType}


/**
  * Spark expression that adds a byte prefix to an array of bytes.
  *
  * @param left expression of ByteType that is added as a prefix
  * @param right expression of BinaryType to which a prefix is added
  */
case class AddPrefix(
  left: Expression, // byte prefix
  right: Expression // binary array
) extends BinaryExpression with NullIntolerant with ExpectsInputTypes {

  override val dataType: DataType = BinaryType

  override val inputTypes: Seq[DataType] = Seq(ByteType, BinaryType)

  override protected def nullSafeEval(byte: Any, byteArray: Any): Any = {
    AddPrefix.addPrefix(byte.asInstanceOf[Byte], byteArray.asInstanceOf[Array[Byte]])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (a, p) => s"(byte[])(${AddPrefix.getClass.getName.dropRight(1)}.addPrefix($a, $p))")
}

object AddPrefix {

  @inline
  final def addPrefix(p: Byte, a: Array[Byte]): Array[Byte] = {
    val n = new Array[Byte](a.length + 1)
    n(0) = p
    System.arraycopy(a, 0, n, 1, a.length)
    n
  }

  implicit class ColumnPrefixOps(val c: Column) extends AnyVal {

    def addPrefix(prefix: Column): Column = new Column(AddPrefix(prefix.expr, c.expr))

  }

}
