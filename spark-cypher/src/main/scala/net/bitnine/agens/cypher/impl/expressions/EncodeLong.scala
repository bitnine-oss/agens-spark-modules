package net.bitnine.agens.cypher.impl.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, LongType}

import net.bitnine.agens.cypher.api.value.CAPSEntity._


/**
  * Spark expression that encodes a long into a byte array using variable-length encoding.
  *
  * @param child expression of type LongType that is encoded by this expression
  */
case class EncodeLong(child: Expression) extends UnaryExpression with NullIntolerant with ExpectsInputTypes {

  override val dataType: DataType = BinaryType

  override val inputTypes: Seq[LongType] = Seq(LongType)

  override protected def nullSafeEval(input: Any): Any =
    EncodeLong.encodeLong(input.asInstanceOf[Long])

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => s"(byte[])(${EncodeLong.getClass.getName.dropRight(1)}.encodeLong($c))")
}

object EncodeLong {

  private final val moreBytesBitMask: Long = Integer.parseInt("10000000", 2)
  private final val varLength7BitMask: Long = Integer.parseInt("01111111", 2)
  private final val otherBitsMask = ~varLength7BitMask
  private final val maxBytesForLongVarEncoding = 10

  // Same encoding as as Base 128 Varints @ https://developers.google.com/protocol-buffers/docs/encoding
  @inline
  final def encodeLong(l: Long): Array[Byte] = {
    val tempResult = new Array[Byte](maxBytesForLongVarEncoding)

    var remainder = l
    var index = 0

    while ((remainder & otherBitsMask) != 0) {
      tempResult(index) = ((remainder & varLength7BitMask) | moreBytesBitMask).toByte
      remainder >>>= 7
      index += 1
    }
    tempResult(index) = remainder.toByte

    val result = new Array[Byte](index + 1)
    System.arraycopy(tempResult, 0, result, 0, index + 1)
    result
  }

  // Same encoding as as Base 128 Varints @ https://developers.google.com/protocol-buffers/docs/encoding
  @inline
  final def decodeLong(input: Array[Byte]): Long = {
    assert(input.nonEmpty, "`decodeLong` requires a non-empty array as its input")
    var index = 0
    var currentByte = input(index)
    var decoded = currentByte & varLength7BitMask
    var nextLeftShift = 7

    while ((currentByte & moreBytesBitMask) != 0) {
      index += 1
      currentByte = input(index)
      decoded |= (currentByte & varLength7BitMask) << nextLeftShift
      nextLeftShift += 7
    }
    assert(index == input.length - 1,
      s"`decodeLong` received an input array ${input.toSeq.toHex} with extra bytes that could not be decoded.")
    decoded
  }

  implicit class ColumnLongOps(val c: Column) extends AnyVal {

    def encodeLongAsCAPSId(name: String): Column = encodeLongAsCAPSId.as(name)

    def encodeLongAsCAPSId: Column = new Column(EncodeLong(c.expr))

  }

}
