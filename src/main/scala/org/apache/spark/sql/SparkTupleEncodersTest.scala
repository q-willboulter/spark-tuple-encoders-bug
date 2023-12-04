package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.ObjectType
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

object SparkTupleEncodersTest extends App {
  val spark = SparkSession.builder().master("local[1]").getOrCreate()
  import spark.implicits._

  // ======= Simple Example =======

  val simpleDataframe = Seq((1, Some(1)), (2, None)).toDF

  val spark332Simple =
    CastDataframe.spark332SimpleData(simpleDataframe).collect()

  val spark333Simple =
    CastDataframe.spark333SimpleData(simpleDataframe).collect()
  println("=== SIMPLE EXAMPLE 3.3.2")
  spark332Simple.foreach(println)
  println("=== SIMPLE EXAMPLE 3.3.3")
  spark333Simple.foreach(println)

  // ======= End Simple Example =======

  // ======= Left Join Example =======
  case class Left(key: Int, b: String)
  case class Right(key: Int, c: List[String])
  val leftDs = Seq(Left(1, "1"), Left(2, "2")).toDS
  val rightDs = Seq(Right(1, List("1"))).toDS

  val joinedDf =
    leftDs.join(rightDs, Seq("key"), joinType = "LEFT").select("key", "b", "c")

  val spark332Join = CastDataframe.spark332JoinData(joinedDf).collect
  val spark333Join = CastDataframe.spark333JoinData(joinedDf).collect

  println("=== LEFT JOIN EXAMPLE 3.3.2")
  spark332Join.foreach(println)
  println("=== LEFT JOIN EXAMPLE 3.3.3")
  spark333Join.foreach(println)
  // ======= End Left Join Example =======
}

object CastDataframe {

  def spark332SimpleData(data: DataFrame): Dataset[(Int, Option[Int])] = {
    val goodEncoder = ExpressionEncoder332
      .tuple(
        Seq(ExpressionEncoder.apply[Int], ExpressionEncoder.apply[Option[Int]])
      )
      .asInstanceOf[ExpressionEncoder[(Int, Option[Int])]]
    data.as[(Int, Option[Int])](goodEncoder)
  }

  def spark333SimpleData(data: DataFrame): Dataset[(Int, Option[Int])] = {
    val badEncoder = ExpressionEncoder.tuple(
      ExpressionEncoder.apply[Int],
      ExpressionEncoder.apply[Option[Int]]
    )
    data.as[(Int, Option[Int])](badEncoder)
  }

  def spark332JoinData(
      data: DataFrame
  ): Dataset[(Int, String, Option[List[String]])] = {
    val encoder1 = ExpressionEncoder.apply[Int]
    val encoder2 = ExpressionEncoder.apply[String]
    val encoder3 = ExpressionEncoder.apply[Option[List[String]]]
    val encoder332 =
      ExpressionEncoder332
        .tuple(Seq(encoder1, encoder2, encoder3))
        .asInstanceOf[ExpressionEncoder[(Int, String, Option[List[String]])]]

    data.as[(Int, String, Option[List[String]])](encoder332)
  }

  def spark333JoinData(
      data: DataFrame
  ): Dataset[(Int, String, Option[List[String]])] = {
    val encoder1 = ExpressionEncoder.apply[Int]
    val encoder2 = ExpressionEncoder.apply[String]
    val encoder3 = ExpressionEncoder.apply[Option[List[String]]]
    val encoder333 = ExpressionEncoder.tuple(encoder1, encoder2, encoder3)

    data.as[(Int, String, Option[List[String]])](encoder333)
  }
}

// This function was taken directly from the source code for Spark 3.3.2 before breaking commit:
//  https://github.com/apache/spark/commit/9110c05d54c392e55693eba4509be37c571d610a
object ExpressionEncoder332 {

  /** Given key set of N encoders, constructs key new encoder that produce objects as items in an N-tuple. Note that
    * these encoders should be unresolved so that information about name/positional binding is preserved.
    */
  def tuple(encoders: Seq[ExpressionEncoder[_]]): ExpressionEncoder[_] = {
    if (encoders.length > 22) {
      throw QueryExecutionErrors.elementsOfTupleExceedLimitError()
    }

    encoders.foreach(_.assertUnresolved())

    val cls = Utils.getContextOrSparkClassLoader.loadClass(
      s"scala.Tuple${encoders.size}"
    )

    val newSerializerInput = BoundReference(0, ObjectType(cls), nullable = true)
    val serializers = encoders.zipWithIndex.map { case (enc, index) =>
      val boundRefs = enc.objSerializer.collect { case b: BoundReference =>
        b
      }.distinct
      assert(
        boundRefs.size == 1,
        "object serializer should have only one bound reference but " +
          s"there are ${boundRefs.size}"
      )

      val originalInputObject = boundRefs.head
      val newInputObject = Invoke(
        newSerializerInput,
        s"_${index + 1}",
        originalInputObject.dataType,
        returnNullable = originalInputObject.nullable
      )

      val newSerializer = enc.objSerializer.transformUp { case BoundReference(0, _, _) =>
        newInputObject
      }

      Alias(newSerializer, s"_${index + 1}")()
    }
    val newSerializer = CreateStruct(serializers)

    val newDeserializerInput = GetColumnByOrdinal(0, newSerializer.dataType)
    val deserializers = encoders.zipWithIndex.map { case (enc, index) =>
      val getColExprs = enc.objDeserializer.collect { case c: GetColumnByOrdinal =>
        c
      }.distinct
      assert(
        getColExprs.size == 1,
        "object deserializer should have only one " +
          s"`GetColumnByOrdinal`, but there are ${getColExprs.size}"
      )

      val input = GetStructField(newDeserializerInput, index)
      enc.objDeserializer.transformUp { case GetColumnByOrdinal(0, _) =>
        input
      }
    }
    val newDeserializer =
      NewInstance(cls, deserializers, ObjectType(cls), propagateNull = false)

    def nullSafe(input: Expression, result: Expression): Expression =
      If(IsNull(input), Literal.create(null, result.dataType), result)

    new ExpressionEncoder[Any](
      nullSafe(newSerializerInput, newSerializer),
      nullSafe(newDeserializerInput, newDeserializer),
      ClassTag(cls)
    )
  }

}
