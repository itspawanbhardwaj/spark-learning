package com.spark.two.updates

import com.spark.driver.SparkDriver
import org.apache.spark.sql.Encoders

object EncodersExample {

  /*  Encoder is the fundamental concept in the serialization and deserialization (SerDe) framework in Spark SQL 2.0.
 *  Spark SQL uses the SerDe framework for IO to make it efficient time- and space-wise.
*/

  /*Encoder is also called "a container of serde expressions in Dataset".*/

  /*Encoders know the schema of the records. 
  This is how they offer significantly faster serialization and deserialization 
  (comparing to the default Java or Kryo serializers).*/

  case class Person(id: Long, name: String)

  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("Encoder example")
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val personEncoder = Encoders.product[Person]
    println(personEncoder.schema)

    println(personEncoder.clsTag)

    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

    val personExprEncoder = personEncoder.asInstanceOf[ExpressionEncoder[Person]]

    // ExpressionEncoders may or may not be flat
    println(personExprEncoder.flat)

    // The Serializer part of the encoder
    println(personExprEncoder.serializer)

    // The Deserializer part of the encoder
    println(personExprEncoder.deserializer)

    println(personExprEncoder.namedExpressions)

    val jacek = Person(0, "Jacek")

    // Serialize a record to the internal representation, i.e. InternalRow
    val row = personExprEncoder.toRow(jacek)

    // Spark uses InternalRows internally for IO
    // Let's deserialize it to a JVM object, i.e. a Scala object
    import org.apache.spark.sql.catalyst.dsl.expressions._

    val attrs = Seq(DslSymbol('id).long, DslSymbol('name).string)

    val jacekReborn = personExprEncoder.resolveAndBind(attrs).fromRow(row)

    // Are the jacek instances same?
    println(jacek == jacekReborn)
  }
}