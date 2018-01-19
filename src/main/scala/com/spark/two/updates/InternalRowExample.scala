package com.spark.two.updates

import com.spark.driver.SparkDriver

object InternalRowExample {
  case class Person(id: Long, name: String)

  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("Encoder example")
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    import org.apache.spark.sql.Encoders
    val personEncoder = Encoders.product[Person]

    // The expression encoder for Person objects
    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    val personExprEncoder = personEncoder.asInstanceOf[ExpressionEncoder[Person]]

    // Convert Person objects to InternalRow
    val row = personExprEncoder.toRow(Person(0, "Jacek"))

    println(row.numFields)

    println(row.anyNull)

    // You can create your own InternalRow objects
    import org.apache.spark.sql.catalyst.InternalRow

    val ir = InternalRow(5, "hello", (0, "nice"))
    println(ir)
  }
}