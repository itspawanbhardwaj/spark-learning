package com.spark.learning.chapter3

import com.spark.driver.SparkDriver
import org.apache.spark.sql.SaveMode
import java.sql.Date

object DatasetExample {
  case class Customers(name: String, date: Date, amountSpent: Double)
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Window function", true)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.sparkContext.parallelize(List(
      ("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).
      toDF("name", "date", "amountSpent")

    val ds = df.as[Customers]
    ds.show
  }

}