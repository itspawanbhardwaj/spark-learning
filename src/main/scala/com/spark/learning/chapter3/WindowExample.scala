package com.spark.learning.chapter3

import com.spark.driver.SparkDriver
import org.apache.spark.sql.expressions.Window

object WindowExample {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Window function", true)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val customers = spark.sparkContext.parallelize(List(
      ("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).
      toDF("name", "date", "amountSpent")

    val windowSpec = Window.orderBy("date")
      .partitionBy("name")
      .rowsBetween(start = -1, end = 1)

    /*In this window spec, the data is partitioned by customer.
     * Each customer’s data is ordered by date.
     * And, the window frame is defined as starting from -1
     * (one row before the current row) and ending at 1
     * (one row after the current row), for a total of 3 rows in the sliding window.
       */

    customers.withColumn(
      "movingAvg",
      avg(customers("amountSpent")).over(windowSpec)).show //.orderBy("name", "date").show()

    // customers.select($"name", $"amountSpent", $"date", avg("amountSpent").over(windowSpec).as("avg_spent")).show
  }
}