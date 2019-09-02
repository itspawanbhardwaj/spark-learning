package com.spark.learning.chapter3

import com.spark.driver.SparkDriver
import org.apache.spark.sql.SaveMode

object SaveToParquet {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Spark App", true)

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

    df.write.mode(SaveMode.Ignore).parquet("src/main/resources/data/customers")
    //df.write.format("parquet").save(path)
  }
}