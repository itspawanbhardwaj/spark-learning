package com.spark.learning.chapter3

import com.spark.driver.SparkDriver

object ReadAllRecordsToDetermineSchema {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Window function", true)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val jsonDF = spark.read
      .format("json")
      .option("samplingRatio", "1.0")
      .load("src/main/resources/sales.json")

    jsonDF.show()

  }
}