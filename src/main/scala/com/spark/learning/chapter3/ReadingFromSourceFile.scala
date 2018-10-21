package com.spark.learning.chapter3

import com.spark.driver.SparkDriver

object ReadingFromSourceFile {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Window function", true)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    spark.sql("select count(*) from parquet.`src/main/resources/data/customers`").show
  }
}