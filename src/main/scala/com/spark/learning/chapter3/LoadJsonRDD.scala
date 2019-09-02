package com.spark.learning.chapter3

import com.spark.driver.SparkDriver

object LoadJsonRDD {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Spark App", true)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val rdd = spark.sparkContext.textFile("src/main/resources/sales.json")

    val df = spark.read.json(rdd)

    df.show()
  }
}