package com.spark.learning.chapter3

import scala.collection.mutable.WrappedArray
import com.spark.driver.SparkDriver

object GroupByExample {
  case class RawPanda(id: Long, zip: String, pt: String, happy: Boolean, attributes: WrappedArray[Double])
  case class PandaPlace(name: String, pandas: Array[RawPanda])

  def main(args: Array[String]) {
    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 0.1))
    val damao1 = RawPanda(2, "M1B 5K7", "red", true, Array(0.1, 0.1))
    val damao2 = RawPanda(1, "M1B 5K7", "blue", true, Array(0.1, 0.1))
    val spark = SparkDriver.getSparkSession("If/else in Spark SQL")
    val df = spark.createDataFrame(Seq(damao, damao1, damao2))
    df.printSchema()

    import org.apache.spark.sql.functions._
    val pandas = df.select(
      df("id"),
      (when(df("pt") === "giant", 0)
        .when(df("pt") === "red", 1)
        .otherwise(2).as("encodedType")))

    pandas.groupBy(pandas("id")).max("encodedType").show
  }
}