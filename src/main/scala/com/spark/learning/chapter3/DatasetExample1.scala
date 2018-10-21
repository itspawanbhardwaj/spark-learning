package com.spark.learning.chapter3

import com.spark.driver.SparkDriver

object DatasetExample1 {
  case class RawPanda(id: Long, zip: String, pt: String, happy: Boolean, attributes: Array[Double])
  case class PandaPlace(name: String, pandas: Array[RawPanda])

  def main(args: Array[String]) {
    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 0.1))
    val pandaPlace = PandaPlace("toronto", Array(damao))
    val spark = SparkDriver.getSparkSession("Create a Dataset with the case class")
    val df = spark.createDataFrame(Seq(pandaPlace))
    df.printSchema()
  }
}