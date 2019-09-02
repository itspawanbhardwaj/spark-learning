package com.spark.learning.chapter2

import com.spark.driver.SparkDriver

object StageBoundariesExample {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Different types of transformations showing stage boundarie")

    val rdd = spark.sparkContext.parallelize(Seq(100, 355, 562, 356, 787, 634, 765, 926), 3)

    //stage 1
    val count = rdd.filter(_ < 500)
      .map(x => (x, x))
      // stage 2
      .groupByKey()
      .map {
        case (value, group) => (group.sum, value)
      }
      //stage 3
      .sortByKey().count

    println(count)
  }
}