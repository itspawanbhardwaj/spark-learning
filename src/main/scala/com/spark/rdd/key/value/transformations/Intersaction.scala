package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object Intersaction {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;

    val parallel = sparkContext.parallelize(1 to 9)
    val parallel2 = sparkContext.parallelize(5 to 15)

    parallel.intersection(parallel2).collect.foreach(println)
  }
}