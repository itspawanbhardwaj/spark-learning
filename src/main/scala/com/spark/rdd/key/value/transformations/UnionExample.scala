package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object UnionExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;

    val parallel = sparkContext.parallelize(1 to 9)
    val parallel2 = sparkContext.parallelize(5 to 15)

    val unionRDD = parallel.union(parallel2)

    unionRDD.collect.foreach(println)
    println("distinct example")

    unionRDD.distinct.collect.foreach(println)
  }
}