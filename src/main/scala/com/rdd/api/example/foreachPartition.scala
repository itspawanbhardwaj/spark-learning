package com.rdd.api.example

import org.apache.spark.SparkContext

object foreachPartition {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "foreachPartition")
    val b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    b.foreachPartition(x => println(x.reduce(_ + _)))
  }
}