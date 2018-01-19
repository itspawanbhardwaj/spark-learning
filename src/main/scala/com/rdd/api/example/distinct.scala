package com.rdd.api.example

import org.apache.spark.SparkContext

object distinct {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "distinct")
    val b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1))
    b.distinct(5).collect.foreach(println)

  }
}