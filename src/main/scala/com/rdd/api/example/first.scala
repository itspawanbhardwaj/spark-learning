package com.rdd.api.example

import org.apache.spark.SparkContext

object first {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "first")
    val b = sc.parallelize(List(6, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1))
    println(b.first);

  }
}