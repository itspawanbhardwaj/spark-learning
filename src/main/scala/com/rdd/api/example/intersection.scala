package com.rdd.api.example

import org.apache.spark.SparkContext

object intersection {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "id")
    val x = sc.parallelize(1 to 20)
    val y = sc.parallelize(10 to 30)
    x.intersection(y).collect.foreach(println)
  }
}