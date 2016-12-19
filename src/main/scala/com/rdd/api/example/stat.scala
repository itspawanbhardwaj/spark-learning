package com.rdd.api.example

import org.apache.spark.SparkContext

object stat {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "mean")
    val x = sc.parallelize(List(1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 19.29, 11.09, 21.0), 2)
   // x.s
  }
}