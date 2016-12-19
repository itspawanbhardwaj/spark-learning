package com.rdd.api.example

import org.apache.spark.SparkContext

object max {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "max")
    val a = sc.parallelize(1 to 9, 3)
    println(a.max);
  }
}