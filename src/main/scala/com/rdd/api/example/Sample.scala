package com.rdd.api.example

import org.apache.spark.SparkContext

object Sample {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Sample Test")
    val d = sc.parallelize(1 to 100, 10)
    val result1 = d.sample(false, 0.1, 0)
    val result2 = d.sample(true, 0.1, 0)
    println("result 1:")
    result1.collect.foreach(x => print(x + " "))

    println("result 2:")
    result2.collect.foreach(x => print(x + " "))

  }
}