package com.edx.spark.action

import org.apache.spark.SparkContext

object EdxReduce {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Reduce example")
    val b = sc.parallelize(List(2, 3, 4))
    println(b.reduce((a, b) => a * b))

  }
}