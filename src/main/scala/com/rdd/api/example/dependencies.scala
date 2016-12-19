package com.rdd.api.example

import org.apache.spark.SparkContext

object dependencies {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "dependencies")
    val b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1))
    println(b.dependencies.length)

    println(b.map(a => a).dependencies.length)
  }
}