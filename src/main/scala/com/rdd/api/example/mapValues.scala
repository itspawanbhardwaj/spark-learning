package com.rdd.api.example

import org.apache.spark.SparkContext

object mapValues {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "mapValues")
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
  //TODO  b.map
  }
}