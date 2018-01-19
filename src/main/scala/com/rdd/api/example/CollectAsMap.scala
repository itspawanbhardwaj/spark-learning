package com.rdd.api.example

import org.apache.spark.SparkContext

object CollectAsMap {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "CollectAsMap Test")

    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val v = sc.parallelize(List('a', 'b', 'c', 'd'))
    val b = a.zip(v)

    val result = b.collectAsMap

    result.foreach(println)

  }
}