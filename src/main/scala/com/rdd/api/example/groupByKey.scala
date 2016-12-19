package com.rdd.api.example

import org.apache.spark.SparkContext

object groupByKey {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "groupByKey")
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val b = a.keyBy(_.length)
    b.collect.foreach(x=> println(x))
   //TODO: b.groupByKey.collect
  }
}