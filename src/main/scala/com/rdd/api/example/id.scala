package com.rdd.api.example

import org.apache.spark.SparkContext

object id {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "id")
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    println(a.id)
  }
}