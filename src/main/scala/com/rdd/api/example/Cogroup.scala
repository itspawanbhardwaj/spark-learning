package com.rdd.api.example

import org.apache.spark.SparkContext

object Cogroup {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Cogroup Test")

    val a = sc.parallelize(List(1, 2, 1, 3), 2)
    val b = sc.parallelize(List(1, 2, 3, 4, 5, 6), 3)
    val d = a.map((_, "b"))

    val e = b.map((_, "c"))

    d.foreach(println)
    val result = a.groupBy("");

  }
}