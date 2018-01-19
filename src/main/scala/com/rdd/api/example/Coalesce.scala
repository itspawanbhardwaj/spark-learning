package com.rdd.api.example

import org.apache.spark.SparkContext

object Coalesce {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Coalesce Test")

    val y = sc.parallelize(1 to 10, 10)

    println("y : ");
    y.foreach(println)

    val z = y.coalesce(2, true)

    println("after coalesce");
    z.foreach(println)
  }
}