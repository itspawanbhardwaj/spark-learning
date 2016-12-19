package com.edx.spark.action

import org.apache.spark.SparkContext

object EdxReduceByKey {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "filter")
    val lines = sc.parallelize(List((6, 2), (3, 4), (6, 6)))
    lines.reduceByKey((a, b) => a + b).collect.foreach(println)
  }
}