package com.edx.spark.action

import org.apache.spark.SparkContext

object EdxSortByKey {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Sort by key")
    val lines = sc.parallelize(List((6, 2), (3, 4), (6, 6)))
    lines.sortByKey(ascending = true).foreach(println)
  }
}