package com.edx.spark.transformations

import org.apache.spark.SparkContext

object mapValues {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "mapValues")
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)

    /*creating map using .map function*/
    val b = a.map(x => (x.length, x))
    b.collect.foreach(println)

    var list = sc.parallelize(List(1, 3, 2, 5, 6), 3);
    var doubleList = list.map(x => x * 2)
    doubleList.collect.foreach(println)

  }
}