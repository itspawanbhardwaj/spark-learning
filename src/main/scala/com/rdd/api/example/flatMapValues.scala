package com.rdd.api.example

import org.apache.spark.SparkContext

object flatMapValues {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "flatMapValues")
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    b.collect.foreach(println)
   //TODO : b.flatMapValues("x" + _ + "x").collect
  }
}