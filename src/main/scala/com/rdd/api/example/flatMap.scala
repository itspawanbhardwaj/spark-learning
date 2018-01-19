package com.rdd.api.example

import org.apache.spark.SparkContext

object flatMap {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "filterWith")
    val a = sc.parallelize(List(1 ,2,3,4,3))
    a.flatMap(x=> List(x)).foreach(println)
  }
}