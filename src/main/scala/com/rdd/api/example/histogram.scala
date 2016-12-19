package com.rdd.api.example

import org.apache.spark.SparkContext

object histogram {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "histogram")
    val a = sc.parallelize(List(1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0), 3)
    
    //TODO: a.histogram(5)
  }
}