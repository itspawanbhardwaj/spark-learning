package com.rdd.api.example

import org.apache.spark.SparkContext

object foreach {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "foreach")
    val c = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
    c.foreach(x => println(x + "s are yummy"))
  }
}