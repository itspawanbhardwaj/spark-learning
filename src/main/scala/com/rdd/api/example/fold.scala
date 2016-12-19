package com.rdd.api.example

import org.apache.spark.SparkContext

object fold {
  def main(args: Array[String]) {
    /*def fold(zeroValue: T)(op: (T, T) => T): T*/
    //TODO : didnt got
    val sc = new SparkContext("local", "fold")
    val a = sc.parallelize(List(1, 2, 3), 3)
    def value = a.fold(2)(_ + _)
    println(value);
  }
}