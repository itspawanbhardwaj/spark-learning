package com.rdd.api.example

import org.apache.spark.SparkContext

object getCheckpointFile {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "getCheckpointFile")
    sc.setCheckpointDir("src/main/resources/")
    val a = sc.parallelize(1 to 500, 5)
    val b = a ++ a ++ a ++ a ++ a
  //  println(b.getCheckpointFile);
    b.checkpoint
    b.count
    println(b.getCheckpointFile);
  }
}