package com.rdd.api.example

import org.apache.spark.SparkContext

object Checkpoint {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Checkpoint Test")
    val FILE_LOCATION = "src/main/resources/"

    sc.setCheckpointDir(FILE_LOCATION)
    val a = sc.parallelize(1 to 4, 2)
    println(" a.checkpoint " + a.checkpoint);
    a.foreach(println)
  }
}