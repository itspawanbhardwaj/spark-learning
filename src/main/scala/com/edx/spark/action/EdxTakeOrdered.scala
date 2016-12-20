package com.edx.spark.action

import org.apache.spark.SparkContext

object EdxTakeOrdered {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Take ordered")
    val lines = sc.textFile("src/main/resources/employees.csv", 4)
    lines.takeOrdered(5).foreach(println)
  }
}