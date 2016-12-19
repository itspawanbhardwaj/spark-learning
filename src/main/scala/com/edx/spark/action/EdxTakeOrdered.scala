package com.edx.spark.action

import org.apache.spark.SparkContext

object EdxTakeOrdered {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "filter")
    val lines = sc.textFile("src/main/resources/employees.csv", 4)
    lines.takeOrdered(5)
  }
}