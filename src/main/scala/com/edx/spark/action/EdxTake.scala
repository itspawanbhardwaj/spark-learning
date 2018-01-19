package com.edx.spark.action

import org.apache.spark.SparkContext

object EdxTake {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Take example")
    val lines = sc.textFile("src/main/resources/employees.csv", 4)
    lines.take(5).filter(s => s.contains("M")).foreach(println)
  }
}