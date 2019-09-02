package com.spark.learning.chapter2

import com.spark.driver.SparkDriver

object WordCount {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Word Count Example")
    val rdd = spark.sparkContext.textFile("src/main/resources/README.md");
    rdd.flatMap(line => line.split(" ")).map(line => (line, 1))
      .reduceByKey(_ + _).collect().foreach(println)
  }
}