package com.spark.learning.chapter2

import com.spark.driver.SparkDriver

object WordCountWithStopWordsFiltered {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Word count example with stop words filtered")

    val illegalCharacters = Array[Char](',', '.', '>', '<', '=') ++ Array(' ')
    val stopWords = Set[String]("with", "and", "or")
    val rdd = spark.sparkContext.textFile("src/main/resources/README.md");
    val wordRDD = rdd.flatMap(_.split(illegalCharacters))
      .map(_.toLowerCase())

    wordRDD
      .filter(word => !stopWords.contains(word) && word.length() != 0)
      .map(line => (line, 1))
      .reduceByKey(_ + _).collect().foreach(println)
  }
}