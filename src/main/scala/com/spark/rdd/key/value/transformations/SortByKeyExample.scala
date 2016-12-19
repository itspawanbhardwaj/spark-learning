package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object SortByKeyExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val fileRDD = sparkContext.textFile("src/main/resources/Baby_Names__Beginning_2007.csv");

    val filteredRows = fileRDD
      .filter(line => !line.contains("Count"))
      .map(line => line.split(","))

    filteredRows
      .map(row => (row(1), row(4)))
      .sortByKey()
      .take(10)
      .foreach(println)

    filteredRows
      .map(row => (row(1), row(4)))
      .sortByKey(false)
      .take(10)
      .foreach(println)
  }
}