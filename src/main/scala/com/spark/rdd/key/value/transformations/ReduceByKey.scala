package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object ReduceByKey {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val fileRDD = sparkContext.textFile("src/main/resources/Baby_Names__Beginning_2007.csv");

    val filteredRows = fileRDD
      .filter(line => !line.contains("Count"))
      .map(line => line.split(","))

    filteredRows.map(row => (row(1), row(4)))
      .reduceByKey((v1, v2) => v1 + v2)
      .collect
      .foreach(println)

  }
}