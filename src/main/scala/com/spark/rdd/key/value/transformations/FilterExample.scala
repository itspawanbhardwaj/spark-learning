package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object FilterExample {

  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val fileRDD = sparkContext.textFile("src/main/resources/Baby_Names__Beginning_2007.csv");
    println("no of rows: " + fileRDD.count)

    println("rows containing 2013")
    println(fileRDD.filter(_.contains("2013")).count)

  }
}