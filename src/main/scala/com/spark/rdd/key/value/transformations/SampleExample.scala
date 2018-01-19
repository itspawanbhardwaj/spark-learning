package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object SampleExample {

  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val fileRDD = sparkContext.textFile("src/main/resources/Baby_Names__Beginning_2007.csv").sample(false, 1);
    //52253
    println("sample rows: " + fileRDD.count)
  }
}