package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object groupByKeyExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val fileRDD = sparkContext.textFile("src/main/resources/Baby_Names__Beginning_2007.csv");
    val rows = fileRDD.map(row => row.split(","))

    val namesOfCountries = rows.map(name => (name(1), name(2)))

    namesOfCountries.groupByKey.collect.foreach(println)

  }
}