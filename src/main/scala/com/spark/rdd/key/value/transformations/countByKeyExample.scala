package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object countByKeyExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");

    val sparkContext = SparkDriver.getSparkSession("fold example").sparkContext;

    val employeesRDD = sparkContext.parallelize(List(("Jack", 1000), ("Jack", 2000), ("Carl", 7000)))

    employeesRDD.cache
    employeesRDD.count
    employeesRDD.count
    println("finished")

  }
}