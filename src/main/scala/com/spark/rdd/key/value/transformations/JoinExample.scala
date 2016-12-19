package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object JoinExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val names1 = sparkContext.parallelize(List("abc", "abby", "apple", "beauty", "rose", "camel", "lion")).map(a => (a, 1))
    val names2 = sparkContext.parallelize(List("apple", "beauty", "xyz", "lily", "camel")).map(a => (a, 1))
    names1.join(names2).collect.foreach(println)

    println("right")
    names1.rightOuterJoin(names2).collect.foreach(println)
    println("left")
    names1.leftOuterJoin(names2).collect.foreach(println)

  }
}