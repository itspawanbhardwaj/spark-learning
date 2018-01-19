package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object countByValueExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val inputRDD = sparkContext.parallelize(Seq(10, 3, 3, 4));
    val countByValueResult = inputRDD.countByValue
    println(countByValueResult)

    val mapReduceResult = inputRDD.map(x => (x, 1)).reduceByKey((a, b) => a + b)
    mapReduceResult.collect.foreach(println)
  }

}