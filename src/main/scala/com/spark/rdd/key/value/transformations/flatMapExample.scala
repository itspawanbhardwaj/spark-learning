package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object flatMapExample {

  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val fileRDD = sparkContext.textFile("src/main/resources/sample_fpgrowth.txt");
    println("map Result ")
    import scala.runtime.ScalaRunTime._
    fileRDD
      .map(line => line.split(","))
      .collect
      .foreach(array => println(stringOf(array)))

    println("flatmap Result ")

    fileRDD
      .flatMap(line => line.split(",")) // .flatMap(line =>List( line.split(",")))
      .collect
      .foreach(array => println(stringOf(array)))
    println(" stackoverflow ques ")
    foldStackoverflow
  }

  def foldStackoverflow() {
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;

    val rdd = sparkContext.parallelize(List(
      List("userA", "2016-10-12", List(10000, 100001)),
      List("userA", "2016-10-12", List(10000, 100001)),
      List("userA", "2016-10-12", List(10000, 100001))))

    rdd.flatMap(aa => aa(2).asInstanceOf[List[Int]]).collect.foreach(println)

    rdd.collect.foreach(println)

  }

}