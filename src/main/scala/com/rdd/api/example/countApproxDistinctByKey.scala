package com.rdd.api.example

import org.apache.spark.SparkContext

object countApproxDistinctByKey {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "CombineByKey Test")
    val a = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
    val b = sc.parallelize(a.takeSample(true, 10000, 0), 20)
    val c = sc.parallelize(1 to b.count().toInt, 20)
    val d = b.zip(c)
    //TODO: countApproxDistinctByKey not working 
    // d.countApproxDistinctByKey(0.1).collect
  }
}