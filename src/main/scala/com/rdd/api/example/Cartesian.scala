package com.rdd.api.example

import org.apache.spark.SparkContext

object Cartesian {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Cartesian Test")
    val x = sc.parallelize(List(1, 2, 3, 4, 5))
    val y = sc.parallelize(List(6, 7, 8, 9, 10))
    println(x ++ y ++ x)
    val result = x.cartesian(y)
    //result.collect
    result.foreach(println)

    val data1 = Array[(Int, Char)]((1, 'a'), (2, 'b'),
      (3, 'c'), (4, 'd'))
    val pairs1 = sc.parallelize(data1, 2)

    val data2 = Array[(Int, Char)]((1, 'A'), (2, 'B'))
    val pairs2 = sc.parallelize(data2, 2)

    val result2 = pairs1.cartesian(pairs2)

    //pairs1.foreachWith(i => i)((x, i) => println("[pairs1-Index " + i + "] " + x))
    //pairs2.foreachWith(i => i)((x, i) => println("[pairs2-Index " + i + "] " + x))
    //result2.foreachWith(i => i)((x, i) => println("[PartitionIndex " + i + "] " + x))

  }
}