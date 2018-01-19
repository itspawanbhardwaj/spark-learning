package com.rdd.api.example

import org.apache.spark.SparkContext

object join {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "join")
    var x = sc.parallelize(List(("a", 1), ("b", 4)), 2);
    var y = sc.parallelize(List(("a", 2)), 2);
    x.leftOuterJoin(y).collect().foreach(println)
    println("right outer join")
    x.rightOuterJoin(y).collect().foreach(println)
  }
}