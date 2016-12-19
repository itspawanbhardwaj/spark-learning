package com.edx.spark.transformations

import org.apache.spark.SparkContext

object EdxFlatMap {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "filter")
    val b = sc.parallelize(List(2, 3, 4))
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    /*applying map function*/
    println("result from map function for creating list ")
    b.map(x => List(x, x + 5)).collect.foreach(println)

    println("result from map function for creating map ")
    b.map(x => (x +"pawn", x))

    println("result from flatmap function for creating list")
    b.flatMap(x => List(x, x + 5)).collect.foreach(println)

    println("result from flatmap function for creating map can not be done ")
   // a.flatMap(x => (x.length, x))
  }
}