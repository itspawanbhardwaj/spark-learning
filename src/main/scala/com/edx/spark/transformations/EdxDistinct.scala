package com.edx.spark.transformations

import org.apache.spark.SparkContext

object EdxDistinct {
def main(args: Array[String]) {

    val sc = new SparkContext("local", "Distinct")
    val b = sc.parallelize(List(6, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1))
    b.distinct.collect.foreach(println)

  }
}