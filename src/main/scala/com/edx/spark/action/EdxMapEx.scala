package com.edx.spark.action

import org.apache.spark.SparkContext

object EdxMapEx {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "filter")

    val b = sc.parallelize(List(("2008-M", 100), ("2008-F", 200), ("2009-M", 300), ("2009-F", 400), ("2010-F", 500), ("2011-M", 600)))
    var list1=b.flatMap(x=> List(x._1))
println(list1)
  }
}