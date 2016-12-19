package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object foldByKeyExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val deptEmployees = List(("cs", ("jack", 1000)), ("cs", ("bron", 1000)), ("phy", ("sam", 1000)), ("phy", ("ronaldo", 1000)))
    val employeesRDD = sparkContext.parallelize(deptEmployees)
    val maxByDept = employeesRDD.foldByKey(("dummy", 0))((e1, e2) =>
      if (e1._2 > e2._2) e1 else e2)

    maxByDept.collect.foreach(println)
  }
}