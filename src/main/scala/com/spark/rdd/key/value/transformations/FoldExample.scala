package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object FoldExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    val sparkContext = SparkDriver.getSparkSession("fold example").sparkContext;

    val employeesRDD = sparkContext.parallelize(List(("Jack", 1000), ("Bob", 2000), ("Carl", 7000)))
    val dummyExmployee = ("dummy", 0)
    val maxSalaryEmployee = employeesRDD.fold(dummyExmployee)(
      (acc, employee) =>
        {
          if (acc._2 < employee._2) employee else acc
        })

    println(maxSalaryEmployee)

    val maxSalaryEmployeeReduce = employeesRDD.reduce(
      (e1, e2) =>
        ("sum", e1._2 + e2._2))
    println(maxSalaryEmployeeReduce)
  }
}