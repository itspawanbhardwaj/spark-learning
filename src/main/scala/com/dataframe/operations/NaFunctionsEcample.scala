package com.dataframe.operations

import com.spark.driver.SparkDriver

object NaFunctionsEcample {

  case class Employee(firstName: String, lastName: String, email: String, salary: Int)

  def main(args: Array[String]) {
    val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
    val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)

    val sparkSession = SparkDriver.getSparkSession("More operations on dataframe")

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val employees = Seq(employee4, employee1, employee2, employee3)
    val employeesDF = employees.toDF

    println("before")
    employeesDF.show
    val naFunctions = employeesDF.na
    val nonNullDF = naFunctions.fill("--")

    println("after")
    nonNullDF.show

  }
}