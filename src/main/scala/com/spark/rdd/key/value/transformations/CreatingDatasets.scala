package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object CreatingDatasets {

  case class Person(name: String, age: Int)
  case class Company(name: String, foundingYear: Int, numEmployees: Int)

  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("Creating Datasets")
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    println("from seq")
    val dataset = Seq(1, 2, 3).toDS()
    dataset.show()

    println("from case class")
    val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
    personDS.show()

    println("Creating Datasets from a RDD You can call rdd.toDS() to convert an RDD into a Dataset.")
    val rdd = sparkSession.sparkContext.parallelize(Seq((1, "Spark"), (2, "Databricks")))
    val integerDS = rdd.toDS()
    integerDS.show()

    println("Creating Datasets from a DataFrame You can call df.as[SomeCaseClass] to convert the DataFrame to a Dataset.")
    val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
    val df = sparkSession.sparkContext.parallelize(inputSeq).toDF()

    val companyDS = df.as[Company]
    companyDS.show()

    println("You can also deal with tuples while converting a DataFrame to Dataset without using a case class")
    val rdd1 = sparkSession.sparkContext
      .parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
    val df1 = rdd.toDF("Id", "Name")

    val dataset1 = df1.as[(Int, String)]
    dataset1.show()
  }
}