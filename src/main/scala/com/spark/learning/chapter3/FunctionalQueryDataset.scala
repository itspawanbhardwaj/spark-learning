package com.spark.learning.chapter3

import java.sql.Date
import com.spark.driver.SparkDriver

object FunctionalQueryDataset {

  case class Customers(name: String, date: Date, amountSpent: Double)
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("Window function", true)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.sparkContext.parallelize(List(
      ("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-04-03", 45.00),
      ("Alice", "2016-03-04", 55.00),
      ("Bob", "2016-07-01", 25.00),
      ("Bob", "2016-03-04", 29.00),
      ("Bob", "2016-04-06", 27.00))).
      toDF("name", "date", "amountSpent")

    val ds = df.as[Customers]
    ds.show

    ds.map(customer => customer.name).show

    ds.select(year(ds("date")).as[Long].as("year_customer")).show

    val ds1 = ds.select(month(ds("date")).as[Long].as("year_customer"), $"amountSpent").as[(Long, Double)]

    val result = ds.groupByKey(value => value.name)
      .mapGroups {
        case (g, itr) => (g, itr.map(_.amountSpent).reduceLeft(Math.max(_, _)))
      }

    result.show

    val strlen = spark.udf.register("strlen", (s: String) => s.length)

    ds.select(strlen($"name")).show
  }
}