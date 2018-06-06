package com.spark.sorting

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sorting {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "DropTable")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()
    val df = spark.read.option("header", true).csv("src/main/resources/sales.csv")
    //transactionId,customerId,itemId,amountPaid
    import sqlContext.implicits._
    df.orderBy(asc("amountPaid")).show
    df.orderBy($"amountPaid".asc).show
    df.sort(desc("amountPaid")).show
    df.sort($"amountPaid".desc).show

  }

}