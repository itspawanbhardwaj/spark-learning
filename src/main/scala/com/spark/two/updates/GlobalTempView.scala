package com.spark.two.updates

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Introduces with spark 2.1.0
 */
object GlobalTempView {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Global Temp View")
      .getOrCreate()

    val df = sparkSession.read.option("header", true).csv("src/main/resources/sales.csv")

    // Register the DataFrame as a global temporary view
  //  df.createGlobalTempView("sales")

    // Global temporary view is tied to a system preserved database `global_temp`
    sparkSession.sql("SELECT * FROM global_temp.sales").show()

    val catalog = sparkSession.catalog

    //print the databases
    catalog.listDatabases().show
    // print all the tables

    catalog.listTables().select("name").show()
  }
}