package com.spark.driver

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkDriver {

  def getSparkSession(appName: String): SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  def getSparkSession(appName: String, isHive: Boolean): SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
  }
}