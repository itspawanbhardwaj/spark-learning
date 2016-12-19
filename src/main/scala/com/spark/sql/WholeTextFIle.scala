package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object WholeTextFIle {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "DropTable")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD
    val people = sc.wholeTextFiles("/home/ideata/setup/sampledata/folderTest/*/*/*/*.csv")

    people.collect.foreach(println)

  }
}