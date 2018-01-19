package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ReadingParquet {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SchemaRdd")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.parquetFile("src/main/resources/peopleTwo.parquet")

    df.printSchema
  }
}