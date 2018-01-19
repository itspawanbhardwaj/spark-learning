package com.spark.two.updates

import org.apache.spark.sql.SparkSession

object PivotExample {
  //

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    import sparkSession.implicits._
    val data = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("/home/mayank-ideata/setup/sampledata/firstpost/SearchWarsStory_v4.csv")

//    data.show
//    data.printSchema
//    data.select($"Category", $"Comparisons", expr("stack(1, '05/06/2016',05/06/2016) as (B, C)")).where("C is not null").show
    
    
//    val df = Seq(("G",Some(4),2,None,"pawan"),("H",None,4,Some(5),"bhardwaj")).toDF("A","X","Y", "Z","name")
//    
//    df.select($"A", expr("stack(3, 'X', X, 'Y', Y, 'Z', Z) as (B, C)")).where("C is not null").show
  }
}