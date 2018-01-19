package com.dataframe.operations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object DataframeHelper {

  def getDataframe(sparkSession: SparkSession, fileLocation: String): DataFrame = {

    // Create a DF
    val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(fileLocation)
    df
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "English Wikipedia pageviews by second")
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.option("header", "true").option("inferSchema", "true").csv("/home/mayank-ideata/setup/project/spark-wiki/sample-data/sample1.txt")

    df.show
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    df.orderBy(desc("COLUMN_A")).take(1).foreach(println) //.agg(count("col2").alias("count"), countDistinct("col2").alias("distinct")).show

    /* df.rdd
      .map(row => (row(0).toString.toInt, row(1)))
      .sortByKey(false)
      .take(1).foreach(println)
*/
    println("finished")

  }
}