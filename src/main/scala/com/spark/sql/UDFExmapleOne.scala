package com.spark.sql

import scala.Array.canBuildFrom
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag.Int
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

object UDFExmapleOne extends Serializable {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SchemaRdd")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD
    val udf = sc.textFile("src/main/resources/udf.txt")

    // The schema is encoded in a string
    val udfColumn = "name date_type"

    // Generate the schema based on the string of schema
    val udfSchema =
      StructType(
        udfColumn.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (udf) to Rows.
    val udfRowRDD = udf.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val udfSchemaRDD = sqlContext.applySchema(udfRowRDD, udfSchema)

    // Register the SchemaRDD as a table.
    udfSchemaRDD.registerTempTable("udf")

    /*  println("udf Table")

    val results = sqlContext.sql("SELECT date_type FROM udf ")

    results.collect().foreach(println)*/

    println("query with UDF")

    def getMonth(date: String) = {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      format.parse(date).getMonth()
    }

    def getYear(date: String) = {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      format.parse(date).getYear()
    }
    def getDay(date: String) = {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      format.parse(date).getDate()
    }

    def getAvg(num1: Int, num2: Int) = {
      (num1 + num2) / 2
    }

    def getMax(num1: Int, num2: Int) = {
      if (num1 > num2) num1
      else num2
    }
    sqlContext.udf.register("getMax", getMax _)
    sqlContext.udf.register("getAvg", getAvg _)
    sqlContext.udf.register("getMonth", getMonth _)
    sqlContext.udf.register("getYear", getYear _)
    sqlContext.udf.register("getDay", getDay _)

    val resultsudf = sqlContext.sql("SELECT getMonth(date_type)  FROM udf ")

    resultsudf.collect().foreach(println)
  }
}