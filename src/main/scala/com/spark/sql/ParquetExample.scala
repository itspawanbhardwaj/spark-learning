package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
object ParquetExample {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SchemaRdd")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // Create an RDD
    val people = sc.textFile("src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    // Register the SchemaRDD as a table.
    peopleSchemaRDD.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people")

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    results.rdd.map(t => "Name: " + t(0)).collect().foreach(println)
    // saving schema to parquet file
    //peopleSchemaRDD.saveAsParquetFile("src/main/resources/peopleTwo.parquet")
  }
}