package com.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import java.util.LinkedList
import org.apache.spark.sql.SQLContext

object DataFrames {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "DropTable")
    val sqlContext = new SQLContext(sc)

    // Create an RDD
    val people = sc.textFile("src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Import Spark SQL data types and Row.
    import org.apache.spark.sql._

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    var df = sqlContext.createDataFrame(rowRDD, schema)

    // Register the SchemaRDD as a table.
    df.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    println("People Table")

    val resultsPeople = sqlContext.sql("select * from (SELECT name FROM people ) a")
    resultsPeople.collect.foreach(println)

    /*println(df.show(1));
    df.printSchema()
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select(df("name"), df("age") + 1).show()

    // Select people older than 21
    df.filter(df("age") > 21).show()

    // Count people by age
    df.groupBy("age").count().show()*/
  }
}