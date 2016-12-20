package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object DropTable {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "DropTable")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

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
    val rowRDD = people.map(_.split(",")).map(p =>
      if (p(0) == null || p(0).length() == 0) {
        Row(null, p(1).trim)
      } else {
        Row(p(0), p(1).trim)
      })

    // Apply the schema to the RDD.
    val peopleSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    // Register the SchemaRDD as a table.
    peopleSchemaRDD.registerTempTable("people")

    sqlContext.cacheTable("people")
    // SQL statements can be run by using the sql methods provided by sqlContext.

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    println("People Table")

    val resultsPeople = sqlContext.sql("SELECT name, count(name) FROM people group by name")

    resultsPeople.collect().foreach(println)

    println("LIST OF TABLES")

    sqlContext.sql("SHOW TABLES").collect.foreach(println)

    println("DESCRIBE TABLE")

    println("uncache")
    sqlContext.uncacheTable("people")

    println(sqlContext.dropTempTable("people"));

    val droppedResultsPeople = sqlContext.sql("SELECT * FROM people ")

    droppedResultsPeople.collect().foreach(println)

  }

}