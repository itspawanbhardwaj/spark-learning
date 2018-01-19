package com.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql._
object SchemaRdd {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SchemaRdd")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD
    val people = sc.textFile("src/main/resources/people.txt")
    val emp = sc.textFile("src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Import Spark SQL data types and Row.

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    val empSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    val salSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    // Register the SchemaRDD as a table.
    peopleSchemaRDD.registerTempTable("people")

    empSchemaRDD.registerTempTable("employee")

    salSchemaRDD.registerTempTable("salary")
    // SQL statements can be run by using the sql methods provided by sqlContext.

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    println("People Table")

    val resultsPeople = sqlContext.sql("SELECT * FROM people ")

    resultsPeople.collect.foreach(println)

    println("Emp Table")

    val resultsEmp = sqlContext.sql("SELECT * FROM employee ")

    resultsEmp.collect.foreach(println)

    println("Salary Table")
    val resultsSal = sqlContext.sql("SELECT * FROM salary ")

    resultsSal.collect.foreach(println)

 /*   val resultsJoin = sqlContext.sql("SELECT * FROM employee inner join people on employee.age = people.age")
    println("join two Table")
    resultsJoin.collect().foreach(println)

    val resultsThreeJoin = sqlContext.sql("SELECT employee.age FROM employee inner join people on employee.age = people.age inner join salary on employee.age = salary.age")
    println("join three Table")
    resultsThreeJoin.collect().foreach(println)*/
  }
}