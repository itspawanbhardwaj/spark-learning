package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
object JoinTables {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SchemaRdd")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD
    val people = sc.textFile("src/main/resources/people.txt")
    val dept = sc.textFile("src/main/resources/dept.txt")
    val salary = sc.textFile("src/main/resources/salary.txt")
    val title = sc.textFile("src/main/resources/title.txt")

    // The schema is encoded in a string
    val peopleColumn = "name age"
    val deptColumn = "deptName age"
    val salaryColumn = "salary age"
    val titleColumn = "title age"

    // Generate the schema based on the string of schema
    val peopleSchema =
      StructType(
        peopleColumn.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val deptSchema =
      StructType(
        deptColumn.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val salarySchema =
      StructType(
        salaryColumn.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val titleSchema =
      StructType(
        titleColumn.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val peopleRowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val deptRowRDD = dept.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val salaryRowRDD = salary.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val titleRowRDD = title.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleSchemaRDD = sqlContext.applySchema(peopleRowRDD, peopleSchema)

    val deptSchemaRDD = sqlContext.applySchema(deptRowRDD, deptSchema)

    val salSchemaRDD = sqlContext.applySchema(salaryRowRDD, salarySchema)

    // Register the SchemaRDD as a table.
    peopleSchemaRDD.registerTempTable("people")

    deptSchemaRDD.registerTempTable("dept")

    salSchemaRDD.registerTempTable("salary")
    // SQL statements can be run by using the sql methods provided by sqlContext.

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    println("People Table")

    val resultsPeople = sqlContext.sql("SELECT * FROM people ")

    resultsPeople.collect().foreach(println)

    println("Dept Table")

    val resultsEmp = sqlContext.sql("SELECT * FROM dept ")

    resultsEmp.collect().foreach(println)

    println("Salary Table")
    val resultsSal = sqlContext.sql("SELECT * FROM salary ")

    resultsSal.collect().foreach(println)

    val resultsJoin = sqlContext.sql("SELECT * FROM dept inner join people on dept.age = people.age")
    println("join two Table")
    resultsJoin.collect().foreach(println)

    val resultsThreeJoin = sqlContext.sql("SELECT dept.deptName , people.age , salary.age FROM dept inner join people on dept.age = people.age inner join salary on dept.age = salary.age")
    println("join three Table")
    resultsThreeJoin.collect().foreach(println)
  }
}