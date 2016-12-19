package com.spark.sql

import scala.Array.canBuildFrom
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext

object TestExplode {
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
        schemaString.split(" ").map(fieldName => {

          StructField(fieldName, StringType, true)

        }))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    val peopleSchemaRDD = sqlContext.createDataFrame(rowRDD, schema)

    peopleSchemaRDD.printSchema
    peopleSchemaRDD.registerTempTable("people")
    println("People Table")

    val resultsPeople = sqlContext.sql("SELECT name, age FROM people where name = \"Justin\"")

    resultsPeople.collect().foreach(println)
  }
}