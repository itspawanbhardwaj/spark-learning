package com.spark.sql

import org.apache.spark.SparkContext

object JSONIngest {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "JSONIngest")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val path = "src/main/resources/people.json"
    // Create a SchemaRDD from the file(s) pointed to by path
    val people = sqlContext.jsonFile(path)

    // The inferred schema can be visualized using the printSchema() method.
   // people.printSchema()

    // root
    //  |-- age: integer (nullable = true)
    //  |-- name: string (nullable = true)

    // Register this SchemaRDD as a table.
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
   // teenagers.foreach(f => println("data :" + f))

    // Alternatively, a SchemaRDD can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
    anotherPeople.printSchema();
  }
}