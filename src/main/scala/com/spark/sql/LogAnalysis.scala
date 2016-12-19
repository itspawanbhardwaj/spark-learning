package com.spark.sql

import java.util.regex.Pattern
import scala.Array.canBuildFrom
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import scala.collection.mutable.HashMap
import org.apache.spark.Accumulable

object LogAnalysis {

  def GRAILS_ENTRY_PATTERN = "(\\d{2}\\/\\d{2}\\/\\d{2}) (\\d{2}:\\d{2}:\\d{2}) ([A-Z]*) [A-Za-z]*:([a-z@\\-.]*) , ([A-Za-z]*).([A-Za-z]*) : (.*)"

  private val GRAILS = Pattern.compile(GRAILS_ENTRY_PATTERN)

  def INGEST_ENTRY_PATTERN = "(\\d{2}\\/\\d{2}\\/\\d{2}) (\\d{2}:\\d{2}:\\d{2}) ([A-Z]*) ([A-Za-z]*):(.*)"

  private val INGEST = Pattern.compile(INGEST_ENTRY_PATTERN)
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "LogAnalysis")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD
    val logFiles = List("/home/ideata/setup/project/bda-core/logs/bda.log",
      "/home/ideata/setup/project/bda-core/logs/bda.log.2015-07-14",
      "/home/ideata/setup/project/bda-core/logs/bda.log.2015-07-15",
      "/home/ideata/setup/project/bda-core/logs/bda.log.2015-07-16")

    val logs = sc.textFile("/home/ideata/setup/project/bda-core/logs/bda.log")

    // The schema is encoded in a string
    val schemaString = "index date time level username class method message source"

    // Import Spark SQL data types and Row.
    import org.apache.spark.sql._

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.

    var index: Long = 0
    val acc = sc.accumulator(index)
    val rowRDD = logs.zipWithIndex.map {
      case (logline, i) => {
        var m = GRAILS.matcher(logline)
        val m1 = INGEST.matcher(logline)

        if (m.find()) {
          acc += 1
          index = acc.localValue
          Row(acc.localValue.toString,
            m.group(1),
            m.group(2),
            m.group(3),
            m.group(4),
            m.group(5),
            m.group(6),
            m.group(7),
            "grails")
        } else if (m1.find()) {
          acc += 1
          Row(acc.localValue.toString,
            m1.group(1),
            m1.group(2),
            m1.group(3),
            "",
            m1.group(4),
            "",
            m1.group(5),
            "ingest")
        } else {
          Row(acc.localValue.toString,
            "",
            "",
            "",
            "",
            "",
            "",
            logline,
            "ingest")
        }
      }
    } //.map(parse(_))

    val logSchemaRDD = sqlContext.createDataFrame(rowRDD, schema)

    // Register the SchemaRDD as a table.
    logSchemaRDD.registerTempTable("logs")

    // SQL statements can be run by using the sql methods provided by sqlContext.

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    println("log Table")

    val resultsPeople = sqlContext.sql("SELECT index, level ,message FROM logs where index = \"2\"")

    resultsPeople.collect().foreach(println)
  }

  def filterLogs(logline: String, i: Long) = {
    val m = GRAILS.matcher(logline)
    val m1 = INGEST.matcher(logline)
    if (!m.find() && !m1.find()) {
      false
    } else {
      true
    }
  }

  def parse(logline: String, i: Long) = {

    var m = GRAILS.matcher(logline)
    val m1 = INGEST.matcher(logline)
    if (m.find()) {
      Row(i,
        m.group(1),
        m.group(2),
        m.group(3),
        m.group(4),
        m.group(5),
        m.group(6),
        m.group(7),
        "grails")
    } else if (m1.find()) {

      Row(i,
        m1.group(1),
        m1.group(2),
        m1.group(3),
        "",
        m1.group(4),
        "",
        m1.group(5),
        "ingest")
    } else {
      Row(i,
        "",
        "",
        "",
        "",
        "",
        "",
        logline,
        "ingest")
    }

  }
}