package com.spark.sql

import scala.Array.canBuildFrom
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WikiDataframe {

  def getDataframe(sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val separator = ","
    // Create an RDD
    val wiki_data = sc.textFile("src/main/resources/wikidata.txt")

    // The schema is encoded in a string
    val header = wiki_data.first

    // Import Spark SQL data types and Row.
    import org.apache.spark.sql._

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        header.split(separator).zipWithIndex.map {
          case (fieldName, i) => {
            if (i == 2) {
              StructField(fieldName.replace("\"", "").trim, LongType, true)
            } else {
              StructField(fieldName.replace("\"", "").trim, StringType, true)
            }

          }
        })

    // Convert records of the RDD (people) to Rows.
    val rowRDD =
      wiki_data
        .filter(line => line != header)
        .map(_.split(separator)).map(p => Row(p(0).replace("\"", "").trim, p(1).replace("\"", "").trim, p(2).toLong))

    // Apply the schema to the RDD.
    var df = sqlContext.createDataFrame(rowRDD, schema)

    /*// Register the SchemaRDD as a table.
    df.registerTempTable("english_wikipedia")*/

    df.registerTempTable("pageviews_by_second")

    df.cache

    sqlContext.cacheTable("pageviews_by_second")

    df
  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "English Wikipedia pageviews by second")
    val sqlContext = new SQLContext(sc)

    val pageViewsDF = WikiDataframe.getDataframe(sc, sqlContext)

    val queryResult = sqlContext.sql("select * from pageviews_by_second")
    queryResult.describe().show()
    
    println("finished")
  }

}