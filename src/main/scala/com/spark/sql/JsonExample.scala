package com.spark.sql

import scala.collection.mutable.WrappedArray
import scala.reflect.runtime.universe

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

object JsonExample {
  case class ExplodeColumn(val newColumnName: String)
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "DropTable")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val path = "src/main/resources/disney.json"
    // Create a SchemaRDD from the file(s) pointed to by path
    val people = sqlContext.jsonFile(path)
    people.printSchema
    println(people.schema.fields.size);
    import scala.collection.mutable
    import scala.collection.JavaConversions._
    import scala.io.Source._
    import org.apache.spark.rdd.RDD
    import org.json4s.JValue
    import org.apache.spark.sql.Row
    import scala.collection.mutable.{ ArrayBuffer, LinkedHashMap }
    import scala.collection.JavaConversions
    import org.apache.spark.sql.DataFrame
    import scala.collection.mutable.HashMap
    import java.util.ArrayList
    import org.apache.spark.sql.AnalysisException
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.Column
    import java.text.SimpleDateFormat
    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._
    import java.util.UUID
    import org.apache.spark.serializer.KryoSerializer
    import scala.collection.mutable.WrappedArray
    import scala.collection.mutable.LinkedList
    import org.json4s.JString
    import org.json4s.JArray
    var dd = people.select("ItemList").explode("ItemList", "ItemList")({
      case Row(pattern: WrappedArray[String]) =>
        pattern.map(
          row => {
            if (row != null)
              ExplodeColumn(row)
            else
              ExplodeColumn("")
          })
      case _ => Array(ExplodeColumn(""))
    })

    println(" dd.count : " + dd.count);
    /*people.rdd.map(line =>
      {
        val lst = new ArrayList[String]
        line.toSeq.foreach(element => {
          val ss = if (element.isInstanceOf[List[Any]]) {
            val sub1 = element.toString.substring(5)
            val sub2 = sub1.substring(0, sub1.length() - 1)
            "\"[" + sub2.toString() + "]\""
          } else {
            if (element != null) {
              element.toString()
            } else {
              " "
            }
          }
          lst.add(ss)
        })
        lst
        JavaConversions.asScalaBuffer(lst).mkString(";")
      })*/

    //.repartition(1).saveAsTextFile("/home/ideata/setup/sampledata/disney/disneyDataTerminator.csv")

    //.take(5).foreach(println)

    //.repartition(1).saveAsTextFile("/home/ideata/setup/sampledata/disney/disneyData1.csv")

    //.repartition(1).saveAsTextFile("/home/ideata/setup/sampledata/disney/disneyData.csv")
    println("finish!!");
  }
}