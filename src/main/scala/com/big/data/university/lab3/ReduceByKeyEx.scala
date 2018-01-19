package com.big.data.university.lab3

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext

object ReduceByKeyEx {
  def main(args: Array[String]) = {
    val sc = new SparkContext("local", "Reduce by key")
    val sqlContext = new SQLContext(sc)
    val input1 = sc.textFile("src/main/resources/bdu/trips/*");
    val header1 = input1.first

    val trips = input1.
      filter(_ != header1).
      map(_.split(",")).
      map(com.big.data.university.lab3.utils.Trip.parse(_))

    val input2 = sc.textFile("src/main/resources/bdu/stations/*")
    val header2 = input2.first

    val stations = input2.
      filter(_ != header2).
      map(_.split(",")).
      map(com.big.data.university.lab3.utils.Station.parse(_))

    val byStartTerminal = trips.keyBy(_.startStation)
    val durationByStart = byStartTerminal.mapValues(_.duration)

    val firstTrip = byStartTerminal.reduceByKey((a, b) => {
      a.startDate.before(b.startDate) match {
        case true => a
        case false => b
      }
    })

    println(firstTrip.toDebugString)
    firstTrip.take(10).foreach(println) //14 secs

  }
}