package com.big.data.university.lab3

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object broadcastVariable {

  def main(args: Array[String]) = {
    val sc = new SparkContext("local", "Broadcast variable")
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

    val bcStations = sc.broadcast(stations.keyBy(_.id).collectAsMap)
    val joined = trips.map(trip =>
      (trip, bcStations.value.getOrElse(trip.startTerminal, Nil),
        bcStations.value.getOrElse(trip.endTerminal, Nil)))
        
        joined.take(10).foreach(println)
        
  }

}