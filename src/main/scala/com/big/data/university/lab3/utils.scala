package com.big.data.university.lab3

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object utils extends Serializable {
  case class Trip(
    id: Int,
    duration: Int,
    startDate: Timestamp,
    startStation: String,
    startTerminal: Int,
    endDate: Timestamp,
    endStation: String,
    endTerminal: Int,
    bike: Int,
    subscriberType: String,
    zipCode: Option[String])

  case class Station(
    id: Int,
    name: String,
    lat: Double,
    lon: Double,
    docks: Int,
    landmark: String,
    installDate: Timestamp)

  object Trip {
    def parse(i: Array[String]) = {
      val fmt = new SimpleDateFormat("M/d/yyyy HH:mm")

      val zip = i.length match {
        case 11 => Some(i(10))
        case _ => None
      }

      Trip(i(0).toInt,
        i(1).toInt,
        new Timestamp(fmt.parse(i(2)).getTime()),
        i(3),
        i(4).toInt,
        new Timestamp(fmt.parse(i(5)).getTime()),
        i(6),
        i(7).toInt,
        i(8).toInt,
        i(9),
        zip)

    }
  }

  object Station {
    def parse(i: Array[String]) = {
      val fmt = new SimpleDateFormat("M/d/yyyy")
      Station(i(0).toInt,
        i(1),
        i(2).toDouble,
        i(3).toDouble,
        i(4).toInt,
        (i(5)),
        new Timestamp(fmt.parse(i(6)).getTime()))
    }
  }

}