package com.big.data.university.lab4

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.SQLContext

import com.big.data.university.lab3.utils.Station
import com.big.data.university.lab3.utils.Trip
import com.esotericsoftware.kryo.Kryo

object KryoSerializerEx {
  def main(args: Array[String]) = {
    val sc = new SparkContext("local", "Kryo serializer example")

    val a = sc.parallelize(List(("a", 1), ("a", 5), ("b", 6), ("b", 3), ("c", 2)))
    a.reduceByKey((a, b) =>
      a > b match {
        case true => a
        case false => b
      }).collectAsMap.foreach(println)
  }

}

class Lab4KryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Trip])
    kryo.register(classOf[Station])
  }
}