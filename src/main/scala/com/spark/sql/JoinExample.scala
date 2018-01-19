package com.spark.sql

import org.apache.spark.SparkContext

object JoinExample {

  case class Register(d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float)

  case class Click(d: java.util.Date, uuid: String, landing_page: Int)

  def main(args: Array[String]) {

    println("lets rock!!");

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val sc = new SparkContext("local", "JSONIngest")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val reg = sc.textFile("src/main/resources/11381941/reg.tsv")
      .map(_.split("\t"))
      .map(r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat)))

    val clk = sc.textFile("src/main/resources/11381941/clk.tsv")
      .map(_.split("\t"))
      .map(
        c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt)))

    //reg.collect.foreach(println)
    //clk.collect.foreach(println)

    println("join")

   // println(reg.join(clk).toDebugString);

    reg.join(clk).collect.foreach(println)

    println("finished!!");
  }
}