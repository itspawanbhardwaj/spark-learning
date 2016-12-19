package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.HashMap
import org.apache.spark.sql.Row
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager
import breeze.util.partition
import java.sql.ResultSet
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.sql.DataFrame
//select PARTITION_EXPRESSION from information_schema.PARTITIONS WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 't2' limit 1;
object MysqlImportWithPartition extends Serializable {
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver";
  val MYSQL_USERNAME = "root";
  val MYSQL_PWD = "root";
  val MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/employees?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Import to spark")
    val sqlContext = new SQLContext(sc)
    val rdd: JdbcRDD[String] = getMysqlRDD(sc, 19520201, 19650201,
      "(select birth_date from employees where (DATE_FORMAT(birth_date, '%Y%m%d')) > ? and (DATE_FORMAT(birth_date, '%Y%m%d')) < ? )", MYSQL_DRIVER, MYSQL_CONNECTION_URL)

    rdd.collect.foreach(println)
    println("finish");
  }

  def getMysqlRDD(sc: SparkContext, min: Long, max: Long, query: String, driver: String, url: String): JdbcRDD[String] = {
    println("min : " + min);
    println("max : " + max);
    println("query : " + query);
    val rdd: JdbcRDD[String] = new JdbcRDD(
      sc, () => {
        Class.forName(driver)
        DriverManager.getConnection(url)
      },
      query,
      min, max, 10,
      (rs: ResultSet) => {
        rs.getString("dt")
      })

    rdd
  }

  def getPartitionInfo(options: Map[String, String], schema: String, tables: List[String]): (String, String, String, Long, Long) = {
    val sc = new SparkContext("local", "Import to spark")
    val sqlContext = new SQLContext(sc)
    var schemaName: String = schema
    var tableName: String = ""
    var lowerBound: Long = 0l
    var upperBound: Long = 0l
    var size = tables.size - 1
    if (tables.size == 0)
      (null, null, null, 0l, 0l)
    else {
      val table = tables.head
      tableName = if (table.toString.contains(".")) {
        schemaName = table.toString.split("\\.").head
        table.toString.split("\\.").tail.mkString(".")
      } else
        table.toString
      val primaryKeySQL = "(SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA IN (\"" + schemaName + "\") AND TABLE_NAME IN (\"" + tableName + "\") AND COLUMN_KEY =(\"PRI\") ) as primaryKeyTable"
      var keyOptions = options
      keyOptions += ("dbtable" -> primaryKeySQL)
      val jdbcDF = sqlContext.load("jdbc", keyOptions)
      val colInfo = if (jdbcDF == null)
        Row()
      else {
        val rdd = jdbcDF.collect
        if (rdd.size != 0)
          rdd(0)
        else
          Row()
      }
      val result = if (colInfo.size > 0 && (colInfo(1).toString.equalsIgnoreCase("int") || colInfo(1).toString.equalsIgnoreCase("long") || colInfo(1).toString.equalsIgnoreCase("bigint")))
        if (colInfo(0).toString.contains("."))
          colInfo(0).toString.split("\\.").tail.mkString(".")
        else
          colInfo(0).toString
      else
        null

      if (result != null) {
        val minMaxSQL = "(select min(" + result + "), max(" + result + ") from " + schemaName + "." + tableName + " ) as minMaxTable"
        var keyOptions = options
        keyOptions += ("dbtable" -> minMaxSQL)
        val jdbcDF = sqlContext.load("jdbc", keyOptions)
        val minMax = jdbcDF.first
        (schemaName, tableName, result, minMax(0).toString.toLong, minMax(1).toString.toLong)
      } else {
        getPartitionInfo(options, schema, tables.tail)
      }

    }
  }

}