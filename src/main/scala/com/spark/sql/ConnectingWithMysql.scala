package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.HashMap
import org.apache.spark.sql.DataFrame
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.LinkedHashMap
import java.util.LinkedList
import scala.util.control.Breaks
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ConnectingWithMysql {
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver";
  val MYSQL_USERNAME = "root";
  val MYSQL_PWD = "root";
  val MYSQL_CONNECTION_URL = "jdbc:mysql://192.168.2.7:3306/employees"

  val PARTITION_QUERY = "(select PARTITION_EXPRESSION from information_schema.PARTITIONS WHERE TABLE_SCHEMA = 'employees' AND TABLE_NAME = 't2' limit 1) "
  val COLUMN_DATATYPE_QUERY = "(SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA IN (\"" + "employees" + "\") AND TABLE_NAME IN (\"" + "t2" + "\"))";
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    try {
      val sc = new SparkContext("local", "Import to spark")
      val sqlContext = new SQLContext(sc)

      //Load MySQL query result as DataFrame
      val connection = getConnection(MYSQL_DRIVER, MYSQL_CONNECTION_URL, MYSQL_USERNAME, MYSQL_PWD)

      val stmt = connection.createStatement();

      val partitionRS = stmt.executeQuery(PARTITION_QUERY);

      var partitionCol: String = null
      while (partitionRS.next) {
        partitionCol = partitionRS.getString(1)
      }

      if (partitionCol.size != 0) {
        val row = partitionCol.apply(0)
        val column = partitionCol.split("\\+")
        var map = new LinkedHashMap[String, String]
        println("column : " + column.apply(0))
        val COL_DATATYPE_RS = stmt.executeQuery(COLUMN_DATATYPE_QUERY);
        var table_schema: Array[(String, String)] = Array()
        while (COL_DATATYPE_RS.next) {
          table_schema = table_schema ++ Array((COL_DATATYPE_RS.getString("COLUMN_NAME"), COL_DATATYPE_RS.getString("DATA_TYPE")))
        }

        for (i <- 0 to table_schema.size - 1) {
          val rows = table_schema.apply(i)

          column.foreach(columnName => {
            val col = columnName.trim();
            val dataType = rows._2.trim
            val schemaCol = rows._1.trim
            if (col.equalsIgnoreCase(schemaCol) || col.equalsIgnoreCase("TO_DAYS(" + schemaCol + ")") && (dataType.equalsIgnoreCase("int") || dataType.equalsIgnoreCase("long") || dataType.equalsIgnoreCase("date"))) {

              map.put(schemaCol, dataType.toLowerCase())

            }
          })
        }

        val mapCols = new LinkedList[String]
        mapCols.addAll(map.keySet())
        val loop = new Breaks;

        loop.breakable(
          for (colCounter <- 0 to mapCols.size() - 1) {
            if (map.get(mapCols.get(colCounter)).equalsIgnoreCase("int") || map.get(mapCols.get(colCounter)).equalsIgnoreCase("long")) {
              println("apply integer partioning")
              loop.break
            } else if (map.get(mapCols.get(colCounter)).equalsIgnoreCase("date")) {
              println("apply date partioning on column : " + mapCols.get(colCounter))
              //select MIN(DATE_FORMAT(hire_date, '%Y%m%d'))
              val mindateRS = stmt.executeQuery("select MIN(DATE_FORMAT(" + mapCols.get(colCounter) + ", '%Y%m%d')) from t2");

              var min: Long = 0l
              while (mindateRS.next) {
                min = mindateRS.getString(1).toLong
              }

              val maxdateRS = stmt.executeQuery("select MAX(DATE_FORMAT(" + mapCols.get(colCounter) + ", '%Y%m%d')) from t2");
              var max: Long = 0l
              while (maxdateRS.next) {
                max = maxdateRS.getString(1).toLong
              }

              val query = "select " + mapCols.get(colCounter) + " from " + " t2 " + "where (DATE_FORMAT(" + mapCols.get(colCounter) + ", '%Y%m%d')) > ? and (DATE_FORMAT(" + mapCols.get(colCounter) + ", '%Y%m%d')) < ?"
              val rdds = MysqlImportWithPartition.getMysqlRDD(sc, min, max, query, MYSQL_DRIVER, MYSQL_CONNECTION_URL + "?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD)
              rdds.collect.foreach(println)
              loop.break
            }
          })

      }
      connection.close()

      println("finish");
    } catch {
      case e: Exception => e.printStackTrace()

    }
  }

  def getConnection(driver: String, url: String, username: String, password: String): Connection = {
    var columns: String = null
    var connection: Connection = null;
    try {
      Class.forName(driver)
      DriverManager.getConnection(
        url, username, password);

    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    } finally {
      /*if (connection != null) {
        println("closing connection!!")
        connection.close()
      }*/
    }
  }

}