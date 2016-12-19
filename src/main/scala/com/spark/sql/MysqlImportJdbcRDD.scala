package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.sql.Connection 

object MysqlImportJdbcRdd {
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver";
  val MYSQL_USERNAME = "root";
  val MYSQL_PWD = "root";
  val MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/employees?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;

  def main(args: Array[String]) {
    try {

      val sc = new SparkContext("local", "Import to spark")
      val sqlContext = new SQLContext(sc)

      val dbConnection: Connection = null;

      
     /* val jdbcRDD = new JDBCRDD<>(sc, dbConnection, "select * from employees where emp_no >= ? and emp_no <= ?",
                              10001, 499999, 10, new MapResult(), ClassManifestFactory$.MODULE$.fromClass(Array(Object).class));
      */
    } catch {
      case e: Exception => e.printStackTrace()

    }
  }
}