package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ExportToMysql {
  val MYSQL_USERNAME = "root";
  val MYSQL_PWD = "root";
  val MYSQL_CONNECTION_URL =
    "jdbc:mysql://localhost:3306/employees?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;

  def main(args: Array[String]) {
    try {

      val sc = new SparkContext("local", "Export to spark")
      val sqlContext = new SQLContext(sc)
      //Sample data-frame loaded from a JSON file
      val usersDf = sqlContext.jsonFile("src/main/resources/users.json");

      //Save data-frame to MySQL (or any other JDBC supported databases)
      //Choose one of 2 options depending on your requirement (Not both).

      //Option 1: Create new table and insert all records.
      //allowExisting: If true, drop any table with same name. If false and there is already a table with same name, it will throw an exception
      //usersDf.createJDBCTable(MYSQL_CONNECTION_URL, "sparkusers", true);

      //Option 2: Insert all records to an existing table.
      // usersDf.insertIntoJDBC(MYSQL_CONNECTION_URL, "sparkusers", true);
      println("check database , data inserted .... ")
    } catch {
      case e: Exception => e.printStackTrace()

    }
  }
}