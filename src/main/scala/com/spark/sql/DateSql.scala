package com.spark.sql

import com.spark.driver.SparkDriver

object DateSql {
  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("date sql")
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val df = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    df.registerTempTable("dates")

    sparkSession.sql("""SELECT date_string,
        from_unixtime(unix_timestamp(date_string,'MM/dd/yyyy'), 'EEEEE') AS dow
      FROM dates""").collect.foreach(println)
  }
}