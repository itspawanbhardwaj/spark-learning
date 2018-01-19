package com.dataframe.operations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object AnalyzeData {

  def main(args: Array[String]) {
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val dataframe = DataframeHelper.getDataframe(sparkSession, "src/main/resources/applestock.csv")

    println("Lets see the sample data , how the data looks like")
    dataframe.show

    println("the data dosnt seems to be sorted , lets see the sorted data")
    dataframe.orderBy("timestamp").show(10)

    println("1.total no of records")
    println(dataframe.count) //7200000

    println("2. No of rows refer to mobile")

    //The ``where()`` clause is equivalent to ``filter()``.

    println(dataframe.filter("site = 'mobile'").count) //3600000

    println("3. No of rows refer to desktop")
    println(dataframe.filter("site = 'desktop'").count) //3600000

    println("4. how many total incoming request were to the mobile site vs desktop site")

    println("total request: ")
    dataframe.select(sum("requests")).collect.foreach(println)

    println("total mobile request : ")
    dataframe.filter("site = 'mobile'").select(sum("requests")).show(10)

    println("total desktop request : ")
    dataframe.filter("site = 'desktop'").select(sum("requests")).show(10)

    println("5. what is the start and end range of time for the page views data ?  How many days of data do we have ? ")

    import sparkSession.implicits._

    val pageViewsDF2 = dataframe.select($"timestamp".cast("timestamp").alias("timestamp"), $"site", $"requests")

    pageViewsDF2.printSchema

    pageViewsDF2.select(year($"timestamp")).distinct.show(10)

    println("We have data for only 2015 year, lets check how many months are there")

    pageViewsDF2.select(month($"timestamp")).distinct.show(10)

    println("lets check how many weeks are there")

    pageViewsDF2.select(weekofyear($"timestamp")).distinct.show(10)

    println("lets check how many days are there")
    pageViewsDF2.select(dayofyear($"timestamp")).distinct.show(10)

    println("statistics of mobile and desktop requests")

    pageViewsDF2.filter("site = 'mobile'").select(max("requests"), min("requests"), avg("requests")).show()

    pageViewsDF2.filter("site = 'desktop'").select(max("requests"), min("requests"), avg("requests")).show()

    val df3 = pageViewsDF2.groupBy(date_format($"timestamp", "E").alias("day of week")).sum().orderBy("day of week").cache
    df3.show

    /*
|day of week|sum(requests)|
+-----------+-------------+
|        Fri|   1842512718|
|        Mon|   2356818845|
|        Sat|   1662762048|
|        Sun|   1576726066|
|        Thu|   1931508977|
|        Tue|   1995034884|
|        Wed|   1977615396|
+-----------+-------------+
*/

    println("The weeks are not in order , lets create a UDF to order day of week")

    def matchDayOfWeek(day: String): String = {
      day match {
        case "Mon" => "1-Mon"
        case "Tue" => "2-Tue"
        case "Wed" => "3-Wed"
        case "Thu" => "4-Thu"
        case "Fri" => "5-Fri"
        case "Sat" => "6-Sat"
        case "Sun" => "7-Sun"
        case _ => ""
      }

    }

    println("register UDF in sqlContext")

    val previewNoUDF = sparkSession.udf.register("matchDayOfWeeks", { (* : String) => matchDayOfWeek(*) })

    println("Select and order by 'day of week' using UDF")

    // import sqlContext.implicits._

    val df4 = df3.withColumnRenamed("sum(requests)", "total requests")
      .select(previewNoUDF($"day of week"), $"total requests").orderBy(previewNoUDF($"day of week"))

    df4.show

    val df5 = pageViewsDF2.groupBy(date_format($"timestamp", "D").alias("day of year")).sum().orderBy("sum(requests)")
    df5.show
  }

}