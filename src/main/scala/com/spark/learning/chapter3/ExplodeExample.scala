package com.spark.learning.chapter3

import com.spark.driver.SparkDriver
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray

object ExplodeExample {
  case class RawPanda(id: Long, zip: String, pt: String, happy: Boolean, attributes: WrappedArray[Double])
  case class PandaPlace(name: String, pandas: Array[RawPanda])

  def main(args: Array[String]) {
    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 0.1))
    val pandaPlace = PandaPlace("toronto", Array(damao))
    val spark = SparkDriver.getSparkSession("Create a Dataset with the case class")
    val df = spark.createDataFrame(Seq(pandaPlace))
    df.printSchema()

    val pandaInfo = df.explode(df("pandas")) {
      case Row(pandas: Seq[Row]) => pandas.map {
        case (Row(id: Long, zip: String, pt: String, happy: Boolean, attrs: WrappedArray[Double])) =>
          RawPanda(id, zip, pt, happy, attrs)
      }
    }

    pandaInfo.printSchema()

    val result = pandaInfo.select((pandaInfo("attributes")(0) / pandaInfo("attributes")(1)).as("squishyness"))

    result.show()
  }
}