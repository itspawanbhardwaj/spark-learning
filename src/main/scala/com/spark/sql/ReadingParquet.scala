package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ReadingParquet {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "SchemaRdd")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.parquetFile("/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00000-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet")
    // Read in the parquet file created above.  Parquet files are self-describing so the schema is preserved.
    // The result of loading a Parquet file is also a SchemaRDD.
    /*val parquetFile = sqlContext.parquetFile("people.parquet","src/main/resources/peopleTwo.parquet")

    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("parquetFile")
    
    val peopleData = sqlContext.sql("SELECT name FROM parquetFile")
    peopleData.map(t => "Name: " + t(0)).collect().foreach(println)*/

   /* val parquetFile = sqlContext.parquetFile("/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00000-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00001-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00002-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00003-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00004-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00005-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00006-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00007-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00008-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet",
      "/home/ideata/setup/sampledata/parquet/ideata_wedfe_2/part-r-00009-e86ef23a-8dcb-4022-b22f-336e72d2046c.gz.parquet")
    parquetFile.rdd.repartition(1).saveAsTextFile("/home/ideata/setup/sampledata/csv/test1.csv")
    */
    val hadoopConfig = new Configuration()
val hdfs = FileSystem.get(hadoopConfig)
FileUtil.copyMerge(hdfs, new Path("/home/ideata/setup/sampledata/parquet/ideata_wedfe_2"), hdfs, new Path("/home/ideata/setup/sampledata/csv/testHadoop.csv"), false, hadoopConfig, null)

  }
}