package com.spark.sql

import org.apache.spark.SparkContext
    import org.apache.spark.sql._
object Sum {
def main(){
  def DATA_FILE="src/main/resources/recordOne.txt"
   val sc = new SparkContext("local", "Sum")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    val file = sc.textFile(DATA_FILE, 2)
 
    val fmt=new java.text.SimpleDateFormat("yyyymmdd")
 
    def pLines(lines:Iterator[String])={
      lines.map(r=>{
      })
    }
   // val data = file.mapPartitions(_.split(",")).map(p => Row(p(0), p(1).trim))
    import org.apache.spark.sql.catalyst.expressions._
   // val result = data.groupBy('id)(First('id) as 'id,  Sum('amt) as 'total)
}
}