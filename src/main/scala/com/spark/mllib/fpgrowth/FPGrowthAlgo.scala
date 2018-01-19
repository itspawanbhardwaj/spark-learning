package com.spark.mllib.fpgrowth
//http://stackoverflow.com/questions/34150547/spark-group-concat-equivalent-in-scala-rdd
import scala.Array.canBuildFrom
import scala.util.hashing.MurmurHash3
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.Dataset

object FPGrowthAlgo {
  case class AssocoationClass(antecedent: Array[String], consequent: String, confidence: Double)
  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)
  case class TransactionProduct(transactionId: String, product: String)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    /*
      * with transactio id : 
        */
    val config: Map[String, Any] =
      Map("transactionId" -> "_c0",
        "product" -> "_c1",
        "separator" -> ",",
        "transactionIdExists" -> "true",
        "fileLocation" -> "src/main/resources/association-rule.txt")

    /*without transaction id*/
    /*val config: Map[String, Any] =
      Map("transactionId" -> "_c0",
        "product" -> "value",
        "separator" -> ",",
        "transactionIdExists" -> "false",
        "fileLocation" -> "src/main/resources/sample_fpgrowth.txt")*/

    val df = getDataframe(config, sparkSession)

    import org.apache.spark.sql.functions._
    import sparkSession.implicits._

    val dataset = executeFpGrowth(config, df, sparkSession)

    println("association rules")
    dataset.show
    println("get consequent");
    getConsequent(dataset, "Pen,Beans").show

    println("get antecedent");
    val AntecedentDS = getAntecedent(dataset, "Banana")

    AntecedentDS.show

    val arrayAntecedent = AntecedentDS.map(rows => rows.antecedent).collect
    getUsers(config, df, sparkSession, arrayAntecedent)
    // df.as("df1").join(AntecedentDS.as("df2"), $"df1.foo" === $"df2.foo")
    ///get
  }

  def getUsers(config: Map[String, Any], df: DataFrame, sparkSession: SparkSession, arrayAntecedent: Array[Array[String]]) {

    val transactionIdExists = config.get("transactionIdExists").getOrElse("false").toString
    if (transactionIdExists.equalsIgnoreCase("true")) {
      import org.apache.spark.sql.functions._
      import sparkSession.implicits._
      println("transactionDS");
      val transactionDS = df.select("transactionId", "product").as[TransactionProduct]

      transactionDS.show

      println("get users : ");
      import scala.util.control.Breaks._
      transactionDS.filter(rows => {

        var isPresent = false
        breakable {
          for (i <- arrayAntecedent) {
            //println((rows._c1.split(",").sameElements(i)))
            if (rows.product.split(",").sameElements(i)) {
              isPresent = true
              break;
            }
          }
        }

        isPresent
      }).show
    }

  }

  def getDataframe(config: Map[String, Any], sparkSession: SparkSession): DataFrame = {
    val transactionIdExists = config.get("transactionIdExists").getOrElse("false").toString
    val fileLocation = config.get("fileLocation").getOrElse(throw new Exception("file location not found")).toString
    import sparkSession.implicits._
    if (transactionIdExists.equalsIgnoreCase("false")) {
      sparkSession.read.textFile(fileLocation).toDF
    } else {
      import org.apache.spark.sql.functions._
      val df = sparkSession.read.csv(fileLocation)

      val result = df.groupBy($"_c0".as("transactionId")).agg(concat_ws(",", collect_list("_c1")).as("product"))

      /*df.registerTempTable("data")

      val result = sparkSession
        .sql("select _c0 as transactionId  ,concat_ws(',', collect_list(_c1)) as product from data group by _c0")
*/
      result
    }

  }

  def getConsequent(dataset: Dataset[AssocoationClass], antecedent: String): org.apache.spark.sql.Dataset[AssocoationClass] = {
    dataset.filter(_.antecedent.sameElements(antecedent.split(",").map(value => value.trim)))
  }

  def getAntecedent(dataset: Dataset[AssocoationClass], consequent: String): org.apache.spark.sql.Dataset[AssocoationClass] = {
    dataset.filter(_.consequent.equalsIgnoreCase(consequent.trim()))
  }

  def executeFpGrowth(config: Map[String, Any], df: DataFrame, sparkSession: SparkSession): Dataset[AssocoationClass] = {

    val transactionIdExists = config.get("transactionIdExists").getOrElse("false").toString

    val product = config.get("product").getOrElse(throw new Exception("Product column not found")).toString
    val separator = config.get("separator").getOrElse(",").toString

    val minSupport = config.get("minSupport").getOrElse("0.2").toString.toDouble

    val numPartition = config.get("numPartition").getOrElse("10").toString.toInt

    val minConfidence = config.get("minConfidence").getOrElse("0.5").toString.toDouble
    println(transactionIdExists);
    val transactions = if (transactionIdExists.equalsIgnoreCase("true")) {

      df.select("product")
        .rdd.map(s => s(0).toString.trim().split(separator).distinct)
    } else {
      df.select(product)
        .rdd.map(s => s(0).toString.trim().split(separator).distinct)
    }

    transactions
      .collect
      .foreach(value => println(JavaConversions.asJavaList(value)))
    val fpg = new FPGrowth().setMinSupport(minSupport)
      .setNumPartitions(numPartition)
    //  transactions.collect.foreach(value => println(JavaConversions.asJavaList(value)))
    val model = fpg.run(transactions)

    val sparkContext = sparkSession.sparkContext

    import sparkSession.implicits._

    val rules = model.generateAssociationRules(minConfidence);
    //rules.
    val rows = model.generateAssociationRules(minConfidence).map(rule =>
      Row.fromSeq(Seq(rule.antecedent,
        rule.consequent.mkString(" "),
        rule.confidence)))
    val schema = StructType(Array(
      StructField("antecedent", ArrayType(StringType)),
      StructField("consequent", StringType),
      StructField("confidence", DoubleType)))
    val newDf = sparkSession.createDataFrame(rows, schema)
    val dataset = newDf.as[AssocoationClass]
    dataset
  }

}