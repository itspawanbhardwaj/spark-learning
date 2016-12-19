/*package com.ideata.bda.ingest.spark.ingest

import org.apache.spark.rdd.RDD
import scala.xml.Node  
import scala.util.control.Exception 
import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD
import java.sql.ResultSet 
import org.apache.spark.streaming.dstream.DStream 
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.streaming.StreamingContext
import scala.util.Try
import java.sql.Connection 
import scala.collection.mutable.LinkedList 
import scala.collection.mutable.HashMap 
import java.sql.SQLException
import org.apache.spark.sql.Row

class JDBCIngest @throws(classOf[Exception]) extends AbstractIngest {
  @throws(classOf[Exception])
  override def ingest(): RDD[SparkRecord] = {
    try {
      val sc = getContext()
      var sql = if (!ingestProperties.contains("sql") ||
        ingestProperties.get("sql").get.length() == 0) {
        throw new IngestException(Constants.JDBC_EXCEPTION)
      } else
        ingestProperties.get("sql").get
      //sql = "(" + sql + ") as sampleTableIdeata"

      val driverClassName = if (!ingestProperties.contains(Constants.driverClassName) ||
        ingestProperties.get(Constants.driverClassName).get.length() == 0) {
        throw new IngestException(Constants.JDBC_DRIVER_EXCEPTION)
      } else
        ingestProperties.get(Constants.driverClassName).get

      val schema = if (!ingestProperties.contains(Constants.schema) ||
        ingestProperties.get(Constants.schema).get.length() == 0) {
        throw new IngestException(Constants.JDBC_URL_EXCEPTION)
      } else
        ingestProperties.get(Constants.schema).get

      val dbUrl = if (!ingestProperties.contains(Constants.dbUrl) ||
        ingestProperties.get(Constants.dbUrl).get.length() == 0) {
        throw new IngestException(Constants.JDBC_URL_EXCEPTION)
      } else
        ingestProperties.get(Constants.dbUrl).get + "?zeroDateTimeBehavior=convertToNull"

      if (columnList == null || columnList.size == 0) {
        getColumnList(null).foreach(column => {
          val columnType = new ColumnType()
          columnType.tableName = ingestProperties.get(Constants.table).get
          columnType.columnName = column._1 //.split("\\.").tail.mkString(".")
          columnType.dataType = column._2
          columnType.originalColumnName = column._1
          columnList = columnList ++ LinkedList(columnType)
        })
      }
      var isMysql = false

      val username = ingestProperties.get(Constants.username).getOrElse("")
      val password = ingestProperties.get(Constants.password).getOrElse("")
      val partition = ingestProperties.get(Constants.partition).getOrElse("20").toString().toInt //partition 

      val tables = ingestProperties.get(Constants.table).get.split(",").toList //(0)
      val connectionProperties = new java.util.Properties
      connectionProperties.setProperty("user", username)
      connectionProperties.setProperty("password", password)
      connectionProperties.setProperty("numPartitions", "10")

      val partitionInfo: (String, String, String, Long, Long) = if (ingestProperties.get(Constants.dbType).getOrElse("mysql").equalsIgnoreCase("mysql")) {
        var options: Map[String, String] = Map()
        options += ("driver" -> "com.mysql.jdbc.Driver");
        val urll = dbUrl + "&user=" + username + "&password=" + password + ""
        options += ("url" -> urll)
        //        val df = SparkDriver.getSqlContext.read.jdbc(urll, "call_center_dimension", connectionProperties)
        //        println(df.rdd.map(row => {
        //          1
        //        }).sum)

        getPartitionInfo(options, schema, tables) //partition
      } else
        (null, null, null, 0l, 0l)

      if (partitionInfo._3 != null)
        sql = sql.replace("? = ?", partitionInfo._2 + "." + partitionInfo._3 + " > ? and " + partitionInfo._2 + "." + partitionInfo._3 + " < ?")

      val rdd: JdbcRDD[SparkRecord] = new JdbcRDD(
        sc, () => {
          Class.forName(driverClassName)
          DriverManager.getConnection(dbUrl, ingestProperties.get(Constants.username).getOrElse(""),
            ingestProperties.get(Constants.password).getOrElse(""))
        },
        sql,
        partitionInfo._4, partitionInfo._5, partition,
        (rs: ResultSet) => {
          val columnName = columnList.filter(p => !p.isEnhanced).map(col => col.columnName)
          val originalColumnName = columnList.filter(p => !p.isEnhanced).map(col => col.originalColumnName)
          var keyMap = new mutable.LinkedHashMap[String, Any]
          try {
            for (x <- 0 to columnName.length - 1) {
              if (originalColumnName(x).split("\\.").size > 1)
                keyMap.put(columnName(x), rs.getString(originalColumnName(x).split("\\.")(1)))
              else
                keyMap.put(columnName(x), rs.getString(originalColumnName(x)))
            }
          } catch {
            case e: Throwable =>
              log.error(e.getLocalizedMessage(), e)
              throw new IngestException(e.getLocalizedMessage())
          }
          new SparkRecord(System.currentTimeMillis() / 1000, keyMap)
        })

      //      println("")
      //      println(rdd.map(row => {
      //        1
      //      }).sum)
      rdd

    } catch {
      case e: Throwable => {
        //        e.printStackTrace()
        log.error(e.getLocalizedMessage(), e)
        throw new IngestException(e.getLocalizedMessage())
      }
    }

  }

  override def streamingIngest(ssc: StreamingContext): DStream[SparkRecord] = {
    throw new IngestException("streamingIngest(DStream[SparkRecord]) method not supported for JDBC")
  }

  @throws(classOf[Exception])
  override def getPreviewData(properties: Map[String, String], size: Int): (RDD[SparkRecord], mutable.LinkedHashMap[String, String]) = {
    if (properties != null)
      parseConfig(properties)
    (ingest(), getColumnList(properties))
  }

  override def getSchemaList(properties: Map[String, String]): ArrayBuffer[String] = {
    if (properties != null)
      parseConfig(properties)
    execute(getSchemaSql(ingestProperties.get(Constants.dbType).get))
  }

  override def getTableList(properties: Map[String, String]): ArrayBuffer[String] = {
    if (properties != null)
      parseConfig(properties)
    val databaseType = ingestProperties.get(Constants.dbType).getOrElse("mysql")
    if (databaseType == "hive")
      execute(getTableSql(databaseType))
    else
      execute(getTableSql(databaseType)
        .replaceFirst("\\?", ingestProperties.get(Constants.schema).getOrElse("mysql")))
  }

  def getPartitionInfo(options: Map[String, String], schema: String, tables: List[String]): (String, String, String, Long, Long) = {
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
      val jdbcDF = SparkDriver.getSqlContext().load("jdbc", keyOptions)
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
        val jdbcDF = SparkDriver.getSqlContext().load("jdbc", keyOptions)
        val minMax = jdbcDF.first
        (schemaName, tableName, result, minMax(0).toString.toLong, minMax(1).toString.toLong)
      } else {
        getPartitionInfo(options, schema, tables.tail)
      }

    }
  }

  override def getColumnList(properties: Map[String, String]): mutable.LinkedHashMap[String, String] = {
    if (properties != null)
      parseConfig(properties)
    val resultMap = new mutable.LinkedHashMap[String, String]
    try {
      //In case we have multiple tables in the same query, 
      //we will get schema and tables as comma seperated values
      val tableList = ingestProperties.get(Constants.table).get.split(",")
        .map(table => {
          val splits = table.split("\\.")
          if (splits.length < 2)
            throw new IngestException("Provided tablename is incorrect: " + table)
          (splits(0), splits(1))
        })
      //Foreach schema.table, we will get the list of columns and add them to resultMap
      tableList.foreach(table =>
        execute(getColumnSql(ingestProperties.get(Constants.dbType).get)
          .replaceFirst("\\?", table._1)
          .replaceFirst("\\?", table._2))
          .foreach(row => resultMap.put(table._2 + "." + row.split("\\?")(0), row.split("\\?")(1))))
    } catch {
      case e: Exception => {
        log.error(e.getLocalizedMessage(), e)
        throw new Exception(e.getLocalizedMessage())
      }
    }
    resultMap
  }

  private def execute(sql: String): ArrayBuffer[String] = {
    val schema = new ArrayBuffer[String]
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(
        ingestProperties.get(Constants.dbUrl).getOrElse(throw new IngestException("Please provide dbUrl property to read data from database")),
        ingestProperties.get(Constants.username).getOrElse(""),
        ingestProperties.get(Constants.password).getOrElse(""))
      val result = conn
        .createStatement()
        .executeQuery(sql)
      val colSize = result.getMetaData().getColumnCount()
      while (result.next()) {
        var record = result.getString(1)
        if (colSize > 1)
          record = record + "?" + result.getString(2)
        schema += record
      }

    } catch {
      case e: Exception => {
        log.error(e.getLocalizedMessage(), e)
        throw new Exception(e.getLocalizedMessage())
      }
    } finally {
      if (conn != null)
        conn.close()
    }
    schema
  }

  private def getSchemaSql(dbType: String): String = {
    dbType match {
      case "mysql" => Constants.mySqlGetSchemaQuery
      case "mssql" => Constants.msSqlServerGetSchemaQuery
      case "oracle" => Constants.oracleGetSchemaQuery
      case "teradata" => Constants.teradataGetSchemaQuery
      case "postgres" => Constants.postgresGetSchemaQuery
      case "hive" => Constants.hiveGetSchemaQuery
    }
  }

  private def getTableSql(dbType: String): String = {
    dbType match {
      case "mysql" => Constants.mySqlGetTableQuery
      case "mssql" => Constants.msSqlServerGetTableQuery
      case "oracle" => Constants.oracleGetTableQuery
      case "terdata" => Constants.teradataGetTableQuery
      case "postgres" => Constants.postgresGetTableQuery
      case "hive" => Constants.hiveGetTableQuery
    }
  }

  private def getColumnSql(dbType: String): String = {
    dbType match {
      case "mysql" => Constants.mySqlGetColumnQuery
      case "mssql" => Constants.msSqlServerGetColumnQuery
      case "oracle" => Constants.oracleGetColumnQuery
      case "teradata" => Constants.teradataGetColumnQuery
      case "postgres" => Constants.postgresGetColumnQuery
      case "hive" => Constants.hiveGetColumnQuery
    }
  }

}*/