name := "spark-learning"

version := "1.0"

scalaVersion := "2.11.4"

val sparkVersion = "2.3.1"

resolvers ++= Seq(

  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "spark-eventhubs" at "https://mvnrepository.com/artifact/com.microsoft.azure/spark-streaming-eventhubs_2.11",
  "ms access jars" at "http://central.maven.org/maven2/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.microsoft.azure" % "spark-streaming-eventhubs_2.11" % "2.0.0",
  "com.microsoft.azure.sdk.iot" % "iot-service-client" % "1.7.23",
  "com.microsoft.azure.sdk.iot" % "iot-device-client" % "1.3.32",
  "com.microsoft.azure" % "azure-eventhubs" % "0.13.0",
  "com.google.code.gson" % "gson" % "2.8.0",
  "commons-lang" % "commons-lang" % "2.6",
  "commons-logging" % "commons-logging" % "1.1.1",
  "org.hsqldb" % "hsqldb" % "2.2.9",
  "com.healthmarketscience.jackcess" % "jackcess" % "2.1.2",
  "net.sf.ucanaccess" % "ucanaccess" % "4.0.1"
)
    
