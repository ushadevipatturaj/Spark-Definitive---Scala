package com.SparkDefinitiveScala.Chapter.nine

import java.sql.DriverManager

object DataSources_Tutorial2 extends App with Context {
  //reading and writing to a sql table
  val driver = "org.sqlite.JDBC"
  val path = "D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\flight_data.db"
  val url = s"jdbc:sqlite:\\$path"
  val tablename = "flights"
  val connection = DriverManager.getConnection(url)
  println(connection.isClosed)
  connection.close()

  val dbTable = spark.read.format("jdbc").option("dbtable",tablename).option("url",url).option("driver",driver).load()
  dbTable.show(5)
  val pushdownQuery = """(select distinct(destination) from flights) as flight_info"""
  val dbTable_pd = spark.read.format("jdbc").option("dbtable",pushdownQuery).option("url",url).option("driver",driver).load()
  dbTable_pd.show(5)

  val dbTable_partition = spark.read.format("jdbc").option("dbtable",pushdownQuery).option("url",url).option("driver",driver)
    .option("numPartitions",10).load()
  dbTable_pd.show(5)
  val props = new java.util.Properties
  props.setProperty("driver","org.sqlite.JDBC")
  val predicates = Array(
    "destination = 'India' or origin ='India'",
    "destination = 'Croatia' or origin ='Croatia'"
  )
  spark.read.jdbc(url,tablename,predicates,props).show()
  val countPartition = spark.read.jdbc(url,tablename,predicates,props).rdd.getNumPartitions
  println(countPartition)

  val column = "count"
  val lowerBound = 0
  val upperBound = 7
  val numPartition = 10

  val parallelRead  = spark.read.jdbc(url,tablename,column,lowerBound,upperBound,numPartition,props).count()
  println(s"count of rows $parallelRead")

  //writing to a table
  val newPath = "jdbc:sqlite:\\D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\flight_data_fromcode.db"
  dbTable.write.mode("overwrite").jdbc(newPath,tablename,props)
  val count_written = spark.read.jdbc(newPath,tablename,props).count()
  println(count_written)

  val csvFile = spark.read.format("csv")
    .option("inferSchema",value = true).option("mode","FAILFAST")
    .option("header",value = true)
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.csv")

  val newCsvPath = "jdbc:sqlite:\\D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary_fromcode.db"
  csvFile.write.mode("overwrite").jdbc(newCsvPath,"RetailData",props)

  //reading and writing text files
  spark.read.textFile("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.csv")
    .selectExpr("split(value,',') as Row").show(10,truncate = false)

  csvFile.select("ORIGIN_COUNTRY_NAME").write.format("text").mode("overwrite")
    .save("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.txt")
  spark.read.textFile("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.txt").show(10)

}
