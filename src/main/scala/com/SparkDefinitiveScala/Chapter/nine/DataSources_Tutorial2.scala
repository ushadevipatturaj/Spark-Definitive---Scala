package com.SparkDefinitiveScala.Chapter.nine

import java.sql.DriverManager

object DataSources_Tutorial2 extends App with Context {
  //reading and writing to a sql table
  val driver = "org.sqlite.JDBC"
  val path = "D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\flight_data.db"
  val url = s"jdbc:sqlite:\\${path}"
  val tablename = "flights"
  val connection = DriverManager.getConnection(url)
  println(connection.isClosed)
  connection.close()

  val dbTable = spark.read.format("jdbc").option("dbtable",tablename).option("url",url).option("driver",driver).load()
  dbTable.show(5)
  val pushdownQuery = """(select distinct(destination) from flights) as flight_info"""
  val dbTable_pd = spark.read.format("jdbc").option("dbtable",pushdownQuery).option("url",url).option("driver",driver).load()
  dbTable_pd.show(5)

}
