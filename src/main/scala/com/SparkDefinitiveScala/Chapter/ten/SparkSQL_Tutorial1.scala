package com.SparkDefinitiveScala.Chapter.ten

import com.SparkDefinitiveScala.Chapter.nine.Context

object SparkSQL_Tutorial1 extends App with Context{
  val csvFile = spark.read.format("csv")
    .option("inferSchema",value = true).option("mode","FAILFAST")
    .option("header",value = true)
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.csv")
  csvFile.createOrReplaceTempView("csvtempview")

  spark.sql("select * from csvtempview").where("count > 5 and count < 25").show(10,truncate =false)
  spark.sql("select sum(count),DEST_COUNTRY_NAME from csvtempview group by DEST_COUNTRY_NAME order by sum(count) desc")
    .show(10,truncate =false)

  spark.sql("select * from csvtempview where ORIGIN_COUNTRY_NAME in (select DEST_COUNTRY_NAME from csvtempview group by DEST_COUNTRY_NAME order by sum(count) desc limit 5)")
    .show(10,truncate = false)
  spark.sql(
    """select * from csvtempview c1 where
      |exists (select 1 from csvtempview c2 where c1.DEST_COUNTRY_NAME = c2.ORIGIN_COUNTRY_NAME)
      |and exists (select 1 from csvtempview c2 where c1.ORIGIN_COUNTRY_NAME = c2.DEST_COUNTRY_NAME)""".stripMargin)
    .show(5,truncate =false)

  spark.sql("select * from csvtempview where DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME").show(10)
}
