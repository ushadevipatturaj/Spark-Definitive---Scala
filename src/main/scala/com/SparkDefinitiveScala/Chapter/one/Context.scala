package com.SparkDefinitiveScala.Chapter.one

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkConf:SparkConf = new SparkConf().setAppName("spark-definitive-scala").setMaster("local[4]").set("spark.cores.max","2")
  lazy val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

}
