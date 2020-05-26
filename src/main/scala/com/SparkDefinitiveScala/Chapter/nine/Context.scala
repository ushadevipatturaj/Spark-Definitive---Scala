package com.SparkDefinitiveScala.Chapter.nine

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkConf:SparkConf= new SparkConf().setMaster("local[4]").setAppName("spark-definitive-scala").set("spark.cores.max","2")
  lazy val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

}
