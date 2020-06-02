package com.SparkDefinitiveScala.Chapter.twelve

import com.SparkDefinitiveScala.Chapter.nine.Context

object LowlevelRDD_Tutorial1 extends App with Context{
  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
    .split(" ")
  val rdd_spark = spark.sparkContext.parallelize(myCollection,2)
  val count_val = rdd_spark.distinct().count()
  println(count_val)
  //filtering
  val rdd_S = rdd_spark.filter(row => row.startsWith("S"))
  println(rdd_S.take(10).mkString(","))
  //flatmap
  println(rdd_spark.flatMap(row => row.toSeq).take(10).mkString(","))
  //sorting
  println(rdd_spark.sortBy(word => word.length() * -1).take(10).mkString(","))
  //randomsplit
  val split_val = rdd_spark.randomSplit(Array[Double](0.5,0.5)).toSeq
  println(split_val)

}
