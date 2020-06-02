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

  //reduce
  val sum = spark.sparkContext.parallelize(1 to 20).reduce(_ + _)
  println(s"sum is $sum")

  //count and count approx
  val count_actual = rdd_spark.count()
  val count_approx = rdd_spark.countApprox(500,0.95)
  println(s"count $count_actual and count_approximate $count_approx")

  val count_distinct = rdd_spark.countApproxDistinct()
  val count_value = rdd_spark.countByValue()
  val count_value_approx = rdd_spark.countByValueApprox(500,0.95)
  println(s"count approximate distinct $count_distinct , countbyvalue $count_value and countby value approximate $count_value_approx")
}
