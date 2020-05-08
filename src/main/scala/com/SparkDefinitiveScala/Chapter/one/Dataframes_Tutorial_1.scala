package com.SparkDefinitiveScala.Chapter.one

object Dataframes_Tutorial_1 extends App with Context {
  val dfRange = sparkSession.range(1000).toDF("Number")
  val evenNum = dfRange.where("Number % 2 =0")
  println("The count of all even values are "+evenNum.count())

}
