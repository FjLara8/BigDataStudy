package org.example

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object prueba01{
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Ejcsv")
      .getOrCreate()

    val dfSimpsons = spark.read.csv("simpsons(1).csv")

    val reduccion = dfSimpsons.select(col = "id", "title","season")

    reduccion.show()



  }
}
