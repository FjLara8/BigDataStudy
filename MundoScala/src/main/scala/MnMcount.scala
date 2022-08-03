package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("MnMCount")
      .getOrCreate()

    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("mnm_dataset.csv")



    // Aggregate counts of all colors and groupBy() State and Color
    // orderBy() in descending order
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for all the states and colors
    countMnMDF.show(10)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    val maxMnMDF = mnmDF
      .select( "State","Color", "Count")
      .groupBy("State","Color")
      .agg(avg("Count").alias("Media"))
      .orderBy((asc("Media")))

    maxMnMDF.show(10)


    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(max("Count"),min("Count"),avg("Count"),count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for California
    caCountMnMDF.show(10)
    // Stop the SparkSession
    spark.stop()
  }


}
