package org.example

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object DataSet01 {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Ejcsv")
      .getOrCreate()



    /*val datos = spark.read.textFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/pruebatx.txt")

    val palabras = datos.flatMap(fun => fun.split(" "))*/



    //val conPalabras = palabras.countByValue
    //palabras.show()

    //datos.show()






  }


}
