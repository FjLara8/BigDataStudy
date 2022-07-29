package org.example

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Column


object prueba01{
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Ejcsv")
      .getOrCreate()


    // al importar un fichero para las opciones usamos option para cada una
    val dfSimpsons = spark.read.option("header",true).options(Map("delimiter"->",")).csv("simpsons(1).csv")

    val reduccion = dfSimpsons.select(col = "id", "title","season")

    reduccion.show()

    //val dffinalsave = reduccion.filter(array_contains(column = "id", value = "10"))
    //reduccion.write.csv("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/SalidaCsV")

    //val df = spark.createDataFrame(rdd)
    //reduccion.write.format("csv").mode("overwrite").save("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/SalidaCsV")


    println {
      //contamos las filas
      dfSimpsons.count()
    }

    //dfSimpsons.select($"_1".alias("nombre")) no funciona

    // mostrar las columnas concretas.
    dfSimpsons.select(col = "_c1", "_c4").show()

    dfSimpsons.show(10)

    //tipo mde datos del dataframe
    dfSimpsons.printSchema()




  }
  /*def dataensayo(): Unit ={

    val datos = spark.read.tar("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/shakespeare.tar")

    datos.show()
  }*/
}
