package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SQLspark2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SQL2")
      .getOrCreate()


    //creamos un squema para poder introducir nuestro fichero
    val namesquema = "id:name:age"
    // creamos una structura
    val squema2 = StructType(namesquema.split(":").map(StructField(_,dataType = StringType, nullable = true)))

    // val schema = ScalaReflection.schemaFor[Empleados].dataType.asInstanceOf[StructType]

    val rddbase = spark.read.options(Map("delimiter"->",")).csv("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/empleado.txt")

    val dfesquema = spark.createDataFrame(rddbase.rdd, squema2)

    dfesquema.createOrReplaceTempView("dfVista")

    dfesquema.show()


    spark.sql("SELECT id FROM dfVista").show()




  }
}
