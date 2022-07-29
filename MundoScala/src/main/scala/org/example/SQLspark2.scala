package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json4s.scalap.scalasig.ClassFileParser.{attributes, fields}


object SQLspark2 {

  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SQL2")
    .getOrCreate()

    //val sc = new SparkContext()





    // todo primer intento
    /*spark.sql("CREATE DATABASE IF NOT EXISTS Ramon")

    spark.sql("CREATE TABLE IF Ramon.empleados(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/empleado.txt' INTO TABLE hivespark.empleados")

    var query1=spark.sql("SELECT * FROM hivespark.empleados").show()*/


    // todo forma 2

    //case class Empleados (id: Int, name:String, age:Int)

    //creamos un squema para poder introducir nuestro fichero
    val namesquema = "id:name:age"
    // creamos una structura
    val squema2 = StructType(namesquema.split(":").map(StructField(_,dataType = StringType, nullable = true)))

    // val schema = ScalaReflection.schemaFor[Empleados].dataType.asInstanceOf[StructType]

    val rddbase = spark.read.options(Map("delimiter"->",")).csv("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/empleado.txt")

    val dfesquema = spark.createDataFrame(rddbase.rdd, squema2)

    dfesquema.show()

    spark.sql("SELECT id FROM dfesquema").show()




    // todo formas 3
    /*val rddbase = spark.read.textFile("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/empleado.txt")

    val nombrecolum = "nombre edad"

    val schemaString = nombrecolum.split(" ")
      .map(fieldname => StructField(fieldname, StringType, nullable = true ))
    val schema = StructType(fields)

    val rowrdd = rddbase
      .map(_.split(","))
      .map(attributes=> Row(attributes(0),attributes(1),trim))

    val peopleDF = spark.createDataFrame(rowrdd,schema)*/

    //todo forma 4


    //val rddbase = spark.read.option("header","false").textFile("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/empleado.txt")

    /*val recurso = sc.textFile("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/empleado.txt")

    case class Empleados (id: Int, name: String, age:Int)
    val empleadosdb = recurso.map(x =>x.split(",")).map{case Array(id, name,age) => Empleados(id.toInt, name.toString, age.toInt)}

    val empleadosreal = empleadosdb.toDF()

    empleadosreal.show()*/


  }
}
