package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object SQLspark3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SQL2")
      .getOrCreate()


    val namesquema = "id:Temporada:Jornada:EquipoL:EquipoV:GolL:GolV:Fecha:Timestamp"
    // creamos una structura
    val squema2 = StructType(namesquema.split(":").map(StructField(_, dataType = StringType, nullable = true)))

    // val schema = ScalaReflection.schemaFor[Empleados].dataType.asInstanceOf[StructType]

    val rddbase = spark.read.options(Map("delimiter" -> "::")).csv("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/DataSetPartidos.txt")

    val dfesquema = spark.createDataFrame(rddbase.rdd, squema2)

    dfesquema.createOrReplaceTempView("dfVista")

    dfesquema.show()

    spark.sql("SELECT Temporada, SUM(GolV) FROM dfVista Where EquipoV = 'Real Oviedo' GROUP BY Temporada ORDER BY SUM(GolV) Desc ").show(1)

    //todo esto hay que mejorarlo peta la salida de datos
    //spark.sql("SELECT Count('Temporada'), EquipoV FROM dfVista Where EquipoV = 'Real Oviedo'").show()

    //spark.sql("""SELECT  count(distinct(Temporada)) as idcnt FROM dfVista """).show()

    // Es necesario el Group By y no poner "" con el distinct
    spark.sql("""SELECT Count(distinct (Temporada)),EquipoV From dfVista Where EquipoV = 'Real Oviedo' OR EquipoV = 'Sporting de Gijon' GROUP BY EquipoV """).show()

  }


}
