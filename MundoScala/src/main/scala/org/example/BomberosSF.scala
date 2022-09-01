package org.example

import org.apache.spark.sql.{Column, SparkSession, functions => F}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.types.{BooleanType, DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.catalyst.expressions.Expression

object BomberosSF {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("BomberosSF")
      .getOrCreate()

    //Forma de leer un csv crear un datafreame sin crear Schema usando samplinRatio
    val fireDF = spark.read
      .option("samplingRatio", 0.001)
      .option("header", true)
      .csv("sf-fire-calls.csv")

    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)))

    val fireDF2 = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv("sf-fire-calls.csv")

    //dataDF.show()

    //Mostramos las intervenciones diferentes a indicente medico
    /*val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")
    fewFireDF.show(5, false)

    //Mostramos las CANTIDAD de diferentes causas de llamadas
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")
      .show()

    //Listamos las diferentes causas
    val listarDF = fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      listarDF.show(10, false)*/

    //Cambiar el nombre de una columna usando withColumnRenamed Delay pasa a ResponseDelay...
    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where(col("ResponseDelayedinMins") > 5) // seleccionamos los de valor menor que 5
      .show(5, false)

    //Cambiar el tipo de dato de una columna. y renombramos la columna
    /*fireDF
      .select("CallDate", "WatchDate", "AvailableDtTm")
      .show(5, false)*/

    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")) //renombramos la columna
      .drop("CallDate")   // marca la columna que cambiamos
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    /*fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)*/

    //Devido a este cambio ya podemos interactuar con las fechas usando Years Moth o Day
    // Usamos Year para ver lso diferentes años de los que hay datos.
    /*fireTsDF
      .select(year(col("IncidentDate")))
      .distinct()
      .orderBy(year(col("IncidentDate")))
      .show()*/

    // Indicar los tipos de llamada que mas se repiten
    /*fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)*/

    //Operaciones de min max y media
    /*fireTsDF
      .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
      .show()*/

    //Cremaos un DF que contenga solo los datos de 2018
    val fire18DF = fireTsDF
      .select("*")
      .where(year(col("IncidentDate")) === 2018)

    // Tipos de llamadas realizadas en 2018
    /*fire18DF
      .select(col("CallType"))
      .distinct()
      .show(10,false)*/

    //Agrupamos por meses para ver el mes con mas llamadas
    /*fire18DF
      .select(month(col("IncidentDate"))as("meses"))
      .groupBy("meses")
      .count()
      .orderBy(desc("count"))
      .show(1)

    // Agrupamos por barrio apra ver en el que producen mas llamadas
    fire18DF
      .select(col("Neighborhood"))
      .groupBy("Neighborhood")
      .count()
      .orderBy(desc("count"))
      .show(5)*/

    // analizamos los barrios donde tardan mas en llegar
    fire18DF
      .select("ResponseDelayedinMins","Neighborhood")
      .groupBy("Neighborhood")
      .agg(sum("ResponseDelayedinMins"))
      .orderBy(desc("sum(ResponseDelayedinMins)"))
      .show(5,false)


    // Analizamos la cantidad de llamadas por la semana en la que han ocurrido.
    fire18DF
      .select(col("OnWatchDate" ))
      .groupBy(weekofyear(col("OnWatchDate")))
      .count()
      .orderBy(desc("count"))
      .show()


    // ver si tiene relacion el barrio, el codigo postal y el numero de llamadas.
    fire18DF
      .select(col("Neighborhood"),col("IncidentDate"),col("Zipcode"))
      .where(col("Neighborhood").isNotNull)
      .groupBy( "Neighborhood","Zipcode")
      .count()
      .orderBy(desc("count"))
      .show()

    //Para calcular la correlacion entre varios datos, no funciona con string
    val correlacion = fireDF2
      .select(col("Neighborhood"),col("Zipcode"))
      .groupBy(col("Neighborhood"),col("Zipcode"))
      .count()
      .stat.corr("count", "Zipcode")

    println("La correlación es de:" + correlacion)

    /*¿Cómo podemos utilizar archivos Parquet o tablas SQL para almacenar estos datos y
    leerlos de nuevo?

    Creando el archivo con las tablas resultantes.
     */

    // todo Guardar una tabla con el contenido de dataDF con la extension parquet
    /* val parquetPath = "C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Recursos/Bomberos"
    dataDF.write.format("parquet").save(parquetPath)

    dataDF.write.format("parquet").saveAsTable(parquetPath) */// parecido al anterior*/


    // Leer un csv y crear un DataFrame usando un Schema definido.
    /*val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)))

    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv("sf-fire-calls.csv")

    fireDF.show()
    fireDF.printSchema()*/








  }
}
