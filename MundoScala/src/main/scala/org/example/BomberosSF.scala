package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}

object BomberosSF {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("BomberosSF")
      .getOrCreate()

    //Forma de leer un csv crear un datafreame sin crear Schema usando samplinRatio
    val dataDF = spark.read
      .option("samplingRatio", 0.001)
      .option("header", true)
      .csv("sf-fire-calls.csv")

    dataDF.show()

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


    fireDF.show()*/

    //Guardar una tabla con el contenido de dataDF con la extension parquet
   /* val parquetPath = "C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Recursos/Bomberos"
    dataDF.write.format("parquet").save(parquetPath)

    dataDF.write.format("parquet").saveAsTable(parquetPath) */// parecido al anterior

    val fewFireDF = dataDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where($"CallType" != "Medical Incident")
    fewFireDF.show(5, false)


  }
}
