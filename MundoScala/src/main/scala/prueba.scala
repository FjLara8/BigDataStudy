import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object prueba {

  val dfSchema = StructType(Array(StructField("COD_DISTRITO", IntegerType, true),
    StructField("DESC_DISTRITO", StringType, true),
    StructField("COD_DIST_BARRIO", IntegerType, true),
    StructField("DESC_BARRIO", StringType, true),
    StructField("COD_BARRIO", IntegerType, true),
    StructField("COD_DIST_SECCION", IntegerType, true),
    StructField("COD_SECCION", IntegerType, true),
    StructField("COD_EDAD_INT", IntegerType, true),
    StructField("EspanolesHombres", IntegerType, true),
    StructField("EspanolesMujeres", IntegerType, true),
    StructField("ExtranjerosHombres", IntegerType, true),
    StructField("ExtranjerosMujeres", IntegerType, true)))


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Mpadron6")
      .getOrCreate()

    //6.1 Se supone que con los ignore quita los espacios cargamos el csv con schema y delimitado.

    val df = spark.read.schema(dfSchema)
      .option("header", "true")
      .option("delimiter", ";")
      .option("parserLib", "univocity")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .csv("Rango_Edades_Seccion_202208.csv")
    // fill nos vale para cambiar los espacios nulos por el valor entre ()
    val df2 = df.na.fill(0)


    df2.write
      .partitionBy("DESC_DISTRITO","DESC_BARRIO")
      .mode("overwrite")
      .csv("tablas/tablacsv")

  }
}
