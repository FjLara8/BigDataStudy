import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, desc, length, lit, round, sum, trim}
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}

object padron6 {

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
      .master("local[1]")
      .appName("MnMCount")
      .getOrCreate()

    //6.1 Se supone que con los ignore quita los espacios cargamos el csv con schema y delimitado.

    val df = spark.read.schema(dfSchema)
      .option("header", "true")
      .option("delimiter", ";")
      .option("parserLib","univocity")
      .option("ignoreLeadingWhiteSpace",true)
      .option("ignoreTrailingWhiteSpace",true)
      .csv("Rango_Edades_Seccion_202208.csv")
    // fill nos vale para cambiar los espacios nulos por el valor entre ()
    val df2 = df.na.fill(0)

    df2.show(10, false)


    //6.2 En este caso usamos Trim para quitar los espacios en blanco, aqui si funciona

    val dfpadron = spark.read.schema(dfSchema)
      .option("header", "true")
      .option("delimiter", ";")
      .csv("Rango_Edades_Seccion_202208.csv")
      .na.fill(0)
      .withColumn("DESC_DISTRITO", trim(col("DESC_DISTRITO")))
      .withColumn("DESC_BARRIO", trim(col("DESC_BARRIO")))


    //6.3 Enumerar los diferentes barrios
    dfpadron
      .select("DESC_BARRIO")
      .where(col("DESC_BARRIO").isNotNull)
      .distinct()
      //.show(10, false)

    // Otra forma de hacerlo
    /*dfpadron
      .select("DESC_BARRIO")
      .distinct()
      .show()*/

    //6.4 Crear vista temporal y contar el numero de barrios distintos usando la vista.
    dfpadron.createOrReplaceTempView("padron")
    spark.sql("""SELECT Count(distinct (DESC_BARRIO)) From padron """)

    //6.5 crear nueva columna mostrando la longitud de DESC_DISTRITO
    //dfpadron.withColumn("Longitud", length(col("DESC_DISTRITO"))).show()

    //6.6 Añadir columna que muestre 5 en todas las filas
    val dfValor = dfpadron.withColumn("Valor", lit(5))
   // dfValor.show()

    //6.7 Eliminar esta columna todo esta modificacion no altera la tabal en si poe eso no la muestra luego aunque si en el momento.
    dfValor.drop("Valor")
    //dfValor.show()

    //6.8 Particionar el DataFrame por DESC_DISTRITO y DESC_BARRIO
    val dfPartition = dfpadron.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))
    //dfPartition.show()

    //6.9 Guardar en cache el DataFrame particionado
    val dfPartitionCache = dfPartition.cache()

    //6.10 Consulta que muestre el total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito apareciento
    // estas 2 como las primeras columnas ademas ordenadas de más a menos por extranjerosmujeres y desempata extranjeroshombres.
    dfPartitionCache
      .select(col("DESC_BARRIO"),col("DESC_DISTRITO"),col("extranjerosmujeres"),col("extranjeroshombres"),
        col("espanoleshombres"),col("espanolesmujeres"))
      .groupBy("DESC_BARRIO","DESC_DISTRITO")
      .agg(sum("extranjerosmujeres"),sum("extranjeroshombres"),sum("espanoleshombres"),sum("espanolesmujeres"))
      .orderBy(desc("sum(extranjerosmujeres)"),desc("sum(extranjeroshombres)"))
      //.show(10,false)

    //6.11 eliminar el registro de cache.
    dfPartitionCache.unpersist()

    //6.12 Crear nuevo DF agrupando el total de hombres en Des_Barrio y Distrito, luego hacer un JOIN con el DF original
    val dfTotalHombres = dfpadron
      .select(col("DESC_BARRIO"),col("DESC_DISTRITO"),col("espanoleshombres"))
      .groupBy("DESC_BARRIO","DESC_DISTRITO")
      .agg(sum("espanoleshombres").alias("SumEspHombres"))
    //dfTotalHombres.show(10,false)

    dfpadron
      .join(dfTotalHombres, dfpadron("DESC_BARRIO")===dfTotalHombres("DESC_BARRIO") && dfpadron("DESC_DISTRITO")===dfTotalHombres("DESC_DISTRITO"),"inner")
      //.show()

    //6.13 Hacemos lo mismo que el punto anterior pero usando funciones de ventana(over(window.partitionBy...)).
    dfpadron
      .withColumn("SumEspHombres",sum("espanoleshombres").over(Window.partitionBy("DESC_BARRIO","DESC_DISTRITO")))
      //.show()

    //6.14 mediante una funcion pivot muesta una tabla que contenga los valores totales de españoles mujeres para cada distrito y rango de edad con COD_EDAD_INT
    // Solo incluiremos los distritos de CENTRO BARAJAS y RETIRO

    val dfdistritos = dfpadron
      .select("COD_EDAD_INT","DESC_DISTRITO","espanolesmujeres")
      .where(col("DESC_DISTRITO") === "CENTRO" or  col("DESC_DISTRITO") === "BARAJAS" or  col("DESC_DISTRITO") === "RETIRO")
    //dfdistritos.show()


    val dfpivot = dfdistritos.groupBy("COD_EDAD_INT")
      .pivot("DESC_DISTRITO")
      .agg(sum("espanolesmujeres"))
      .na.fill(0)
      .orderBy("COD_EDAD_INT")
    dfpivot.show()

    //6.15 usando este ultimo DF crea 3 col nuevas que  hagan referencia al porcentaje de la suma para cada rango de edad de cada distrito redondeado a 2 decimales.
    //Puedes intentar no apoyarte en columnas auxiliares.
    dfpivot.withColumn("% Centro",round(col("CENTRO")/(col("CENTRO") + col("BARAJAS") + col("RETIRO"))*100,2))
      .withColumn("% Barajas",round(col("BARAJAS")/(col("CENTRO") + col("BARAJAS") + col("RETIRO"))*100,2))
      .withColumn("% Retiro",round(col("RETIRO")/(col("CENTRO") + col("BARAJAS") + col("RETIRO"))*100,2))
      .show()

    //6.16 Guardar el csv original Particionado por distrito y barrio en un directorio local. ver el directorio para ver las particiones.

    // todo problemas con la URL
    //val csvPath = "C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/RescursosPadron/CSV"
    /*dfpadron.write
      .partitionBy("DESC_DISTRITO","DESC_BARRIO")
      .mode("overwrite")
      .format("csv")
      .save("///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/PracticaPadron/tablas")*/

    //6.17 Guardar como parquet.
    /*dfpadron.write
      .partitionBy("DESC_DISTRITO","DESC_BARRIO")
      .mode("overwrite")
      .format("parquet")
      .save("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/RescursosPadron/Parquet")*/







  }
}
