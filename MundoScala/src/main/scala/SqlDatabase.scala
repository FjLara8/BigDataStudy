import org.apache.spark.sql.SparkSession

object SqlDatabase {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("SqlDatabase")
      .getOrCreate()

    //Creo la BD y le soy un nombre
    spark.sql("CREATE DATABASE learn_spark_db")

    //Indico que voy a usar la BD
    spark.sql("USE learn_spark_db")




  }
}
//Creo la BD y le soy un nombre
/*spark.sql("CREATE DATABASE learn_spark_db")

//Indico que voy a usar la BD
spark.sql("USE learn_spark_db")

// Creo una tabla gestionada con SQL
spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

//Crear tabla gestionada con Dataframe
val csv_file = "departuredelays.csv"
val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
val flights_df = spark.read.schema(schema).csv(csv_file)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

// Crear tablas NO GESTIONADAS apartir de datos ya dados.
spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
 distance INT, origin STRING, destination STRING)
 USING csv OPTIONS (PATH 'departuredelays.csv')""")

//Acerlo con DF
(flights_df
  .write
  .option("path", "/tmp/data/us_flights_delay")
  .saveAsTable("us_delay_flights_tbl"))

// Podemos crear vistas de nuestras tablar, las vistas son temporales y las tablas persisten aunque se cierre la sesion
// Hay vistas que pueden estar en varias sesion y otras solo pertenecen a una.

//Tabla global varias sesion SQL
spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
            | SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
            | origin = 'SFO';""".stripMargin)

//tabla para una sesion SQL
spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
            | SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
            | origin = 'JFK'""".stripMargin)

// tabla global Scala
val df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

//Creacion tablas sesion Scala
val df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

//Creación de vistas.
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

//Cuando accedemos a una vista global en SQL ponemos la prefijo "global_temp." para que el programa entienda donde buscarla
spark.sql("""SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view""")
//sin embargo en una de seccion no es necesario nada.
spark.sql("""SELECT * FROM us_origin_airport_JFK_tmp_view""")

// En Scala leemos el archivo del tiron. con estas DOS opciones
spark.read.table("us_origin_airport_JFK_tmp_view")
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")

//Tambien podemos lanzar una vista como si fuese una tabla.
spark.sql("""DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view;""".stripMargin)
spark.sql("""DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view""")

// Con Scala la lanzamos asi:
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

// acceder a diferentes metadatos dentro de mi spark session
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

// Indicar que una tabla debe ser almacenada en cache una vez que sea llamada la primera vez en lugar
// de hacerlo si o si de primeras tablas LAZY.
spark.sql("""CACHE [LAZY] TABLE <table-name>""".stripMargin)
spark.sql("""UNCACHE TABLE <table-name""")

//Cuando tenemos tablas y BD creadas,puedes simplemente usar SQL para consultar la tabla y asignar el
//resultado devuelto a un DataFrame Scala, asi conseguimos un DF limpio atraves de una tabla importada de spark sql
val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
val usFlightsDF2 = spark.table("us_delay_flights_tbl")*/

// Python
/*us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
us_flights_df2 = spark.table("us_delay_flights_tbl")*/

//Dataframeread para leer dataframes sirve para varios formatos CsV, Json, Parquet...
//In Scala
// Use Parquet
//val file = """/databricks-datasets/learning-spark-v2/flights/summary-
 //data/parquet/2010-summary.parquet"""
//val df = spark.read.format("parquet").load(file)
// Use Parquet; you can omit format("parquet") if you wish as it's the default
//val df2 = spark.read.load(file)
// Use CSV
//val df3 = spark.read.format("csv")
  //.option("inferSchema", "true")
  //.option("header", "true")
  //.option("mode", "PERMISSIVE")
  //.load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")
// Use JSON
//val df4 = spark.read.format("json")
  //.load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")

// patron de uso de DataFramewrite
/*DataFrameWriter.format(args)
  .option(args)
  .bucketBy(args)
  .partitionBy(args)
  .save(path)

  DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)

  para obtener un gestor de instacias usamos:

  DataFrame.write
// or
  DataFrame.writeStream

//Un pequeño ejemplo de DAtaframewrite con sus argumentos
df.write.format("json").mode("overwrite").save(location)*/

//leyendo una tabla parquet con SQL y creando vista temporal. Skirve para leer otros formatos
//Csv, Json, Parquet, Avro, ORC
//spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
            //| USING parquet
          //  | OPTIONS (
           // | path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
           // | 2010-summary.parquet/" )
           // |""".stripMargin)

//Gruardar un DataFrame sirve para varios formatos Csv, Json, Parquet, Avro, ORC
/*df.write.format("parquet")
  .mode("overwrite")
  .option("compression", "snappy")
  .save("/tmp/data/parquet/df_parquet")

//Guardar Dataframe avro sin compresion
df.write
  .format("avro")
  .mode("overwrite")
  .save("/tmp/data/avro/df_avro")

// Escribir un dataframe como una tabla
df.write
  .mode("overwrite")
  .saveAsTable("us_delay_flights_tbl") // cambia el sabeAsTable*/

// Para leer un CSV podemos indicar el Schema

//val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
/*val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
val df = spark.read.format("csv")
  .schema(schema)
  .option("header", "true")
  .option("mode", "FAILFAST") // Exit if any errors
  .option("nullValue", "") // Replace any null data with quotes
  .load(file)*/

// Escritura de un DataFrame
//df.write.format("csv").mode("overwrite").save("/tmp/data/csv/df_csv")

//Leer un archivo image igual que los anteriores pero tenemos que importar una biblioteca
/*import org.apache.spark.ml.source.image
val imageDir = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val imagesDF = spark.read.format("image").load(imageDir)
imagesDF.printSchema
imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode",
  "label").show(5, false)*/

//Leer un archivo binario con la opcion pathGlobFilter que me lee todos los JPG
// Para ignorar la particion de datos en un directorio añadimos recursiveFileLookup quita la columna label
/*val path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val binaryFilesDF = spark.read.format("binaryFile")
  .option("pathGlobFilter", "*.jpg")
  .option("recursiveFileLookup", "true")
  .load(path)
binaryFilesDF.show(5)*/