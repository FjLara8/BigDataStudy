
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, dayofmonth, days, desc, hour, max, month, regexp_extract, to_timestamp, year}

object WebServer {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Mpadron6")
      .getOrCreate()

    /*val df = spark.read
      .textFile("access_log_Aug95")

    df.show()*/

    // creamos un df usando serdex para depurar nuestro archivo de texto y crear las columnas y contenido necesarios.
    val formatoFecha = "dd/MMM/yyyy:HH:mm:ss"

    /*val dfserver = df.select(regexp_extract(col("value"), """^([^(\s|,)]+)""", 1).alias("Host"),
      to_timestamp(regexp_extract(col("value"), """^.*\[(\d{2}/\w+/\d{4}:\d{2}:\d{2}:\d{2})""", 1),formatoFecha).alias("Date"),
      regexp_extract(col("value"), """^.*\"(\w+)\s""", 1).alias("Method"),
      regexp_extract(col("value"), """^.*\w+\s+([^\s]+)\s\w{4}""", 1).alias("Resource"),
      regexp_extract(col("value"), """^.*\s+(\w{4}\S+)\"""", 1).alias("Protocol"),
      regexp_extract(col("value"), """^.*\"\s+(\d+)\s""", 1).cast("int").alias("HTTP_status"),
      regexp_extract(col("value"), """^.*\d\s(\d+)""", 1).cast("int").alias("Size"))*/

    //dfserver.show(10,false)


    //LeerParquet

    val dffinal = spark.read.parquet("dfparquet")

    dffinal.printSchema()

    //dffinal.show()

    ProtocoloDistinct(dffinal).show(10,false)

    ProtocoloCount(dffinal).show(7)

    CodigosComunes(dffinal) .show(5)

    ResourceCount(dffinal).show(5, false)

    MaxTransBytes(dffinal).show(5,false)

    RmaxTrafico(dffinal).show(10, false)

    DiasTrafico(dffinal).show(5)

    HostFrecuentes(dffinal).show(10, false)

    MaxTrafico(dffinal).show(5)

    Error404(dffinal).show(5,false)
  }

  /*private def LeerParquet = {
    //Crear archivo tipo parquet
    dfserver.write
      .mode("overwrite")
      .parquet("dfparquet")
  }*/

  private def ProtocoloDistinct(dffinal: DataFrame) = {
    //¿Cuáles son los distintos protocolos web utilizados? Agrúpalos
    dffinal
      .select(col("Protocol"))
      .distinct()
  }

  private def ProtocoloCount(dffinal: DataFrame) = {
    // Cuanto se repite cada protocolo
    dffinal
      .select(col("Protocol"))
      .groupBy("Protocol")
      .count()
      .orderBy(desc("count"))
  }

  private def CodigosComunes(dffinal: DataFrame) = {
    //¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos
    //para ver cuál es el más común
    dffinal
      .select(col("HTTP_status"))
      .groupBy("HTTP_status")
      .count()
      .orderBy(desc("count"))
  }

  private def ResourceCount(dffinal: DataFrame) = {
    //¿Y los métodos de petición (verbos) más utilizados?
    dffinal
      .select(col("Resource"))
      .groupBy("Resource")
      .count()
      .orderBy(desc("count"))

  }

  private def MaxTransBytes(dffinal: DataFrame) = {
    //¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
    // En este caso max nos selecciona el maximo tamaño de cada registro y muestra solo 1 de cada.
    dffinal
      .groupBy("Resource")
      .agg(max("Size").alias("Tmax"))
      .orderBy(desc("Tmax"))
  }

  private def RmaxTrafico(dffinal: DataFrame) = {
    //Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es
    //decir, el recurso con más registros en nuestro log.
    dffinal
      .select(col("Resource"))
      .groupBy("Resource")
      .count()
      .orderBy(desc("count"))
  }

  private def DiasTrafico(dffinal: DataFrame) = {
    //¿Qué días la web recibió más tráfico?
    dffinal
      .select(dayofmonth(col("Date")) as ("Días"))
      .groupBy("Días")
      .count()
      .orderBy(desc("count"))

  }

  private def HostFrecuentes(dffinal: DataFrame) = {
    //¿Cuáles son los hosts son los más frecuentes?
    dffinal
      .select(col("Host"))
      .groupBy("Host")
      .count()
      .orderBy(desc("count"))

  }

  private def MaxTrafico(dffinal: DataFrame) = {
    //¿A qué horas se produce el mayor número de tráfico en la web?
    dffinal
      .select(hour(col("Date")) as ("Horas"))
      .groupBy("Horas")
      .count()
      .orderBy(desc("count"))
  }

  private def Error404(dffinal: DataFrame) = {
    //¿Cuál es el número de errores 404 que ha habido cada día?
    dffinal
      .select(col("HTTP_status"), dayofmonth(col("Date")) as ("Día"))
      .where(col("HTTP_status") === "404")
      .groupBy("Día")
      .agg(count("Día").alias("NError"))
      .orderBy(desc("NError"))

  }
}
