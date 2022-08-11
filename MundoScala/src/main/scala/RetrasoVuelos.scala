import javafx.beans.binding.Bindings.select
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, DslSymbol}
import org.apache.spark.sql.functions.{col, date_format, desc, expr, month, to_date, to_timestamp, when}

import scala.reflect.internal.util.TriState.False

object RetrasoVuelos {

 def main (args: Array[String]): Unit ={

   val spark = SparkSession
     .builder
     .master("local[1]")
     .appName("BomberosSF")
     .getOrCreate()

   val csvFile="departuredelays.csv"

   /*val df = spark.read.format("csv")
     .option("inferSchema", "true")
     .option("header", "true")
     .load(csvFile)*/

   // si quieres especificar el squema lo hacemos asi.
   val schema = "date STRING, delay INT, distance INT,origin STRING, destination STRING"

   val df = spark.read.schema(schema)
     .option("header", "true")
     .csv(csvFile)


   df.createOrReplaceTempView("us_delay_flights_tbl")
   df.show(5)
   df.printSchema()



   // Ya podemos trabajar con nuestra vista para hacer consulta SQL
   /*spark.sql("""SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""").show(10)*/

   //Consultamos el retraso de los vuelos
   /*spark.sql("""SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""").show(10)*/

   //Tranformamos de string a fomrato fecha.
   /*val DatosFecha = df.withColumn("newDate", to_timestamp(col("date") ,"MMddHHmm"))
     .drop("date")*/
    //DatosFecha.show(5) // si pongo el Show en luegar de obtener un dataset obtengo un Unit no me deja usar selec ni nada.

   //Transformamos de timestamp al formato que queremos MM-dd HH:mm
   /*val cleandate = DatosFecha
     .select(date_format(col("newDate"),"MM-dd HH:mm") as ("dateFinal"),col("delay"),col("distance"),col("origin"),col("destination"))*/

   // todo la query de abajo no me funciona mes con el formato de arriba.
   // vemos en que meses se producen mas retrasos.
   /*DatosFecha
     .select(month(col("newDate"))as("Meses"), col("delay"))
     .groupBy("Meses")
     .count()
     .orderBy(desc("count"))
     .show()*/

//Separamos los retrasos en tramos para ver si han sido mas o menos largos.
  /* spark.sql("""SELECT delay, origin, destination,
 CASE
 WHEN delay > 360 THEN 'Very Long Delays'
 WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
 WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
 WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
 WHEN delay = 0 THEN 'No Delays'
 ELSE 'Early'
 END AS Flight_Delays
 FROM us_delay_flights_tbl
 ORDER BY origin, delay DESC""").show(5)*/

   // todo me lo hace en varias columnas no tiene sentido.
  /* df.select(col("delay"),col("origin"),col("destination")
     ,when(col("delay")>"360", "Very Long Delays")
     ,when(col("delay")<"360" and (col("delay")>"120"),"Long Delays")
     ,when(col("delay")<"120" and (col("delay")>"60"), "Short Delays")
     ,when(col("delay")<"60" and (col("delay")>"0"), "Tolerable Delays")
     .otherwise("no delais")
     .alias("Flight_Delays"))
     .orderBy(desc("delay"))
     .show()*/
   /*val df2 =  df.withColumn("Flight_Delays"
     ,expr("CASE when 'delay' > 360 , THEN 'Very Long Delays' ,CASE when 'delay' < 360 and 'delay' > 120 ,'Long Delays', CASE when 'delay' < 120 and 'delay' > 60 , 'Short Delays',CASE when 'delay' < 120  and 'delay' > 60, THEN 'Short Delays,CASE when 'delay' < 60  and 'delay' >0, THEN 'Tolerable Delays' ELSE 'no delais' END"))
     .orderBy(desc("delay"))
     .show()*/



  // Mostramos los vuelos que recorren mas de 1000 millas
   /*df
     .select("distance", "origin", "destination")
     .where(col("distance") > 1000)
     .orderBy(desc("distance"))
     .show(10)*/
  // Misma query que arriba pero con SQL
   spark.sql("""SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""").show(10)



 }

}
