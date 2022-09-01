import javafx.beans.binding.Bindings.select
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}

object Cap5Vuelos {


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("Cap5ExternalData")
      .getOrCreate()

    import  org.apache.spark.sql.functions._

    val delaysPath = "departuredelays.csv"
    val airportsPath = "airport-codes-na.txt"

    // cargamos nuestros datos en tablar para poder usarlas.
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")

    // usamos la expresion expr() para convertir las columnas delay y distance en int en luegar de string
    val delays = spark.read
      .option("header","true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")


    // creamos una tabla con  origen en (SEA) destino (SFO) para una fecha 01-01 y un delay mayor que 0
    val foo = delays.filter(
      expr("""origin == 'SEA' AND destination == 'SFO' AND
      date like '01010%' AND delay > 0"""))
    foo.createOrReplaceTempView("foo")
    foo.show()

    // Limito a 10 la cantidad de datos.
    /*spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    spark.sql("SELECT * FROM foo").show()

    // Unir dos DF con el mismo esquema.
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
    AND date LIKE '01010%' AND delay > 0""")).show()

    //Como era de esperar el DF foo se relplica para rellenar los hucos.
    spark.sql("""SELECT * FROM bar WHERE origin = 'SEA' AND destination = 'SFO'
    AND date LIKE '01010%' AND delay > 0 """).show()


   // Joins unir dos DF por defecto. Marcamos las columnas que se igualan.
    //Pongo col por que $ no me esta funcionando
    foo.join(airports.as("air"), col("air.IATA") === col("origin"))
      .select("City", "State", "date", "delay", "distance", "destination").show()*/

    //Join en SQL
    /*spark.sql("""SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
    FROM foo f JOIN airports_na a ON a.IATA = f.origin""").show()*/
    import org.apache.spark.sql.expressions.Window


    // todo No me deja crear la tabla medienate CREATE TABLE,no deja por que no tiene siotio donde hacerlo como hive o tal
    /*spark.sql("""CREATE OR REPLACE TEMP VIEW departureDelaysWindow AS SELECT origin, destination, SUM(delay) AS TotalDelays
                FROM departureDelays
                WHERE origin IN ('SEA', 'SFO', 'JFK')
                AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
                GROUP BY origin, destination""".stripMargin)
    spark.sql("""SELECT * FROM departureDelaysWindow""").show()

    //todo Where origin ='[ORIGIN]' tampoco funciona, '[ORIGIN]' no exite directamente es para que tu pongas lo que necesites.
    spark.sql("""SELECT origin, destination, SUM(TotalDelays) AS TotalDelays2
                |FROM departureDelaysWindow
                |WHERE origin = 'JFK' OR origin = 'SEA' OR origin = 'SFO'
                |GROUP BY origin, destination
                |ORDER BY SUM(TotalDelays) DESC
                |LIMIT 3
                |""".stripMargin).show()


    // Uso de ventanas con dense_rank()
    spark.sql("""SELECT origin, destination, TotalDelays, rank
    FROM ( SELECT origin, destination, TotalDelays, dense_rank()
    OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
    FROM departureDelaysWindow) t
    WHERE rank <= 3""").show()*/


    //Modificaciones de DF pese a ser inmutables se pueden modificar creando nuevos.
    foo.show()

    //Añadimos columnas usando withColum()
    import org.apache.spark.sql.functions.expr
    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))

    foo2.show()

    //Borramos columnas usando drop()
    val foo3 = foo2.drop("delay")
    foo3.show()

    //Renombramos las columnas withColum()
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

  // Pivotamos los datos cambiamos columnas por filas para esto usamos SQL
    spark.sql("""SELECT * FROM (
                |SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
                | FROM departureDelays WHERE origin = 'SEA'
                |)
                |PIVOT (
                | CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
                | FOR month IN (1 JAN, 2 FEB)
                |)
                |ORDER BY destination""".stripMargin).show()


  }
}
