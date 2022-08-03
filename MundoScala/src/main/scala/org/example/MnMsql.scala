package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object MnMsql {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("MnMsql")
      .getOrCreate()


    /*val namesquema = "State:Color:Count"
    // creamos una structura
    val squema2 = StructType(namesquema.split(":").map(StructField(_, dataType = StringType, nullable = true)))

    // val schema = ScalaReflection.schemaFor[Empleados].dataType.asInstanceOf[StructType]*/

    val rddbase = spark.read
      .options(Map("delimiter" -> ","))
      .option("header",true)
      .option("inferSchema", true )
      .csv("mnm_dataset.csv")


    //val dfesquema = spark.createDataFrame(rddbase.rdd, squema2)

    rddbase.createOrReplaceTempView("MnMdf")

    rddbase.show()
    rddbase.printSchema()

    //todo Esta parte no funciona ni SUM ni COUNT... Animo
    spark.sql("SELECT State, Color, COUNT(Count) FROM MnMdf GROUP BY Color, State ORDER BY COUNT (Count) Desc").show(10)

    spark.sql("SELECT State, Color, AVG(Count) FROM MnMdf GROUP BY Color, State ").show(10)

    spark.sql("SELECT State,Color,count(Count) AS Total, MAX(Count),MIN(Count),AVG(Count) FROM MnMdf GROUP BY State,Color ORDER BY count(Count) ").show(10)


    //spark.sql("SELECT Count('Temporada'), EquipoV FROM dfVista Where EquipoV = 'Real Oviedo'").show()

    //spark.sql("""SELECT  count(distinct(Temporada)) as idcnt FROM dfVista """).show()

    // Es necesario el Group By y no poner "" con el distinct
    //spark.sql("""SELECT Count(distinct (Temporada)),EquipoV From dfVista Where EquipoV = 'Real Oviedo' OR EquipoV = 'Sporting de Gijon' GROUP BY EquipoV """).show()



  }

}
