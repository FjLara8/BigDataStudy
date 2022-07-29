package org.example

import org.apache.spark.sql.SparkSession

object SQLspark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SQL1")
      .getOrCreate()

    val zips = spark.read.json("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/zips.json")

    zips.show()

    //Obtén las filas cuyos códigos postales cuya población es superior a 10000
    zips.filter(zips("pop") > 10000).collect()

    // Crear DF temporal basada en el df anterior
    zips.createOrReplaceTempView("zipsTemp")

    //Misma consulta que la anterior pero usando SQL
    spark.sql("select * from zipsTemp where pop > 10000").collect()

    //obtén la ciudad con más de 100 códigos postales
    spark.sql("select city from zipsTemp group by city having count(*)>100 ")

    // Mostrar la poblacion total de Wisconsin (WI)
    spark.sql("select SUM(pop) as POPULATION from zipsTemp where state='WI'").show()

    //Mostar los 5 estado con mayor problacion
    spark.sql("select SUM(pop) as POPULATION, state as ESTADOS from zipsTemp Group by state order by Sum(pop) Desc")
      .collect()
      .foreach(println)
  }

}
