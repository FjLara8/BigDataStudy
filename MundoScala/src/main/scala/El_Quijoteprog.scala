import org.apache.spark.sql.SparkSession

object El_Quijoteprog {


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("El_Quijoteprog")
      .getOrCreate()

    val quijote = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Recursos/el_quijote.txt")

    // el metodo truncate indica si el texto se queda cortado o no false no te corta el texto muestra la linea completa

    //quijote.show(truncate=false)

    //quijote.show(numRows = 8)

    //quijote.show(numRows = 8, truncate = 1, vertical = true)

    // devulve un dataset
    quijote.show(1)

    //devuelve un array
    println(quijote.head())


    quijote.take(1).foreach{
      x => println(x.mkString(sep = ","))
    }

   /* quijote.head(4).foreach{
      x => println(x.mkString(sep = ","))
    }

    quijote.take(8).foreach{
      x => println(x.mkString(sep = ","))
    }*/

  }


}
