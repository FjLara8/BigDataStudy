package org.example

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.internal.util.NoPosition.line

object PMun{

  // Recuento palabras

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("PMun")
    //  .setSparkHome("src/main/resources")

    val sc = new SparkContext(conf)

    //primertexto(sc = sc)
    //segundaparte(sc = sc)
    //ejercicioC(sc = sc)
    leer(sc = sc)

    //pruebadataframe(sc = sc)

  }

  def primertexto(sc: SparkContext): Unit ={

//Indicas donde se enceuntra el fichero que vamos a usar.
 val relato = sc.textFile("C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/relato.txt")
 val count = relato.flatMap(line => line.split(" "))
   .map(word =>(word,1))
   .reduceByKey(_+_)


 //Cuenta en numero de lineas
 println {
   relato.count()
 }
println {
  relato.collect()
}

//spark.sparkContext.setLogLevel("ERROR")

 //muestra el total del texto
 relato.foreach { item => {
   println(item)
 }
 }

}

def segundaparte(sc: SparkContext): Unit ={

 val ruta = "file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/weblogs/2013-09-15.log"
 val logs = sc.textFile(ruta)
 val jpglogs = logs.filter(x=>x.contains(".jpg"))
  jpglogs.take(5).foreach{
    x => println(x.mkString(sep = ","))
  }
 //anidar varios metodos en una linea
 //val jpglogs = logs.filter(x=>x.contains(".jpg")).count()

 //calcula al lilongitud de las primeras 5 lineas tambien vale length()
 val longitud = logs.map(x=>x.size)

 //mostrar las palabras de las 5 primeras linas
 val palabras = jpglogs.map(x=>x.split(" "))
  palabras.take(5).foreach{
    x => println(x.mkString("Array: (", ", ", ")"))
  }

  // Mostar por pantalla las 5 primeras palabras de las 5 primeras lineas del texto
   val palabras2 = palabras.filter(line=>line.contains(".jpg")).count()
  println(palabras2)

  //mapear el contenido de logs en array de palabras
  var logwords = logs.map(line => line.split(' '))

  // crear un nuevo RDD que contenga el primer elemento de cada final (ips)
  var ips = logs.map { line =>
    line.split(' ')(0)
  }

  ips.take(5).foreach{
    x => println(x.mkString(sep = ","))
  }
  //Mostar la informacion usando un For
  for (x <- ips.take(10) ) { println(x) }

  // Guardar el archivo de ips en un blog
  ips.saveAsTextFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/ipssave.txt")

  //println {
   //println(palabras.take(10).foreach(println))
// }

}

  def ejercicioC(sc : SparkContext) : Unit={

    val ruta2 = "file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/weblogs/2013-09-15.log"
    val logs2 = sc.textFile(ruta2)


    //Creamos un fichero que contenga tolos lo ipl de los archivos de la carpeta weblogs
    sc.textFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/weblogs/*")
      .map(line => line.split(' ')(0))saveAsTextFile
      ("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/iplistw")
    // creamos un rdd que contenga la ip e id de nuestro fichero
    var htmllogs=logs2.filter(_.contains(".html")).map(line => (line.split(' ')(0),line.split(' ')(2)))
    //mostrar htmllogs
    htmllogs.take(5).foreach(t => println(t._1 + "/" + t._2))

  }

def leer(sc : SparkContext): Unit ={

  val datosA = sc.textFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/shakespeare.tar")
  val nulasA = sc.textFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/stop-word-list.csv")



  val nulasB = nulasA.flatMap(_.split(","))


  val datosB = datosA.flatMap(line => line.split(" "))
    .map(x=>x)
    .map(x=> x.toLowerCase())
    .map(word =>word)

  datosB.take(20).foreach(println)

  val datosC = datosB.map(_.replaceAll("[^a-zA-Z]+", ""))
  val nulasC = nulasB.map(_.replaceAll("[^a-zA-Z]+", ""))

  val datosD = datosC.map(word => word.replace(" ", ""))

  // Dejar solo los caracteres numericos

  //val datosD = datosC.map(word =>word)
  datosC.take(20).foreach(println)
  nulasC.take(5).foreach(println)


  // separo el csv por " " luego convierto a minusculas le doy un valor a cada palabra de 1 y luego las agrupo para contarlas


  //counts.take(5).foreach(println)
  val limpieza = datosC.subtract(nulasC)
  val limpieza2 = limpieza.filter(_.length>1)

  val finalrdd = limpieza2.map(word =>(word, 1)).reduceByKey(_+_)

  val mostrarfinal = finalrdd.map(p =>(p._2, p._1)).sortByKey(false, 1).map(_.swap)



  mostrarfinal.take(10).foreach(println)
  //val limpieza = counts.filter(_.(palnulas)*/


  }

  /*def leer2 (sc : SparkContext):Unit={

    val logs=sc.textFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/shakespeare.tar")
    val logs2=logs.map(line => line.replaceAll(“[^a-zA-Z]+”,” “))
    val logs3=logs2.flatMap(line => line.split(“ “))
    val logs4= logs3.map(word => word.toLowerCase)
    val stopwords =sc.textFile(“stop-word-list.csv”)
    val stopwords2=stopwords.flatMap(line => line.split(“,”))
    val stopwords3=stopwords2.map(word => word.replace(“ “,””)
    val logs5=logs4.substract(sc.parallelize(Seq(“ “)))
    val logs6=logs5.substract(stopwords3)
    val logs7=logs6.map(word => (word,1)).reduceByKey(_+_)
    val logs8=logs7.map(word => word.swap).sortByKey(false).map(value.swap)
    val logs9=logs8.filter(word =>word._1.size !=1)
    logs9.take(20).foreach(println)

  }*/










  def pruebadataframe(sc: SparkContext): Unit = {

 val data = Seq(
   (10, "Spark"),
   (11, "POM"),
   (12, "Scala")
 )

 val rdd = sc.parallelize(data)
 rdd.foreach(item => {
   println(item)

   println("Estoy en foreach")
 })

}

// spark.sparkContext.setLogLevel("ERROR") para quitar lineas de error no imp
}
