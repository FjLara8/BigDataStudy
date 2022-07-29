package org.example

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object paiRdd {

  def main (args: Array[String]): Unit = {
    val confrdd = new SparkConf()
      .setMaster("local")
      .setAppName("PMun")

    val sc = new SparkContext(confrdd)

    parteA(sc = sc)


}

  def parteA (sc : SparkContext): Unit={
    var logs=sc.textFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/data_spark/weblogs/*")

    // Muestra los id de usuario y el número de accesos para los 10 usuarios con mayor número de accesos

    var userreqs = logs.map(line => line.split(' ')).map(words => (words(2),1)).reduceByKey((v1,v2) => v1 + v2)
    // Usamos swap intercambiar la Clave por el Valor
    val swapped=userreqs.map(field => field.swap)

    // Ahora usamos sortByKey para ordenar y de nuevo Swap para devolver los valores como estaban.
    /*swapped.sortByKey(false).map(field => field.swap).take(10).foreach(println)*/

    //swapped.collect().foreach(println)

    //Crea un RDD donde la clave sea el userid y el valor sea una lista de ips a las que el userid se ha conectado
    var userips = logs.map(line => line.split(' ')).map(words => (words(2),words(0))).groupByKey()
    userips.take(10)

    //Importamos un nuevo csv usando map() y dejando Userid como clave y el resto de datos como valor.
    var accounts = sc.textFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/accounts.csv")
      .map(line => line.split(',')).map(account => (account(0),account))

    // Realizamos un Join y juntamos los rdds account y userreqs.
    var accounthits = accounts.join(userreqs)

    //Crea un RDD que contenga el userid, número de visitas, nombre y apellido de las 10primeras lineas.
    for (pair <- accounthits.take(10)) {println(pair._1,pair._2._2, pair._2._1(3),pair._2._1(4))}

    // Usa keyBy para crear un RDD con el código postal como clave (noveno campo)
    var accountsByPCode = sc.textFile("file:///C:/Users/franciscojavier.lara/Desktop/Teoria/BigData/Ejercicios/Recursos/accounts.csv")
      .map(_.split(',')).keyBy(_(8))

    // Usamos la anterior para crear una que contenga solo el nombre y apellidos (por 5-4) agrupada por Codigo Postal
    var namesByPCode = accountsByPCode.mapValues(values => values(4) + ',' + values(3)).groupByKey()

    //Mostrar los 5 primeros codigos. Ambas formas funcionan.
    /*namesByPCode.sortByKey().take(5).foreach{

       case(x,y) => println ("---" + x)
         y.foreach(println)};*/

    namesByPCode.sortByKey().take(5).foreach{

       x => println ("---" + x._1)

       x._2.foreach(println)};

  }
}
