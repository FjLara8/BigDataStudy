package EjerciciosScala

import scala.io.Source

object Ejercicio2 {

  def main (args: Array[String]): Unit ={

    // crear una variable inmutable
    val ab = 25
    ab
    // mostrar la variable.

    println(ab)

    // Todo esto aqui no se puede hacer.
    /*"Hello, Scala"
    print(res1)*/

    /*scala> print(ab
      |
      |
     Esto ocurre en la Shell si te falta cerrar el ) detecta como que aun no has terminado*/

    //Cargo mi archivo Loudacre
    val datadir = scala.util.Properties.envOrElse("SCADATA", ".") // hay que declarar antes SCADATA
    Source.fromFile("loudacre.log").foreach(print)


  }

}
