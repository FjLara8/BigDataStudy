package EjerciciosScala

import scala.io.Source

object Ejercicio3 {

  def main (args: Array[String]): Unit={

    val varinmu = 19
    //varinmu = 42 // da error al ser inmutable

   // val varinmu = 27.0 // indica que ya esta definida

    var mutable = 15
    //mutable = "Tiempo" //Indica que la la var no es de tipo String
    mutable = 67
    //mutable = 34.6  //En este caso es Double
    mutable = 2
    println(mutable) // no puedo realizar mas por que en intellij se corta si algo da error
    //var mutable = 3.5 // no se puede redefinir una variables

    val phoneModel: String = "Sorrento"
    val temp: Int = 30
    val location: Double = 33.1765

    //6
    val tempC = 27
    val c_to_f = 9/5
    val temF = tempC * c_to_f + 32
    println(tempC + "; "+c_to_f+"; "+ temF)

    //7
    val c_to_f2 = 9.0/5.0 // Para ahcer estas cuentas es mejor indicas .0 que ponerlos como float si no no saca el numero real
    val temF2 = tempC * c_to_f2 + 32
    println(tempC + "; "+c_to_f2 +"; "+ temF2)
    println(c_to_f2.getClass, temF2.getClass)

    //9 como realizar bien un calculo aunque sea Double no realiza bien el calculo
    val cpu = 17
    val cpu2 = 38
    val media = (cpu+cpu2) / 2
    println(media)
    val media2: Double = (cpu+cpu2) / 2
    println(media2)
    val media3: Double = (cpu+cpu2) / 2.0
    println("La media es: "+media3)

    //13
    val cpu12 = 12/100.0
    val cpuBest = cpu2 - (cpu2 * cpu12)
    println("12%: " + cpu12 + " Rendimiento CPU: " + cpuBest)

    //esto no vale por que es % como estan los datos
    val cpuBestB = media3 * 0.88
    println("Rendimiento Real: " + cpuBestB)

    //14
    //cpu + = 1 // no lo permite pr que es un val
    val cpumas = cpu + 1

    //16
    cpu.toDouble
    cpu2.toFloat
    cpu.toString
    println(cpu.toString+";"+cpu2.toFloat)

    //17
    println(cpu.+(1),cpu.equals(8))

    //18
    println(math.sqrt(42))

    //19
    import scala.math.sqrt
    println(sqrt(87))

    //20

    val record = Source.fromFile("sample_one_record.txt")
    record.foreach(print)
    println(record.length)

    val record2 = "2014-03-15:10:10:31,Titanic 4000,1882b564-c7e0-4315-aa24-228c0155ee1b,58,36,39,31,15,0,TRUE,enabled,enabled,37.819722,-122.478611"
    record2.foreach(print)
    println("\n" + record2.length)

    //22
    println(record2.contains("Titanic"))
    //23
    val inicioTitanic =record2.indexOf("T")
    println(inicioTitanic)
    //24
    println(record2.substring(inicioTitanic, inicioTitanic+"Titanic".length).toUpperCase)
    //25
    println(record2.substring(inicioTitanic, inicioTitanic+"Titanic".length) + record2.substring(inicioTitanic+"Titanic".length + 1,inicioTitanic+"Titanic".length + 5))

    //28 Comprobar el estado de Bluetooth y wifi que estan en las posiciones cuarta y tercera desde el final.
    val record3 = "2014-03-15:10:10:31,Titanic 4000,1882b564-c7e0-4315-aa24-228c0155ee1b,58,36,39,31,15,0,TRUE,disabled,enabled,37.819722,-122.478611"
    println(record3.containsSlice("connected"))//indica ci la cadena contiene esta cadena
    println(record3.containsSlice("disabled"))
    println(record3.containsSlice("enabled"))
    //Buscamos la posicion de los elementos que contiene true sabiendo la posicion sabemos cual es cada uno.
    val indexBlue = record3.indexOfSlice("disabled")
    val indexWifi = record3.indexOfSlice("enabled")

    val bluethooth = record3.substring(indexBlue, indexBlue+"disabled".length)
    val wifi = record3.substring(indexWifi, indexWifi+"enabled".length)

    println(bluethooth,wifi)

    println("¿tienen el mismo estatus? " + (bluethooth == wifi))

    //todo mirar lo de la api pag 21 ejercicios













  }
}
