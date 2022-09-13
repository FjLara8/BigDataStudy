package EjerciciosScala


object Apuntes {

  def main (args: Array[String]): Unit = {

    println("Hello, Scala")

    //Source.fromFile("loudacre.log").foreach(print)

    val s = "Titanic 4000"
    println(s)

    val MyInt: Int = 123

    MyInt

    //println(res0)

    //Para introducir datos por pantalla
    /*var str: String= scala.io.StdIn.readLine("Enter:\n")

    println(str)*/

    val s1 = "Sorrento F41L"

    println(s1)

    //Introducir datos en un print con Format
    val formatStr = "Temperature range is %d to %f celsius"
    print(formatStr.format(24, 31.24))


    val filename: String = "loudacre.log"

    //no funciona y es similar a la de abajo.
    /*val buffertex = Source.fromFile(filename)
      .bufferedReader(print)*/

    //Source.fromFile(filename).foreach(print)


    // Que nos indique la URL de nuestro proyecto
    //Si $SCADATA no está definido, establecemos datadir al directorio actual
    val datadir = scala.util.Properties.envOrElse("SCADATA", ".")


    // Accedecmos a nuestro directorio para mostrar por pantalla el contenido de loudacre
    /*Source.fromFile(datadir + "/loudacre.log").foreach(print)*/




    // Todo TEMA 4
    //Tipos de variables:
    //inmutable declarada
    val numa: Int = 0

    //Mutable inferida, aunque sea mutable el tipo no varia de Int a String no podemos cambiar aunque si el valor de Int
    var numb = 0

    print(numa, numb)

    //numa = 5 Da fallo ya que es inmutable.
    numb = 5
    print(numa, numb)

    //Si puedo redefinir una variable. todo no es real lo de redefinir una variable 4-08

    /*var numb = "Var Redefinida"
    print(numa, numb)*/

    // Esto devuelve un Unit, "nada", equivalente a void en java
    val myreturn = println("Hello, world")

    // Any usado cuando no sabe que type utilizar todo esta parte no funciona 4-11
    /*val myreturn = if (true) "hi" //mezcla bool con String*/
    //para indicar el tipo uasmos
    //val mystring = myreturn.asInstanceOf[String] // le decimos que es tipo String

    val PhoneStyle: Char = 'd'

    //getClass  determina la clase del objeto. devuelve el tipo de objeto. al tener Arity-0 no es necesario el ()
    PhoneStyle.getClass
    PhoneStyle.getClass()

    //hacer el cuadrado de un numero
    math.pow(3, 2)
    // Division enteros
    val dicisionint = 7 / 5
    // resto division
    val remainder = 7 % 5
    //Division float
    val divFloat = 7.0 / 5.0

    //Conversiones de tipo
    val tempCelsius = 35
    val tempFahrenheit = 9.0 / 5.0 * tempCelsius + 32
    val fahrInt: Int = tempFahrenheit.toInt
    val iLat = 331913
    val dLat = iLat.toDouble * 1000

    // variables Booleanas
    val gpsStatus: Boolean = false
    val gpsStatus2 = true

    // Mezclar tipos numericos 1 == 1.0
    // para poder poner \ en un texto y que no interprete un escape usamos raw"Scala\text" or """Scala\text"""

    //Coger caracter de una cadena.

    val oneChar = s1(4) // devulve caracter de la posicion 4
    val version = s1.substring(2, 6) //devulve ese intervalo si pongo mas de la cadenba devuelve hasta el final.
    //Metodos para String
    s1.sorted // devuelve las legttras en orden alfabetico y tanbtas veces como se repitan
    s1.toUpperCase() // devuvle las letras en mayusculas
    s1.toArray // devuelve un array con las letras separadas.

    // Formas de separar las cadenas
    val stex = "Bananas, apples, and oranges"
    stex.splitAt(4) // Separa las 4 primeras letras de las demas
    s.split(',') //Separa por el elemento indicado.

    //Encadenando metodos en este caso tuUpperCase y sorted
    println(s.toUpperCase().sorted)

    // Todo Uso del TAB aqui es automatico¿?

    //Sustituyendo variables
    val phoneName = "Titanic"
    val phoneTemp = 35
    println(s"Name: $phoneName")
    println(s"Name: $phoneName", s"Temp: $phoneTemp")

    // Sustitucion y cambio de formato.
    println(f"Temp: $phoneTemp%f")
    println(f"Temp: $phoneTemp%.2f")
    println(f"Temp: $phoneTemp%h as hex")
    println(phoneTemp.getClass)


    // Todo TEMA 5
    //Tuplas
    //Tuplas2 o pair(pares) 2 formas de declararla
   /* val myTup2 = Tuple2(4,"iFruit")
    val mytup2b = 4->"iFruit"

    //Metodos para Tupla2 (similar a diccionario de Python)
    println(myTup2._1)//muestra clave
    println(mytup2b._2)//mestra valor
    println(myTup2.swap)// cambia la posicion de los datos.

    // Las tuplas con mas de dos elementos llegan hasta 22 usan el mismo sistema que tuple2
    val myTup5 = (4,"MeToo", "1.0", 37.5, 41.3)
    println(myTup5._3 + "/" + myTup5._5)

    val myTup6 = ("Plato", "Kant", "Voltaire", "Descartes",
      "deBeauvoir", "Camus")
    println(myTup6.productPrefix) // indica la clase de la tupla
    println(myTup6.productArity) // indica el tamaño de la tupla

    //Converti una tupla en una cadena
    val myStr = myTup6.toString()
  //pasar de cadena a tupla con una condición, la primer parte de la tupla la forma lo que cumple la condicion
    // el resto la segunda parte
    val newTup = myStr.partition(_.isUpper)//clave letras en mayuscula
    val sortedÑastNameInitials = newTup._1.sorted // mezclar los letras de la clave.

    //Uso de foreach Realizara un metodo en cada elemento de nuestra tupla
    val modelTrav = Traversable("MeToo", "Ronin", "iFruit")
    println(modelTrav)
    modelTrav.foreach(println) // el print no lleva()

    //Gestionar al memoria, Iterable guarda los datos en memoria solo cuando se utilizan
    val models = Iterable ("MeToo", "Ronin", "iFruit")
    //iteratos proporciona una manera de recorrer cada elemento una vez
    val modelIter = models.iterator
    println(modelIter.next)
    println(modelIter.next)
    println(modelIter.next)
    //Sec acceder a cada elemento usando un desplazamiento fijo.

    val mySeq = Seq("MeToo", "Ronin", "iFruit")
    println(mySeq(1))//empieza en 0

    //Set Elimina ducpliados y puedes preguntar por un valor retornando true or false
    val mySet = Set("MeToo", "Ronin", "iFruit","MeToo")
    println(mySet)
    println(mySet("Banana"))

    //Map Key -> Value
    val wifiStatus = Map(
      "disabled" -> "Wifi off",
      "enabled"-> "Wifi on but disconnected",
      "Connected" -> "Wifi on and connected")
    println(wifiStatus("enabled"))//devulve el valor de esta key
    //podemos indicar los tipos
    val myMap: Map[Int, String] = Map(1->"a",2->"b")
    // LLamamos conjunto a un iterable sin elementos duplicados.
    println(mySet)//nuestro set elimina los duplicados.
    println(mySet.size)//tamaño de mySet
    val mySet2 = mySet.drop(1)// elimina desde el inicio, indicamos cuantos queremos eliminar. no la posicion
    println(mySet2)
    println(mySet)

    // Listas son finitas e inmutables.
    val newList = "a":: "b" :: "c" :: Nil // Nil indica le final de la lista.
    val modelsL = List("Titanic", "Sorrento", "Ronin")
    println(modelsL(1), newList(2))
    //Las lista spueden contener datos simples o variados
    val randomList= List("ifruit",3, "robin", 5.2)
    //o colecciones
    val devices = List(("Sorrento", 10), ("Sorrento", 20),("Sorrento", 30))
    //Metodos para listas
    val myList: List[Int]= List(1,5,7,1,3,2)
    myList.sum
    myList.max
    myList.take(3)
    myList.sorted// ordena la lista
    myList.reverse // invierte la lista

    //Union or intersection de listas
    val myListA = List("iFruit","Sorrento", "Ronin")
    val myListB = List("iFruit", "MeToo", "Ronin")
    val myListC = myListA.union(myListB)
    val myListD = myListA++myListB
    myListC==myListD
    val myLisE = myListA.intersect(myListB)
    // Las operaciones no modifican las listas originales.
    //Para añadir a una lista elementos
    val myListF = myListA :+ "xPhone"
    // Crear lista mutables
    val listBuf = scala.collection.mutable.ListBuffer.empty[Int]
    listBuf += 17
    listBuf += 29
    listBuf += 45
    listBuf -= 17

    var listBuf2 = scala.collection.mutable.ListBuffer.empty[Int]

    listBuf2 = listBuf

    listBuf+=88
    //Aunque se modifique despues de igualar se guarda el modificado.
    println(listBuf2)

    // Array un solo tipo de elemento, mutable pero sin modificar el tamaño
    val devs = Array("iFruit", "MeToo", "Ronin")
    devs(2) = "Titanic"
    println(devs)

    //Crear Array con tamaño pero sin todos los datos.
    val device2: Array[String] = new Array[String](4)
    device2.update(0,"Sorrento")
    device2(0) = "Titanic"
    //device2(1)= 12 // Error al no ser int
    println(device2.length)// Muestra el tamaño del array en este caso 4

    // Vectores inmutables de tamaño flexible las modificaciones no se hacen en el lugar.
    var vec = Vector(1,18,6)
    println(vec.updated(2,30))
    println(vec)
    vec = vec :+5
    vec = 77 +: vec
    println(vec)

    //Map coleccion de clave, valor las claves son unicas no se repiten los valores da igual
    //Es inmutable y no permite cambiar los valores.
    val phoneStatus = Map(
      ("DTS" -> "2014-03-15:10:10:31"),
      ("Brand" -> "Titanic"),
      ("Model" -> "4000"),
      ("UID" -> "1882b564-c7e0-4315-aa24-228c0155ee1b"),
      ("DevTemp" -> 58),
      ("AmbTemp" -> 36),
      ("Battery" -> 39),
      ("Signal" -> 31),
      ("CPU" -> 15),
      ("Memory" -> 0),
      ("GPS" -> true ),
      ("Bluetooth" -> "enabled"),
      ("WiFi" -> "enabled"),
      ("Latitude" -> 40.69206648),
      ("Longitude" -> -119.4216429))
    println(phoneStatus.contains("DTS"))
   println(phoneStatus.keys) // Devuelve las Keys
   println(phoneStatus.values)//Devuelve los valores
    // para evitar excepciones de claves no existentes usamos get ot getorelse
    phoneStatus("DTS")
    //phoneStatus("Motor") // esta clave dara error por que no se encuentra.
    println(phoneStatus.get("motor"))
    println(phoneStatus.getOrElse("Motor", "No esta"))//deviolvera la opcion de no esta, pasa al else.
    //Para cambiar los valores hay que crear un mapa mutable
    val mutRec = scala.collection.mutable.Map(
      ("Brand" -> "Titanic"),
      ("Model" -> "4000"),
      ("Wireless" -> "enabled"))

    mutRec("Wireless")="disabled"
    println(mutRec)

    // Metodos para convertir entre colecciones de tipos.
    val tipList = List("Titanic", "F01L", "enabled", 32)
    val tipArray = tipList.toArray
    val tipIterable = tipList.toIterable
    val tipLista2 = tipIterable.toList
    val tipLista3 = tipArray.toList
    val tipTupla = (4, "MeToo", "1.0", 37.5, 41.3, "Enabled")
    val tipLista4 = tipTupla.productIterator.toList
    println(tipTupla.getClass)
    println(tipLista4.getClass)
    val tipString = "A Banana"
    tipString.toArray
    tipString.toList
    tipString.toSet*/



    // Todo TEMA 6 Control de flujo
    // Wile
    val sorrentoPhones = List("F00L", "F01L", "F10L", "F11L",
      "F20L", "F21L", "F22L", "F23L", "F24L")
    var i= 0
    while (i< sorrentoPhones.length){

      println(sorrentoPhones(i))
      i = i + 1
    }

    //similares pero con menos datos until te ahorras el -1
    for (i <- 0 to sorrentoPhones.length -1){
      println(sorrentoPhones(i))
    }
    for (i <- 0 until  sorrentoPhones.length ){
      println(sorrentoPhones(i))
    }

    // trabaja en intervalos de 2 en este caso po el by
    for (i <- 0 until sorrentoPhones.length by 2){
      println(sorrentoPhones(i))
    }

   // Para contar el numero de datos:
    for (i<- 0 until sorrentoPhones.length){

      println(i.toString + ":" + sorrentoPhones(i))

    }

    //Mejor forma de iterar sin conteo ni limites. el modelos ya sabe que tiene que procesar la coleccion
    for(modelo <- sorrentoPhones){

      print(modelo + " ")
    }

    //Bucles anidados:
    val phonebrands = List("iFruit", "MeToo")
    val newmodels = List("Z1", "Z-Pro")
    for (brand <- phonebrands ; model <- newmodels){

      println(brand+ ":" + model)

    }

    //Bucles con if
    for(modelo <- sorrentoPhones){

      if(modelo.contains("2"))print(modelo + " ")
    }

    for(modelo <- sorrentoPhones ; if(modelo.contains("2"))){

      print(modelo + " ")
    }

    // Yield lo usamos para que nos devuelva una lista nueva en luegar de ver uno a uno los elementos
    val phoneList =
      for (brand <- phonebrands ; model <- newmodels)
        yield brand + " " + model

    println("\n" + phoneList)

    //podemos usar iteradores
    val phones = Array("iFruit", "MeToo")
    val iter = phones.toIterator
    iter.next
    iter.next
    //iter.next // este ua dara fallo por que solo contiene 2 elementos

    // Uso de While con Iterator si ya hemos visto uno ese no lo mostramos mas.
    val titanicPhonics = List ("1000","2000","3000", "Bananas")
    val iterPhones = titanicPhonics.toIterator
    println(iterPhones.next)
    while(iterPhones.hasNext){

      print(iterPhones.next + " ")

    }

    //Usando definiciones
    val myConstant = 10
    var myVariable = 24

    def myFunction = myConstant + myVariable
    myFunction

    myVariable = 9
    myFunction
    myVariable = 20
    myFunction
    /*val myConstant = 3
    myFunction*/

    //Todas las funciones devulven algo si no es asi se autoasigna Unit
    //solo es necesario los parentesis cuando recibe parametros
    def listPhones2{
      println("MeToo")
      println("Titanic")
      println("iFruit")
    }
    listPhones2

    // definimos una funcion y le damos valores
    def CtoF (celsius: Double) = {
      (celsius *9 / 5) +32
    }
    print(CtoF(34.0))

    //funcion de ordedn superior en convert entra un Double y sale un Double
    def convertList (myListCel: List[Double], convert: (Double)=> Double): Unit ={
      for(n<-myListCel){

        println((n, convert(n)))

      }
    }
    val phoneCelsius = List (34.0, 23.5, 12.2)
    convertList(phoneCelsius,CtoF)

    //Funciones anonimas, funciones que se llaman una vez
    //(parameter: type) => {function_definition: type}
    convertList(phoneCelsius,cc => (cc*9/5)+32) // cambio la funcion por el contenido

    //foreach la_ se usa para indeicar que es el elemento sobre el que itera o quitamos el() de print
    phoneList.foreach(println(_))
    // la _ puede crear ambiguedad a la hora de dar el tipo de dato para eso lo especificamos
    phoneList.foreach(println(_).toString.toUpperCase)

    // formas de declarar un metodo Map
    phoneCelsius.map(c=> CtoF(c)) // indica el elemento que entra y el retorno de ese elemento
    phoneCelsius.map(CtoF(_)) // indicamos que hace CtoF con los datos que itera
    phoneCelsius.map(c => c*9/5+32)//c es el elemento y hace la cuenta explicita
    phoneCelsius.map(_ * 9/5+32)// Como el anterior pero con _

    // Usamos Filter
    phoneCelsius.filter(val1 =>val1 < 23)
    phoneCelsius.filter(_ <23)
    phones.filter(_.startsWith("2")) // que empiecen por 2
    phones.filter(_.length>4) //longitud mayor de 4

    // sortWith ordenar comparando elementos de nuestra coleccion
    println(phoneCelsius.sortWith((val1,val2) => val1<val2))
    phoneCelsius.sortWith(_<_)

    var myListt6: List[Int] = List(1, 5, 7, 3, 2, 1)
    myListt6.map(_+10)//sumas 10 a cada elemento
    myListt6.filter(_>4)
    myListt6.map(_ + 10).filter(_>4)

    println(titanicPhonics.filter(_.endsWith("00")).sortWith(_>_))

    //Uso Case
    val phoneWireless ="bien"
    var msg = "Radio state Unknown"

    phoneWireless match {
      case "enabled" => msg = "Radio is On"
      case "disabled" => msg = "radio is Off"
      case "connected" => msg = "Radio On, Protocol Up"
      case default => msg = "Radio state unknown"
    }
    println(msg)

    val mixedArr = Array("11", 12, "thirteen", 14.0, 'F', null)
    for (elem<- mixedArr){
      elem match {
        case elem: String => println("String: " + elem)
        case elem: Int    => println("Int: " + elem)
        case elem: Double => println("Double: " + elem)
        case elem: AnyRef => println("AnyRef: " + elem) //aqui mete F pero si cambias el orden lo reconoce como Char
        case elem: Char   => println("Char: " + elem)
        case null         => println("Found Null")
      }
    }


    //Uso de SOME Y NONE
    val superPhone = Some("Model 6") //declaro la variable con some
    println(superPhone.getOrElse("Not Found")) // al tener some devuelve el contenido de lo contrario realiza la accion
    val superPhone2 = None
    println(superPhone2.getOrElse("Not Found"))// en este caso nos devuelve Not Found ya que no hay some


    // Uso de OPTION en funciones
    //Devulve un valor encapsulado en Some/None para poder eleguir que accion a tomar.
    def str2Double(in: String): Option[Double]= {
      try{
        Some(in.toDouble)
      }catch {
        case e: NumberFormatException => None
      }
    }
    println(str2Double("2.5").getOrElse("Dato malo"))
    println(str2Double("Warm").getOrElse("Dato malo"))

    def convert2Float(x: Option[Any]) = x match {
      case Some(d: Double) => d.toFloat
      case Some(i: Int) => i.toFloat
      case Some(f: Float) => f
      case Some(_: Any) => println("Invalid data provided.")
      case None => println("No data provided.")
    }

    println(convert2Float(Some(8)))
    println(convert2Float(Some("")))
    println(convert2Float(None))


    //Funciones PARCIALES
    // Permiten controlar los datos de entrada antes de realizas la funcion
    //Para devolver datos solo a un subconjunto de posibles valores de entrada Ej divisionpor 0
    val div = (x: Int) => 24 /x // 0 daria un error aritmetico, evitemos esto.
    val div2 = new PartialFunction[Int, Int] {
      def apply (x:Int) = 24 / x //indica la accion de nuestra funciuon
      def isDefinedAt (x:Int) = x != 0 // indica la restriccion de los datos de entrada
    }
    println("FUNCIONES PARCIALES")
    println(div2(2))
    println(div2.isDefinedAt(0))
    println(div2.isDefinedAt(2))
    println(if (div2.isDefinedAt(2)) div2(2))

    //cuando una funcion pàrcial tiene case se genean solos apply y isDefinedAt
    val getThirdItem : PartialFunction[List[Int], Int] = {

      case x :: y :: z :: _ => z

    }

    getThirdItem.isDefinedAt(List(25)) // return False faltan elementos
    getThirdItem.isDefinedAt(List(25, 35, 45, 85)) // true esta compuesto por 4 elementos
    println(getThirdItem(List(25, 35, 45, 85)))







  }
}
