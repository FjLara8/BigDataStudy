import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, year}

object Cap6Dataset {

  //Al crear case class hay que tener lo tipos de datos que coincidan con los del archivo si no printSchema y se realiza
  case class Bloggers(id: Long, first: String, last: String, url: String, published: String,
                      hits: Long, campaigns: Array[String])

  //Para crear el DS nuevo con ramdon
  case class Usage(uid:Int, uname:String, usage: Int)

  // Nuevo Case Calss para poder añadir una columna a Usage
  case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)

  case class Person(id: Long, firstName: String, lastName: String,
    gender: String, birthDate: String, salary: Long,ssn: String)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("Cap6Dataset")
      .getOrCreate()


    // CReamos un dataset usando Clase Case
    import spark.implicits._


    val bloggers = "blogs.json"
    val bloggersDS = spark
      .read
      .format("json")
      .option("path", bloggers)
      .load()
      .as[Bloggers]

    //Creamos un DataSet usando Ramdon con 1000 estancias
    // In Scala
    import scala.util.Random._
    // Our case class for the Dataset

    val r = new scala.util.Random(42)
    // Create 1000 instances of scala Usage class
    // This generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
        r.nextInt(1000)))
    // Create a Dataset of Usage typed data
    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)

    // Nunciones de orden superior o lambda no recomendadas por serializar y deserializar
    // No todas la lambda devuelven valores booleanos, pueden hacer calculos
    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    // No todas la lambda devuelven valores booleanos, pueden hacer calculos
    // Use an if-then-else lambda expression and compute a value
    dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 })
      .show(5, false)

    // Define a function to compute the usage todo otra forma de hacerlo
    def computeCostUsage(usage: Int): Double = {
      if (usage > 750) usage * 0.15 else usage * 0.50
    }
    dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

    //Crea una clase Scala case o JavaBean, UsageCost, con un campo o columna adicional llamada coste.
    def computeUserCostUsage(u: Usage): UsageCost = {
      val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
      UsageCost(u.uid, u.uname, u.usage, v)
    }
      //Defina una función para calcular el coste y utilícela en el método map()
      dsUsage.map(u => {
        computeUserCostUsage(u)
      }).show(5)

    // todo Minimizar cosates en las consultas. dejamos los lambda y usamos expresiones normales.
    //imginamos que tenemos un DS de personal

    val personruta = "person.json"
    val personDS = spark
      .read
      .format("json")
      .option("path", personruta)
      .load()
      .as[Person]

    //La consulta mas lenta seria:
    import java.util.Calendar
    val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40
    personDS
    // Everyone above 40: lambda-1
    .filter(x => x.birthDate.split("-")(0).toInt > earliestYear)

    // Everyone earning more than 80K
    .filter($"salary" > 80000)
    // Last name starts with J: lambda-2
    .filter(x => x.lastName.startsWith("J"))

    // First name starts with D
    .filter($"firstName".startsWith("D"))
    .count()

    //La forma mas economica de hacerlo es:todo te ahorras el serializar y deserializar.
    personDS
      .filter(year($"birthDate") > earliestYear) // Everyone above 40
      .filter($"salary" > 80000) // Everyone earning more than 80K
      .filter($"lastName".startsWith("J")) // Last name starts with J
      .filter($"firstName".startsWith("D")) // First name starts with D
      .count()

  }
}
