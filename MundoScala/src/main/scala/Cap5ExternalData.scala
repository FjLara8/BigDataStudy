import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

object Cap5ExternalData {

  def main(args: Array[String]) {


    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("Cap5ExternalData")
      .getOrCreate()

    import  org.apache.spark.sql.functions._


    // Creamos una funcion llamada cubed
    val cubed = (s: Long) => {
      s * s * s
    }
    val cubetoudf = udf(cubed) // Asi si queda registrado para usarlo como DataFrame

    // Registramos el UDF
    spark.udf.register("cubed", cubed) // Esto queda registrado solo para Spark.SQL

    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    //Realizamos una query sobre esta vista
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    // todo el strlen no lo reconoce ni s ni tests1
    //spark.sql("SELECT s FROM test1 WHERE s IS NOT NULL AND strlen(s) > 1")
    //spark.sql("SELECT s FROM test1 WHERE s IS NOT NULL AND length(s) > 1")
    //spark.sql("SELECT cubed(s) FROM udf_test WHERE cubed(s) IS NOT NULL AND length(cubed(s)) > 1").show()

    //
    val df = spark.range(1,4)

    //df.select("id", cubed_udf(col("id"))).show()
    df.select(col("id"), cubetoudf(col("id"))).show()

    // todo pag 132 postgre sql 145 ejercicio


    //todo IMP si lo usas mirar nombres de las variables para que no se produzca conflicto.
    //PostgreSQL
    // Conectar y leer desde PostgreSQL mediante JDBC // Read Option 1: Loading data from a JDBC source using load method
    /*val jdbcDF1 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:[DBSERVER]")
      .option("dbtable", "[SCHEMA].[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .load()*/

    // Read Option 2: Loading data from a JDBC source using jdbc method
    // Create connection properties
    /*import java.util.Properties
    val cxnProp = new Properties()
    cxnProp.put("user", "[USERNAME]")
    cxnProp.put("password", "[PASSWORD]")*/

    // Load data using the connection properties
   /* val jdbcDF2 = spark
      .read
      .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)*/

    // Write Option 1: Saving data to a JDBC source using save method
   /* jdbcDF1
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql:[DBSERVER]")
      .option("dbtable", "[SCHEMA].[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .save()*/
    // Write Option 2: Saving data to a JDBC source using jdbc method
    /*jdbcDF2.write
      .jdbc(s"jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)*/


    //MySQL

    // Loading data from a JDBC source using load
    /*val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .load()

    // Saving data to a JDBC source using save
    jdbcDF
      .write
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .save()*/


    //Azure Cosmos DB
    // Import necessary libraries
    //import com.microsoft.azure.cosmosdb.spark.schema._
    //import com.microsoft.azure.cosmosdb.spark._
    //import com.microsoft.azure.cosmosdb.spark.config.Config
    // Loading data from Azure Cosmos DB
    // Configure connection to your collection
    /*val query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"
    val readConfig = Config(Map(
      "Endpoint" -> "https://[ACCOUNT].documents.azure.com:443/",
      "Masterkey" -> "[MASTER KEY]",
      "Database" -> "[DATABASE]",
      "PreferredRegions" -> "Central US;East US2;",
      "Collection" -> "[COLLECTION]",
      "SamplingRatio" -> "1.0",
      "query_custom" -> query
    ))
    // Connect via azure-cosmosdb-spark to create Spark DataFrame
    val df = spark.read.cosmosDB(readConfig)
    df.count
    // Saving data to Azure Cosmos DB
    // Configure connection to the sink collection
    val writeConfig = Config(Map(
      "Endpoint" -> "https://[ACCOUNT].documents.azure.com:443/",
      "Masterkey" -> "[MASTER KEY]",
      "Database" -> "[DATABASE]",
      "PreferredRegions" -> "Central US;East US2;",
      "Collection" -> "[COLLECTION]",
      "WritingBatchSize" -> "100"
    ))

    // Upsert the DataFrame to Azure Cosmos DB
    import org.apache.spark.sql.SaveMode
    df.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)*/


    //MS SQL Server


    // Loading data from a JDBC source
    // Configure jdbcUrl
    /*val jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"
    // Create a Properties() object to hold the parameters.
    // Note, you can create the JDBC URL without passing in the
    // user/password parameters directly.
    val cxnProp = new Properties()
    cxnProp.put("user", "[USERNAME]")
    cxnProp.put("password", "[PASSWORD]")
    cxnProp.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    // Load data using the connection properties
    val jdbcDF = spark.read.jdbc(jdbcUrl, "[TABLENAME]", cxnProp)
    // Saving data to a JDBC source
    jdbcDF.write.jdbc(jdbcUrl, "[TABLENAME]", cxnProp)*/

    /*val jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"
    // Loading data from a JDBC source
    val jdbcDF = (spark
      .read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .load())
    // Saving data to a JDBC source
      jdbcDF
        .write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "[TABLENAME]")
        .option("user", "[USERNAME]")
        .option("password", "[PASSWORD]")
        .save())*/


    //Manipular tipos de datos complejos.

    //En SQL osando Explode and collect
   /* spark.sql("""SELECT id, collect_list(value + 1) AS values
                |FROM (SELECT id, EXPLODE(values) AS value
                | FROM table) x
                |GROUP BY id
                |""".stripMargin)

    //En SQL Usando Defined Function
    spark.sql("""SELECT id, collect_list(value + 1) AS values
                |FROM (SELECT id, EXPLODE(values) AS value
                | FROM table) x
                |GROUP BY id
                |""".stripMargin)*/

    import spark.implicits._

//todo 142
    //crear un a array y una vista usando otros dos arrays
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    val tC=Seq (t1,t2).toDF("celsius")
    tC.createOrReplaceTempView("tC")
    tC.show()

    // esto tambien concatena 2 arrays
    /*val tC = Array(t1,t2)
    tC.take(12).foreach{
      x => println(x.mkString(sep = ","))
    }
     //val empDataFrame = Seq(("Alice", 24), ("Bob", 26)).toDF("name","age")*/

    // Transform() cambia el contenido del array aplicando una funcion similar a map (para todos los valores).
    // transform(array<T>, function<T, U>): array<U>
    // Calculate Fahrenheit from Celsius for an array of temperatures
    spark.sql("""SELECT celsius,transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
    FROM tC """).show()

    // Filter() produce un array en funcion del filtro que se le aplique a los valores
    // filter(array<T>, function<T, Boolean>): array<T>
    // Filter temperatures > 38C for array of temperatures
    spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high
    FROM tC""").show()

    // Exist() exists() devuelve true si la función es válida para cualquier elemento de la matriz
    // exists(array<T>, function<T, V, Boolean>): Boolean
    // Is there a temperature of 38C in the array of temperatures
    spark.sql("""SELECT celsius, exists(celsius, t -> t = 38) as threshold
    FROM tC""").show()

    // Reduce() reduce los elementos a un solo valor fusionando los elementos usando la función<B, T, B> y aplicando una función de acabado
    //función<B, R> en el buffer final:
    //reduce(array<T>, B, function<B, T, B>, function<B, R>)
    // Calculate average temperature and convert to F
    spark.sql("""SELECT celsius, reduce(celsius, 0, (t, acc) -> t + acc,
    acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit FROM tC""").show()






  }

}
