import org.apache.spark.sql.SparkSession
import java.sql.{Connection,DriverManager}
object Cap5EjConect {

  def main(args: Array[String]) {


    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("Cap5ExternalData")
      .getOrCreate()


    //MySQL

    // Loading data from a JDBC source using load
    /*val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "employees")
      .option("user", "MySQL80")
      .option("password", "LaraMySql")
      .load()*/

    /*<groupId>mysql</groupId>
      <artifactId>mysql-connector-java</artifactId>
      <version>5.1.38</version>
    </dependency>*/



    //MS SQL Server
    //name server L2206002
    import java.util.Properties
    // Loading data from a JDBC source
    // Configure jdbcUrl
    // todo en database= L2206002 tambien lo acepta aunque falla igual el inicio de sesion
    val jdbcUrl = "jdbc:sqlserver://localhost:1434;database=AdventureWorks2008R2; encrypt=false; trustStore= C:/Program Files/Java/jdk-18.0.1.1/lib/security/cacerts;trustStorePassword=changeit"
    // Create a Properties() object to hold the parameters.
    // Note, you can create the JDBC URL without passing in the
    // user/password parameters directly.
    val cxnProp = new Properties()
    cxnProp.put("user", "UsuarioLocal2")
    cxnProp.put("password", "LocalUsuario08")
    cxnProp.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    // Load data using the connection properties
    val jdbcDF = spark.read.jdbc(jdbcUrl, "[Person].[Person]", cxnProp)
    // Saving data to a JDBC source
    //jdbcDF.write.jdbc(jdbcUrl, "[Person].[Person]", cxnProp)

    // todo da error al conectar el usuario aunque usuario y contraseña son correctos y en sql funcionan
    // todo no conecta ni para meter el usuario ya que no sabe si es o no ese usuario
    // todo si reconoce la direccion
  }
}
