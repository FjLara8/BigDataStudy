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
    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "employees")
      .option("user", "MySQL80")
      .option("password", "LaraMySql")
      .load()

    // Saving data to a JDBC source using save
   /* jdbcDF
      .write
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .save()*/

    /*<groupId>mysql</groupId>
      <artifactId>mysql-connector-java</artifactId>
      <version>5.1.38</version>
    </dependency>*/

  }
}
