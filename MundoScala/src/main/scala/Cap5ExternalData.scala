import org.apache.spark.sql.SparkSession

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

    // Registramos el UDF
    spark.udf.register("cubed", cubed)

    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    //Realizamos una query sobre esta vista
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    // todo el strlen no lo reconoce ni s ni tests1
    spark.sql("SELECT s FROM test1 WHERE s IS NOT NULL AND strlen(s) > 1")
    spark.sql("SELECT cubed(s) FROM udf_test WHERE cubed(s) IS NOT NULL AND length(cubed(s)) > 1").show()


  }

}
