import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .master("local[1]")
  .appName("SQL1")
  .getOrCreate()