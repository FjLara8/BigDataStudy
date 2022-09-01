import org.apache.spark
import org.apache.spark.sql.SparkSession

object cap7optimizacion {



  def printConfigs(session: SparkSession) = {
    // Get conf
    val mconf = session.conf.getAll
    // Print them
    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
  }

  def main(args: Array[String]) {
    // Create a session
    val spark = SparkSession.builder
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.executor.memory", "2g")
      .master("local[*]")
      .appName("SparkConfig")
      .getOrCreate()
    printConfigs(spark)
    spark.conf.set("spark.sql.shuffle.partitions",
      spark.sparkContext.defaultParallelism)
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    printConfigs(spark)

    spark.sql("SET -v").select("key", "value").show(5, false)

  }

  /*spark.driver.host -> 10.8.154.34
  spark.driver.port -> 55243
  spark.app.name -> SparkConfig
  spark.executor.id -> driver
  spark.master -> local[*]
  spark.executor.memory -> 2g
    spark.app.id -> local-1580162894307
  spark.sql.shuffle.partitions -> 5*/


}
