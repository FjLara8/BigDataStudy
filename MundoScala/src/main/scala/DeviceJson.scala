import jdk.jfr.internal.handlers.EventHandler.timestamp
import org.apache.hadoop.shaded.org.jline.keymap.KeyMap.display
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.functions.{asc, avg, col, desc}
import org.apache.spark.sql.{Column, SparkSession, functions => F}



object DeviceJson {
//Las case class van fuera del main.

  case class DeviceIoTData (battery_level: Long, c02_level: Long,
                            cca2: String, cca3: String, cn: String, device_id: Long,
                            device_name: String, humidity: Long, ip: String, latitude: Double,
                            lcd: String, longitude: Double, scale:String, temp: Long,
                            timestamp: Long)

  case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
                                 cca3: String)

  case class Deviceco2 (c02_level: Long, cca2: String, cca3: String, cn: String, device_name: String)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("DeviceJson")
      .getOrCreate()


    import org.apache.spark
    import spark.implicits._

    //mostrar por posicion indicando el tipo.
    val row = Row(350, true, "Learning Spark 2E", null)
    println("Primera" + row.getInt(0))
    println("Segunda" +row.getBoolean(1))
    println("Tercera" +row.getString(2))




    val ds = spark.read
      .json("iot_devices.json")
      .as[DeviceIoTData]

    ds.show()


    val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70}})
      filterTempDS.show(5, false)



    // filtramos por temperatura y mostramos las columnas de map convertidas en DF
    val dsTemp = ds
      .filter(d => {d.temp > 25})
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]
      .show(5, false)


    // Otra forma de hacer lo de arriba
    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
      .where("temp > 25")
      .as[DeviceTempByCountry]
      .show(5,false)

    /*val dsTemp2 = ds
      .select($"battery_level", $"device_name", $"device_id")
      .where("battery_level < 5")
      .show(10,false)*/


    // mostrar los paises con menos CO2
    val dsTemp2 = ds
      .select($"c02_level", $"cca2", $"cca3", $"cn", $"device_name")
      .orderBy("c02_level")
      .as[Deviceco2]
      .show(10,false)


    // mostrar los maximos y minimos de diferentes columnas
    val DsMaxMin = ds
      .select(F.max("temp"),F.min("temp"),F.max("battery_level"),F.min("battery_level"),
        F.max("c02_level"), F.min("c02_level"),F.max("humidity"), F.min("humidity"))
      .show()


    // agrupar por media de temperatura pais, CO2 y humedad
    val DsOrder = ds
      .select($"temp",$"cn",$"c02_level",$"humidity")
      .groupBy("cn","c02_level", "humidity")
      .agg(avg("temp").alias("Media"))
      .orderBy((asc("Media")))
      .show()




    /*val dsTemp = ds
      .filter("temp > 25")
      .map("temp","device_name", "device_id", "cca3")
      .toDF("temp", "device_name", "device_id", "cca3")*/

      //filterTempDS: org.apache.spark.sql.Dataset[DeviceIoTData] = [battery_level...]
      //filterTempDS.show(5, false)



  }
}
