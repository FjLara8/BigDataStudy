import org.apache.spark.sql.SparkSession

object padron7 {


    def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .master("local[1]")
        .appName("padron7")
        .getOrCreate()


      spark.sql(
        """create table if not exist datospadron_txt (
          |COD_DISTRITO int,
          |DESC_DISTRITO string,
          |COD_DIST_BARRIO int,
          |DESC_BARRIO string,
          |COD_BARRIO int,
          |COD_DIST_SECCION int,
          |COD_SECCION int,
          |COD_EDAD_INT int,
          |EspanolesHombres int,
          |EspanolesMujeres int,
          |ExtranjerosHombres int,
          |ExtranjerosMujeres int)
          |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
          |WITH SEDEPROPERTIES(
          |'separatorChar' = ';',
          |'quoteChar' = ' ')
          |Stored as textfile
          |TBLPROPERTIES("skip.header.line.count"="1")
          |LOAD DATA LOCAL INPATH 'Rango_Edades_Seccion_202208.csv';
          |""".stripMargin)

    }
}
