import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.io.File

import java.sql.DriverManager
import java.time.LocalDateTime

object Project2 {

def run():Unit = {
  
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


val nmb = sc.getConf.get("spark.driver.args")(0)

  val schema = StructType(
    Seq(
      StructField("InvoiceNo", StringType, true),
      StructField("StockCode", StringType, true),
      StructField("Quantity", StringType, true),
      StructField("InvoiceDate", StringType, true),
      StructField("CustomerID", StringType, true),
      StructField("Country", StringType, true)
    ))
    
val warehouseLocation = new File("spark-warehouse").getAbsolutePath

 val spark =
    SparkSession
      .builder()
      .appName("FeatureExtractor")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate();

  val invoices = spark
    .read
    .schema(schema)
    .option("delimiter", "|")
    .csv("hdfs://namenode:8020/user/test/invoices/" + nmb + "/")  

  print(invoices.count())
  invoices.show(100)
  invoices.printSchema()

  
  invoices.write.mode(SaveMode.Append).saveAsTable("table3")
}
}
Project2.run()
