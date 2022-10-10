import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.util.{Failure, Success, Try}
import java.io.File

object SaveInvoices {

  def run():Unit = {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val args = sc.getConf.get("spark.driver.args").split("\\s+")
  val nmb = args(0)
  val bool:Boolean = args(1).toBoolean

    val schemaProduct = StructType(
      Seq(
        StructField("StockCode", StringType, true),
        StructField("Description", StringType, true),
        StructField("UnitPrice", DoubleType, true),
        StructField("Price_On_Date", StringType, true)      
      ))

    val schemaCountry = StructType(
      Seq(
        StructField("CountryCode", StringType, true),
        StructField("CountryName", StringType, true)       
      ))

    val schemaInvoice = StructType(
      Seq(
        StructField("InvoiceNo", StringType, true),
        StructField("StockCode", StringType, true),
        StructField("Quantity", IntegerType, true),
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
      .schema(schemaInvoice)
      .option("delimiter", "|")
      .csv("hdfs://namenode:8020/user/test/invoices/" + nmb + "/")  

    print(invoices.count())
    invoices.show(100)
    invoices.printSchema()
      
    invoices.write.mode(SaveMode.Append).saveAsTable("invoices")

    println(nmb)
    println(bool)

    if(bool){
        val prod:Try[Unit] = Try{  
          val products = spark
          .read
          .schema(schemaProduct)
          .option("delimiter", "|")
          .csv("hdfs://namenode:8020/user/test/products/")  

          products.write.mode(SaveMode.Append).saveAsTable("products")
        }

        prod match {
          case Success(_) => println("PRODUCTS SAVED")
          case Failure(_) => println("product failed")
        }

        val cntr:Try[Unit] = Try{ 
          val countries = spark
          .read
          .schema(schemaCountry)
          .option("delimiter", "|")
          .csv("hdfs://namenode:8020/user/test/countries/")

          countries.write.mode(SaveMode.Append).saveAsTable("countries")
        }

        cntr match {
          case Success(_) => println("COUNTRIES SAVED")
          case Failure(_) => println("no countries")
        } 

    }
  }
}
SaveInvoices.run()
