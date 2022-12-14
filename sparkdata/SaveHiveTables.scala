import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.io.File
import scala.util.{Failure, Success, Try}

object SaveHiveTables {

  def run():Unit = {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath    

    
    val args      = sc.getConf.get("spark.driver.args").split("_")
    val command   = args(0)
    val directory = if (args.length > 1) args(1) else "" 

     val schemaCountry = StructType(
      Seq(
        StructField("CountryCode", StringType, true),
        StructField("CountryName", StringType, true)       
      )) 

      val schemaProduct = StructType(
      Seq(
        StructField("StockCode", StringType, true),
        StructField("Description", StringType, true),
        StructField("UnitPrice", DecimalType(10,2), true),
        StructField("Price_On_Date", StringType, true)      
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

    val setInvoices  = Set("productcountryinvoice", "productinvoice", "countryinvoice", "invoice")
    val setCountries = Set("productcountryinvoice", "countryinvoice", "productcountry", "country")  
    val setProducts  = Set("productcountryinvoice", "productinvoice", "productcountry", "product")
    
    val success = Try[Unit]{

     if(setCountries.contains(command)) {
        val countries = spark
            .read
            .schema(schemaCountry)
            .option("delimiter", "|")
            .csv("hdfs://namenode:8020/user/test/countries/")

            countries.write.mode(SaveMode.Append).saveAsTable("countries")       
    }  
    
    if(setProducts.contains(command)) {      
        val products = spark
            .read
            .schema(schemaProduct)
            .option("delimiter", "|")
            .csv("hdfs://namenode:8020/user/test/products/")  

            products.write.mode(SaveMode.Append).saveAsTable("products")       
    }
    
    if(setInvoices.contains(command)) {      
        val invoices = spark
            .read
            .schema(schemaInvoice)
            .option("delimiter", "|")
            .csv("hdfs://namenode:8020/user/test/invoices/" + directory)

          invoices.write.mode(SaveMode.Overwrite).saveAsTable("invoices")  
    }
    }
    success match {

          case Success(_)   => println("Job Succeeded")

          case Failure(e)   => e.printStackTrace()
                                 println("\nTRALALALALALA!!!!!!!!!!!!!!!!")

   }
   
  }
}
SaveHiveTables.run()