import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.io.File

object SaveInvoices {

  def run():Unit = {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

 
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

    val args = sc.getConf.get("spark.driver.args").split("_")
    val strng = args(0)
    val direc = if (args.length > 1) args(1) else "" 
    
    if(strng.equals("procntinv") || strng.equals("proinv") || strng.equals("cntinv") || strng.equals("inv")) {      
        val invoices = spark
            .read
            .schema(schemaInvoice)
            .option("delimiter", "|")
            .csv("hdfs://namenode:8020/user/test/invoices/" + direc + "/")  

          invoices.write.mode(SaveMode.Append).saveAsTable("invoices")
    }

    if(strng.equals("procntinv") || strng.equals("proinv") || strng.equals("procnt") || strng.equals("pro")) {      
        val products = spark
            .read
            .schema(schemaProduct)
            .option("delimiter", "|")
            .csv("hdfs://namenode:8020/user/test/products/")  

            products.write.mode(SaveMode.Append).saveAsTable("products")
       
    }

    if(strng.equals("procntinv") || strng.equals("cntinv") || strng.equals("procnt") || strng.equals("cnt")) {
        val countries = spark
            .read
            .schema(schemaCountry)
            .option("delimiter", "|")
            .csv("hdfs://namenode:8020/user/test/countries/")

            countries.write.mode(SaveMode.Append).saveAsTable("countries")
       
    }  
  }
}
SaveInvoices.run()
