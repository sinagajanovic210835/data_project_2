import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.LocalDateTime

object SparkApp extends App {

  val ISOcodes = Map("Norway" -> "NOR", "Netherlands" -> "NLD", "France" -> "FRA", "Germany" -> "DEU", "Australia" -> "AUS", "United Kingdom" -> "GBR",
                 "Ireland" -> "IRL", "Italy" -> "ITA", "Switzerland" -> "CHE", "Portugal" -> "PRT", "Spain" -> "ESP", "Belgium" -> "BEL", "Poland" -> "POL",
                 "Japan" -> "JPN", "Lithuania" -> "LTU", "Iceland" -> "ISL", "Malta" -> "MLT", "Denmark" -> "DNK", "Unspecified" -> "", "RSA" -> "ZAF",
                 "United Arab Emirates" -> "ARE", "Canada" -> "CAN", "Cyprus" -> "CYP", "Channel Islands" -> "GBR", "Brazil" -> "BRA", "Israel" -> "ISR",
                 "Austria" -> "AUT", "Finland" -> "FIN", "USA" -> "USA", "Bahrain" -> "BHR", "Greece" -> "GRC", "Hong Kong" -> "HKG", "Saudi Arabia" -> "SAU",
                 "Singapore" -> "SGP", "European Community" -> "", "Lebanon" -> "LBN", "Czech Republic" -> "CZE", "Sweden" -> "SWE")
 
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val db = Map("user" -> "postgres",
              "password" -> "postgres",
              "driver" -> "org.postgresql.Driver",
              "url" -> "jdbc:postgresql://postgres:5432/postgres",
              "truncate" -> "true")
  val spark =
    SparkSession
      .builder()
      .appName("SavePostgres")
      .config("spark.master", "local")
      .getOrCreate();

  val schemaProd = StructType(
    Seq(
      StructField("StockCode", StringType, true),
      StructField("Description", StringType, true),
      StructField("UnitPrice", DecimalType(10, 2), true),
      StructField("Price_On_Date", TimestampType, true)
    ))

  val schemaInvoice = StructType(
    Seq(
      StructField("InvoiceNo", StringType, true),
      StructField("StockCode", StringType, true),
      StructField("Quantity", IntegerType, true),
      StructField("InvoiceDate", TimestampType, true),
      StructField("CustomerID", StringType, true),
      StructField("Country", StringType, true),
      StructField("Region", StringType, true)
    ))

  val schemaCountry = StructType(
    Seq(
      StructField("CountryCode", StringType, true),
      StructField("CountryName", StringType, true),
      StructField("ISOCountryCode", StringType, true)
    ))

  val productEncoder = RowEncoder(schemaProd)
  val countryEncoder = RowEncoder(schemaCountry)
  val invoiceEncoder = RowEncoder(schemaInvoice)

  // Function to convert time string to timestamp type

  def convertTime(timeString: String): Timestamp = {
    val str = timeString.split(" ")
    val date = str(0).split("/")
    val time = str(1).split(":")
    val (day, month, yr) = (date(1).toInt, date(0).toInt, date(2).toInt)
    val (hour, minute) = (time(0).toInt, time(1).toInt)
    Timestamp.valueOf(LocalDateTime.of(yr, month, day, hour, minute))
  }

  def mapFuncCountry(r: Row): Row =  Row(r(0), r(1), ISOcodes(r(1).toString))
  def mapFuncProduct(r: Row): Row = Row(r(0), r(1), r(2), convertTime(r(3).toString))
  def mapFuncInvoice(r: Row): Row = {
        val regions = Map[String, String](
          "1" -> "North",
          "2" -> "South",
          "3" -> "East",
          "4" -> "West",
          "5" -> "Center",
          "6" -> "Islands or peripheral territories")
        val country_region = r(5).toString.split("-")
        Row(r(0), r(1), r(2), convertTime(r(3).toString), r(4), country_region(0), regions(country_region(1)))
  }

  try {

    val products  = spark.read.parquet("hdfs://namenode:8020/user/hive/warehouse/products/").map(mapFuncProduct)(productEncoder)

    val countries = spark.read.parquet("hdfs://namenode:8020/user/hive/warehouse/countries/").map(mapFuncCountry)(countryEncoder)

    val invoices  = spark.read.parquet("hdfs://namenode:8020/user/hive/warehouse/invoices/").map(mapFuncInvoice)(invoiceEncoder)

    val invoicesProductsCountries = invoices.as("invoices")
                                      .join(products.as("products"),
                                        col("invoices.InvoiceDate") === col("products.Price_On_Date") &&
                                        col("invoices.StockCode") === col("products.StockCode"), "left")
                                      .withColumn("total", col("invoices.Quantity") * col("products.UnitPrice"))
                                      .drop(col("products.StockCode"))
                                      .alias("inv_prod")
                                      .join(countries.as("countries"),
                                        col("inv_prod.Country") === col("countries.CountryCode"), "left")
                                      .drop("Price_On_Date", "Country")
                                      .orderBy("InvoiceDate")

    println("invoicesProductsCountries")
    invoicesProductsCountries.show(20, false)

    // invoicesProductsCountries.write
    // .format("jdbc")
    // .options(db + ("dbtable" -> "invoices_products_countries"))
    // .mode(SaveMode.Overwrite)
    // .save()


    val invoicesTotal = invoicesProductsCountries
                                      .filter(col("CustomerId").isNotNull && col("total").isNotNull)
                                      .select(col("InvoiceNo"), col("InvoiceDate"), col("CustomerID"), col("CountryName"),
                                        col("Region"), col("ISOCountryCode"), col("total"))
                                      .groupBy(col("InvoiceNo"), col("InvoiceDate"), col("CustomerID"), col("CountryName"),
                                        col("Region"), col("ISOCountryCode"))
                                      .agg(sum(col("total")).as("total"))
                                      .orderBy(col("InvoiceDate"), col("InvoiceNo"))
                                      .cache()

    println("invoicesTotal")
    invoicesTotal.show(20, false)

    val topBuyersPerUser = invoicesTotal
                                   .select("CustomerID", "total")
                                   .groupBy("customerId")
                                   .agg(sum(col("total")).alias("total_spent"))
                                   .orderBy(col("total_spent").desc)

    println("topBuyersPerUser")
    topBuyersPerUser.show(20, false)

    val window1 = Window.partitionBy("CountryName", "Region").orderBy(col("total_spent").desc)

    val topByersPerCountryRegion = invoicesTotal.select(col("CustomerID"), col("CountryName"), col("Region"), col("ISOCountryCode"), col("total"))
                                  .groupBy(col("CustomerID"), col("CountryName"), col("Region"), col("ISOCountryCode"))
                                  .agg(sum(col("total")).alias("total_spent"))
                                  .orderBy(col("CountryName").desc, col("Region"))
                                  .withColumn("row", row_number().over(window1))
                                  .filter(col("row") === 1).drop("row")
                                  .orderBy(col("CountryName"), col("Region"))
                                  .drop("row")

    println("buyersPerCountryRegion")
    topByersPerCountryRegion.show(20, false)

    val customerMoreThenOneCountry = invoicesTotal
                                .select(col("CustomerID"), col("CountryName"))
                                .distinct()
                                .groupBy(col("CustomerId"))
                                .count()
                                .filter("count > 1")

    println("customerMoreThenOneCountry")
    customerMoreThenOneCountry.show()

    val customerMoreThenOneRegion = invoicesTotal
                                .select(col("CustomerID"),col("Region"))
                                .distinct()
                                .groupBy("CustomerID")
                                .count()
                                .filter("count > 1")
                                .orderBy(col("count").desc)

    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    println("customerMoreThenOneRegion")
    customerMoreThenOneRegion.show(100, false)

    val usersOrders = invoicesTotal
                                .select("invoiceNo", "CustomerID").distinct()
                                .groupBy("CustomerID")
                                .agg(count("CustomerID")
                                .as("count"))
                                .orderBy(col("count").desc)
                                .persist()

    val usersWithMoreThanOneOrder = usersOrders.filter("count > 1")

    println("usersWithMoreThanOneOrder")
    usersWithMoreThanOneOrder.show(20, false)

    val usersWithOneOrder = usersOrders.filter("count < 2")

    println("usersWithOneOrder")
    usersWithOneOrder.show(100,false)

  } catch {
      case e: AnalysisException =>  e.printStackTrace(); println("Something went wrong!!!!!!!!!!!!!!!!!!!!!!!")
  }
}