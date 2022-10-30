import jakarta.mail.internet.{InternetAddress, MimeMessage}
import jakarta.mail.{Message, PasswordAuthentication, Session, Transport}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object SavePostgres  extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  val host = "smtp.gmail.com"
  val props = new Properties
  props.put("mail.smtp.auth", "true")
  props.put("mail.smtp.ssl.enable", "true")
  props.put("mail.smtp.host", host)
  props.put("mail.smtp.port", "465")

  def messageText(r:Row) = {
    val text = s"Invoice No: ${r(0).toString}\nStock Code: ${r(1).toString}\nInvoice Date: ${r(2).toString}\nCustomer ID: ${r(3).toString}\n" +
                s"Description: ${r(4).toString}\nUnit Price: ${r(5).toString}\nCountry Name: ${r(6).toString}\nRegion: ${r(7).toString}\nMedian: ${r(8).toString}\n" +
                s"Standard Deviation: ${r(9).toString}\nQuantity Ordered: ${r(10).toString}\nAlarm: ${r(11).toString}"

          val session = Session.getInstance(props, new jakarta.mail.Authenticator() {
            override protected def getPasswordAuthentication = new PasswordAuthentication("sinagajanovic", "zzqitezvxpyviisw")
          })
          val message = new MimeMessage(session)
          message.setFrom(new InternetAddress("sinagajanovic@gmail.com"))
          message.setRecipient(Message.RecipientType.TO, new InternetAddress("sinagajanovic@gmail.com"))
          message.setSubject("Order over two standard deviations over median")
          message.setText(text)
          Transport.send(message)
        }


  val db = Map("user" -> "postgres",
    "password" -> "postgres",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://postgres:5432/postgres",
  )
    val ISOcodes = Map("Norway" -> "NOR", "Netherlands" -> "NLD", "France" -> "FRA", "Germany" -> "DEU", "Australia" -> "AUS", "United Kingdom" -> "GBR",
      "Ireland" -> "IRL", "Italy" -> "ITA", "Switzerland" -> "CHE", "Portugal" -> "PRT", "Spain" -> "ESP", "Belgium" -> "BEL", "Poland" -> "POL",
      "Japan" -> "JPN", "Lithuania" -> "LTU", "Iceland" -> "ISL", "Malta" -> "MLT", "Denmark" -> "DNK", "Unspecified" -> "", "RSA" -> "ZAF",
      "United Arab Emirates" -> "ARE", "Canada" -> "CAN", "Cyprus" -> "CYP", "Channel Islands" -> "GBR", "Brazil" -> "BRA", "Israel" -> "ISR",
      "Austria" -> "AUT", "Finland" -> "FIN", "USA" -> "USA", "Bahrain" -> "BHR", "Greece" -> "GRC", "Hong Kong" -> "HKG", "Saudi Arabia" -> "SAU",
      "Singapore" -> "SGP", "European Community" -> "", "Lebanon" -> "LBN", "Czech Republic" -> "CZE", "Sweden" -> "SWE")

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
      val timeDate = timeString.split(" ")
      val date = timeDate(0).split("/")
      val time = timeDate(1).split(":")
      val (day, month, year) = (date(1).toInt, date(0).toInt, date(2).toInt)
      val (hour, minute) = (time(0).toInt, time(1).toInt)
      Timestamp.valueOf(LocalDateTime.of(year, month, day, hour, minute))
    }

    def mapFuncCountry(r: Row): Row = Row(r(0), r(1), ISOcodes(r(1).toString))
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

      val products  = spark.read.parquet("hdfs://namenode:8020/user/hive/warehouse/products/").map(mapFuncProduct)(productEncoder)

      val countries = spark.read.parquet("hdfs://namenode:8020/user/hive/warehouse/countries/").map(mapFuncCountry)(countryEncoder)

      val invoices  = spark.read.parquet("hdfs://namenode:8020/user/hive/warehouse/invoices/").map(mapFuncInvoice)(invoiceEncoder)

      val invoicesProductsCountriesHive = invoices.as("invoices")
        .join(products.as("products"),
          col("invoices.InvoiceDate") === col("products.Price_On_Date") &&
            col("invoices.StockCode") === col("products.StockCode"), "left")
        .withColumn("total", col("invoices.Quantity") * col("products.UnitPrice"))
        .drop(col("products.StockCode"))
        .alias("inv_prod")
        .filter(col("CustomerId").isNotNull && col("total").isNotNull)
        .join(countries.as("countries"),
          col("inv_prod.Country") === col("countries.CountryCode"), "left")
        .drop("Price_On_Date", "Country")
        .orderBy("InvoiceDate")

  val succes = Try[Unit] {

    val invoicesProductsCountriesPostgres = spark.read.format("jdbc").options(db + ("dbtable" -> "invoices_products_countries")).load()

    val schemaMedianDev = StructType(
      Seq(
        StructField("StockCode", StringType, true),
        StructField("Median", DoubleType, true),
        StructField("StandardDev", DoubleType, true),
        StructField("Alarm", IntegerType, true)
      ))

    val decoderMedian = RowEncoder(schemaMedianDev)

    def mapFunctionMedian(r: Row): Row = {
      val arr = r(1).asInstanceOf[mutable.WrappedArray[Int]]
      val (m, d) = findMedianDeviation(arr)
      Row(r(0), m, d, (m + 2 * d).toInt + 1)
    }

    def findMedianDeviation(arr: mutable.WrappedArray[Int]): (Double, Double) = {
      val length = arr.length
      if (length > 0) {
        val avg = arr.sum.toDouble / length
        val med = if (length % 2 != 0) arr(length / 2).toDouble else (arr(length / 2 - 1).toDouble + arr(length / 2).toDouble) / 2
        val dev = math.sqrt(arr.foldLeft(0.0)((b, el) => b + (el - avg) * (el - avg)) / length)
        (med, dev)
      }
      else (0, 0)
    }

    val invoicesMedianAlarm = invoicesProductsCountriesPostgres
      .filter("Quantity > 0")
      .select("StockCode", "Quantity")
      .groupBy("StockCode")
      .agg(sort_array(collect_list("Quantity")))
      .map(mapFunctionMedian)(decoderMedian)

    val joined = invoicesProductsCountriesHive.as("invoices")
      .join(invoicesMedianAlarm.as("alarm"),
        col("invoices.StockCode") === col("alarm.StockCode"), "inner")
      .filter(col("invoices.Quantity") >= col("alarm.Alarm") && col("alarm.StandardDev" ) =!= 0.0)
      .select("InvoiceNo", "invoices.StockCode", "InvoiceDate", "CustomerID", "Description", 
              "UnitPrice", "CountryName", "Region","Median", "StandardDev", "Quantity", "Alarm")
      .collect()

      joined.foreach(r => messageText(r))



  }
  succes match{
    case Failure(exception) => exception.printStackTrace()
    case Success(_)         => println("SUCCESS")
  }

  invoicesProductsCountriesHive.write
    .format("jdbc")
    .options(db + ("dbtable" -> "invoices_products_countries"))
    .mode(SaveMode.Append)
    .save()
  }

