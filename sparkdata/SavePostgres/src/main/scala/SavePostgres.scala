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

    val ISOcodes = Map(
                      "Norway" -> "NO", "Netherlands" -> "NL", "France" -> "FR", "Germany" -> "DE", "Australia" -> "AU", "United Kingdom" -> "GB",
                      "Ireland" -> "IE", "Italy" -> "IT", "Switzerland" -> "CH", "Portugal" -> "PT", "Spain" -> "ES", "Belgium" -> "BE", "Poland" -> "PL",
                      "Japan" -> "JP", "Lithuania" -> "LT", "Iceland" -> "IS", "Malta" -> "MT", "Denmark" -> "DK", "Unspecified" -> "", "RSA" -> "ZA",
                      "United Arab Emirates" -> "AE", "Canada" -> "CA", "Cyprus" -> "CY", "Channel Islands" -> "GB", "Brazil" -> "BR", "Israel" -> "IL",
                      "Austria" -> "AT", "Finland" -> "FI", "USA" -> "US", "Bahrain" -> "BH", "Greece" -> "GR", "Hong Kong" -> "HK", "Saudi Arabia" -> "SA",
                      "Singapore" -> "SG", "European Community" -> "", "Lebanon" -> "LB", "Czech Republic" -> "CZ", "Sweden" -> "SE"
                      )

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

    val db = Map(
                "user"      -> "postgres",
                "password"  -> "postgres",
                "driver"    -> "org.postgresql.Driver",
                "url"       -> "jdbc:postgresql://postgres:5432/postgres",
              )

    def saveDb(df:DataFrame) = {
        df.write
        .format("jdbc")
        .options(db + ("dbtable" -> "invoices_products_countries"))
        .mode(SaveMode.Append)
        .save()
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
                                                  .filter(col("CustomerId").isNotNull && col("total").isNotNull && col("total") =!= 0.0)
                                                  .join(countries.as("countries"),
                                                    col("inv_prod.Country") === col("countries.CountryCode"), "left")
                                                  .drop("Price_On_Date", "Country")
                                                  .orderBy("InvoiceDate")
                                                  .persist() 

    val success = Try[Unit] {  

      val invoicesProductsCountriesPostgres = spark.read
                                                .format("jdbc")
                                                .options(db + ("dbtable" -> "invoices_products_countries"))
                                                .load()
      
      val schemaMedianDev = StructType(
          Seq(
            StructField("StockCode", StringType, true),
            StructField("Average", DoubleType, true),
            StructField("Median", DoubleType, true),
            StructField("StandardDev", DoubleType, true),
            StructField("Alarm", DoubleType, true)
          ))

      val decoderMedian = RowEncoder(schemaMedianDev)

      def mapFunctionMedian(r: Row): Row = {
          val arr = r(1).asInstanceOf[mutable.WrappedArray[Int]]
          val (avg, med, dev) = findMedianDeviation(arr)
          val alarm = med + 2 * dev
          Row(r(0), avg, med, dev, alarm)
      }

      def findMedianDeviation(arr: mutable.WrappedArray[Int]): (Double, Double, Double) = {      
            val length = arr.length
            if (length > 1) {
              val avg = arr.sum.toDouble / length
              val med = if (length % 2 != 0) arr(length / 2).toDouble else (arr(length / 2 - 1).toDouble + arr(length / 2).toDouble) / 2
              val dev = math.sqrt(
                (arr.foldLeft(0.0)((b, el) => b + ((el - avg) * (el - avg)))) / (length - 1)
                )             
              (avg, med, dev)
            }
            else if (length == 1) (arr(0), arr(0), 0)
            else (0, 0, 0)
      }

      val invoicesMedianDeviationAlarm = invoicesProductsCountriesPostgres
                                          .filter("Quantity > 0")
                                          .select("StockCode", "Quantity")
                                          .groupBy("StockCode")
                                          .agg(sort_array(collect_list("Quantity")))
                                          .map(mapFunctionMedian)(decoderMedian)

      val invoicesForAlarm = invoicesProductsCountriesHive.as("invoices")
                              .join(invoicesMedianDeviationAlarm.as("alarm"),
                                col("invoices.StockCode") === col("alarm.StockCode"), "inner")
                              .filter(col("invoices.Quantity") > col("alarm.Alarm") && col("alarm.StandardDev" ) =!= 0.0)
                              .select("InvoiceNo", "invoices.StockCode", "InvoiceDate", "CustomerID", "Description", 
                                      "UnitPrice", "CountryName", "Region", "Average", "Median", "StandardDev", "Quantity", "Alarm")
                              .collect()

      val text = new mutable.StringBuilder("______________________________________________\n")
      
      invoicesForAlarm.foreach(r => {
                    val rw = s"Customer ID: ${r(3).toString}\nInvoice No: ${r(0).toString}\nStock Code: ${r(1).toString}\nInvoice DateTime: ${r(2).toString}\n" +
                            s"Product Description: ${r(4).toString}\nUnit Price: ${r(5).toString}\nCountry: ${r(6).toString}\nRegion: ${r(7).toString}\n" +
                            s"Average: ${r(8).toString}\nMedian: ${r(9).toString}\nStandard Deviation: ${r(10).toString}\nQuantity Ordered: ${r(11).toString}\nAlarm: ${r(12).toString}\n"
                    text.append(rw + "______________________________________________\n")
                  })

      if (!text.toString.equals("______________________________________________\n")){          
              val props = new Properties
              props.put("mail.smtp.auth", "true")
              props.put("mail.smtp.ssl.enable", "true")
              props.put("mail.smtp.host", "smtp.gmail.com")
              props.put("mail.smtp.port", "465")  
              
              val session = Session.getInstance(props, new jakarta.mail.Authenticator() {
                              override protected def getPasswordAuthentication = new PasswordAuthentication("sinagajanovicanother", "kggjlmfgnvwdhrlr")
                            })

              val message = new MimeMessage(session)    
              message.setFrom(new InternetAddress("sinagajanovicanother@gmail.com"))         
              message.setRecipients(Message.RecipientType.TO, "sinagajanovicanother@gmail.com,sinagajanovic@gmail.com")
              message.setSubject("Orders greater than two standard deviations above the median")
              message.setText(text.toString)
              Transport.send(message)            
      } 

      saveDb(invoicesProductsCountriesHive)
    }
    
    success match {
            case Failure(exception) => exception.printStackTrace() 
                                       saveDb(invoicesProductsCountriesHive) 

            case Success(_)         => println("SUCCESS!!!!!!!!!!!")    
  } 
    
}
  