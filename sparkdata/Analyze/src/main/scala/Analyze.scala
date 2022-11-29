import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object Analyze  extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val db = Map("user"     -> "postgres",
               "password" -> "postgres",
               "driver"   -> "org.postgresql.Driver",
               "url"      -> "jdbc:postgresql://postgres:5432/postgres",
              )

  val spark =
       SparkSession
         .builder
         .appName("Analyze")
         .config("spark.master", "local")
         .getOrCreate;

  val twitterSchema = StructType(Seq(
      StructField("word", StringType, true)
    ))

  val sc = StructType(Seq(
      StructField("StockCode", StringType, true),
      StructField("words", ArrayType(StringType, true))
    ))

  val twEncoder = RowEncoder(twitterSchema)

  val success = Try[Unit] {

    val twitterTags = spark.read.format("csv")
                      .option("header", "false")
                      .schema(twitterSchema)
                      .load("file:///bool/entities.csv")
                      .map(r => Row(r(0).toString.toUpperCase))(twEncoder)
                      .groupBy("word")
                      .count                                        

    val invoicesProductsCountries = spark.read.format("jdbc").options(db + ("dbtable" -> "invoices_products_countries")).load

    def mapFunc(r:Row) = Row(r(1), r(6).toString.replaceAll("[-,'2\"]", " ").split(" ").map(_.trim))

    val dc = RowEncoder(sc)

    val words = invoicesProductsCountries.map(mapFunc)(dc)

    val twsc = StructType(
      Seq(
        StructField("StockCode", StringType, true),
        StructField("Description", StringType, true),
        StructField("hashtag", StringType, true),
        StructField("hashtags_count", IntegerType, true),
      ))

    val twitterEncoder = RowEncoder(twsc)

    def mapFunction(r:Row):Row = {
      val arr = r(3).asInstanceOf[mutable.WrappedArray[String]]
      val descrip = arr.mkString(" ")
      Row(r(2).toString, descrip, "#" + r(0).toString, r(1).toString.toInt)
    }

    val joinedTwitterProducts = twitterTags
      .filter(r => r(0).toString.length > 1)
      .join(words, array_contains(col("words"), col("word")), "inner")
      .distinct
      .orderBy(col("count").desc, col("word"))
      .map(mapFunction)(twitterEncoder)

    twitterTags.write
          .format("jdbc")
          .options(db + ("dbtable" -> "twitter_hashtag_count"))
          .mode(SaveMode.Overwrite)
          .save

    joinedTwitterProducts.write
      .format("jdbc")
      .options(db + ("dbtable" -> "twitter_hashtags_matching_products"))
      .mode(SaveMode.Overwrite)
      .save

    }

    success match {
      case Success(_)         => println("Job Succeeded")
      case Failure(exception) => exception.printStackTrace                                
    }
}
