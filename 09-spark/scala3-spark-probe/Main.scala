import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*

@main def run(): Unit =
  val spark = SparkSession.builder().master("local[2]").appName("s3").config("spark.ui.enabled", "false").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.range(0, 1000).select((col("id") % 3).as("k")).groupBy("k").count().orderBy("k")
  println(s"Scala ${scala.util.Properties.versionNumberString} (runtime thư viện), Spark ${spark.version}")
  println(df.collect().map(r => s"${r.getLong(0)}=${r.getLong(1)}").mkString(", "))
  spark.stop()
