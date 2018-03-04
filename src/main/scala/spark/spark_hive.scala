package spark
import org.apache.spark.sql.{Row, SQLContext, SparkSession, expressions, functions}
import org.apache.spark.sql.types._
object spark_hive {
  def hive_practice(spark: SparkSession): Unit = {
    import spark.implicits._
  }
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("spark_hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    hive_practice(spark)
    spark.stop()
  }
}
