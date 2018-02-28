package spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

//https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#operations-on-streaming-dataframesdatasets
//https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html


//tongzhous-mbp:Scala4fun Joy4fun$ nc -lk 9999
//apache spark
//apache hadoop
//apache cassandra flink

//spark-submit --class spark.structured_streaming target/scala-2.11/Scala4fun-assembly-0.1.jar

object structured_streaming {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    lines.printSchema

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
