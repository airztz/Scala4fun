package spark
//https://hortonworks.com/tutorial/setting-up-a-spark-development-environment-with-scala/
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

//spark-submit --class spark.word_count target/scala-2.11/Scala4fun-assembly-0.1.jar

object word_count {
  def main(args: Array[String]){
    val words = Array("one", "two", "two", "three", "three", "three")

    val sparkConf = new SparkConf().setAppName("spark_scala_practice").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1)).cache()

    val wordCountsByKey = wordPairsRDD.countByKey()
    wordCountsByKey.foreach(println)

    val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _).collect()
    wordCountsWithReduce.foreach(println)

    //t: (String, Iterable[Int])
    val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum)).collect()
    wordCountsWithGroup.foreach(println)

    wordPairsRDD.unpersist()

    sc.stop()
  }
}