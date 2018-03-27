package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object spark_join {

  def rdd_map_side_join(sc: SparkContext): Unit = {
    // Fact table
    val flights = sc.parallelize(List(
      ("SEA", "JFK", "DL", "418", "7:00"),
      ("SFO", "LAX", "AA", "1250", "7:05"),
      ("SFO", "JFK", "VX", "12", "7:05"),
      ("JFK", "LAX", "DL", "424", "7:10"),
      ("LAX", "SEA", "DL", "5737", "7:10")))

    // Dimension table
    val airports = sc.parallelize(List(
      ("JFK", "John F. Kennedy International Airport", "New York", "NY"),
      ("LAX", "Los Angeles International Airport", "Los Angeles", "CA"),
      ("SEA", "Seattle-Tacoma International Airport", "Seattle", "WA"),
      ("SFO", "San Francisco International Airport", "San Francisco", "CA")))

    // Dimension table
    val airlines = sc.parallelize(List(
      ("AA", "American Airlines"),
      ("DL", "Delta Airlines"),
      ("VX", "Virgin America")))

    //    We need to join the fact and dimension tables to get the following result:

    //      Seattle           New York       Delta Airlines       418   7:00
    //    San Francisco     Los Angeles    American Airlines    1250  7:05
    //    San Francisco     New York       Virgin America       12    7:05
    //    New York          Los Angeles    Delta Airlines       424   7:10
    //    Los Angeles       Seattle        Delta Airlines       5737  7:10
    val airportsMap = sc.broadcast(airports.map { case (a, b, c, d) => (a, c) }.collectAsMap)
    val airlinesMap = sc.broadcast(airlines.collectAsMap)

    val output = flights.map {
      case (a, b, c, d, e) =>
        (airportsMap.value.get(a).get,
          airportsMap.value.get(b).get,
          airlinesMap.value.get(c).get, d, e)
    }
    output.collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("spark_scala_practice")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    rdd_map_side_join(sc)
    sc.stop()
  }
}
