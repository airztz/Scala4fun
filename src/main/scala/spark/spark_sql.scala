package spark

import org.apache.spark.sql.{Row, SQLContext, SparkSession, expressions, functions}
import org.apache.spark.sql.types._
import java.sql.Date

//spark-submit --class spark.spark_sql --master yarn --deploy-mode cluster target/scala-2.11/Scala4fun-assembly-0.1.jar

object spark_sql {

  def top_market_for_region(spark: SparkSession): Unit = {
    import spark.implicits._
//    val dataframe = spark.read.option("header", true).csv("alu4g_cell_combined_20170701.csv")
//      .select("REGION", "MARKET")
    //read from hdfs
    spark.sparkContext.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")
    //spark.sparkContext.hadoopConfiguration.set("dfs.datanode.use.datanode.hostname", "true")

    val dataframe = spark.read.option("header", true).csv("hdfs://Joy4funCluster/user/ec2-user/alu4g_cell_combined_20170701.csv")
      .select("REGION", "MARKET")
    dataframe.printSchema()
    dataframe.show()
    //    +---------+--------+
    //    |   REGION|  MARKET|
    //    +---------+--------+
    //    |Northeast|NYC East|
    //    |Northeast|NYC East|
    //    |Northeast|NYC East|
    //    |Northeast|NYC East|
    //    |Northeast|NYC East|
    //    +---------+--------+
    //    only showing top 5 rows
    val group_ddf = dataframe.groupBy("REGION")
    val agg_ddf = group_ddf.agg(functions.countDistinct("MARKET").alias("MARKET_COUNT"))
    agg_ddf.show(5)
    //    +---------+------------+
    //    |   REGION|MARKET_COUNT|
    //    +---------+------------+
    //    |     null|           0|
    //    |  Unknown|           1|
    //    |Southeast|           1|
    //    |  Central|          11|
    //    |     West|          11|
    //    +---------+------------+
    //    only showing top 5 rows
    val agg_ddf_ordered = agg_ddf.orderBy(functions.desc("MARKET_COUNT"))
    agg_ddf_ordered.show(5)
    //    +---------+------------+
    //    |   REGION|MARKET_COUNT|
    //    +---------+------------+
    //    |Northeast|          15|
    //    |  Central|          11|
    //    |     West|          11|
    //    |Southeast|           1|
    //    |  Unknown|           1|
    //    +---------+------------+
    //    only showing top 5 rows
    dataframe.groupBy("REGION", "MARKET").count().show()
    //      +---------+----------------+-----+
    //      |   REGION|          MARKET|count|
    //      +---------+----------------+-----+
    //      |Northeast|    Philadelphia| 9156|
    //      |  Central|    North Dakota| 1199|
    //      |     West|      Sacramento|    6|
    //      |     West|      New Mexico| 2076|
    //      |Northeast|        NYC East|12282|
    //      |Northeast|           Harly| 6155|
    //      |     West|      Washington|10017|
    //      |Northeast|   NYC Manhattan| 5119|
    //      |Northeast|     Northern OH| 6635|
    //      |Northeast|   Washington DC| 6744|
    //      |  Central|    South Dakota| 1407|
    //      |Northeast|      Upstate NY|    3|
    //      |  Central|       Minnesota| 7182|
    //      |     West|        Colorado| 5756|
    //      |     null|            null| 2076|
    //      |  Central|       Iowa-PART|   51|
    //      |     West|           Idaho| 1441|
    //      |  Central|Central Illinois|   24|
    //      |Northeast|        Maryland| 6837|
    //      |     West|         Arizona| 7773|
    //      +---------+----------------+-----+
    //      only showing top 20 rows

    val grouped = dataframe.groupBy("REGION", "MARKET").count().na.drop()
    val window = expressions.Window.partitionBy("REGION").orderBy(functions.col("count").desc)
    grouped.select($"REGION", $"MARKET", $"count", functions.rank().over(window).alias("rank")).filter(functions.col("rank") <= 2).show()
    //      +---------+--------------+-----+----+
    //      |   REGION|        MARKET|count|rank|
    //      +---------+--------------+-----+----+
    //      |  Unknown|       Unknown|   57|   1|
    //      |Southeast| Piedmont-PART|    3|   1|
    //      |  Central|Kansas City KS| 8928|   1|
    //      |  Central|  St. Louis MO| 8503|   2|
    //      |     West|    Washington|10017|   1|
    //      |     West|       Arizona| 7773|   2|
    //      |Northeast|      NYC East|12282|   1|
    //      |Northeast|      NYC West|11675|   2|
    //      +---------+--------------+-----+----+
  }

  def page_view_dataframe_practice(spark: SparkSession) {
    import spark.implicits._
    val schema = StructType(
      StructField("date", DateType) ::
        StructField("page_id", IntegerType) ::
        StructField("user_ids", ArrayType(StringType)) :: Nil)

    //    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    //    val date = new java.sql.Date(format.parse("2017-09-15").getTime())
    //    val date2 = new java.sql.Date(format.parse("2017-09-14").getTime())
    val rows = Array(Row(Date.valueOf("2017-09-15"), 1, Array("a", "b", "c")),
      Row(Date.valueOf("2017-09-15"), 2, Array("a", "d", "e")),
      Row(Date.valueOf("2017-09-14"), 2, Array("a", "b", "c"))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.printSchema()
    df.show()
    val flatDF = df.withColumn("user_ids", functions.explode($"user_ids"))

    println("Flatten dataframe:")
    flatDF.show()
    //  # +--------+-------+--------+
    //  # | date | page_id | user_ids |
    //  # +--------+-------+--------+
    //  # | 12152017 | 1 | a /**/|
    //  # | 12152017 | 1 | b |
    //  # | 12152017 | 1 | c |
    //  # | 12152017 | 2 | a |
    //  # | 12152017 | 2 | d |
    //  # | 12152017 | 2 | e |
    //  # | 12152017 | 2 | a |
    //  # | 12152017 | 2 | b |
    //  # | 12152017 | 2 | c |
    //  # +--------+-------+--------+
    //
    flatDF.where("date > to_date(\"2017-09-13\")").groupBy($"page_id").count().show()
    ////  # +---------+-------+
    ////  # | page_id | count |
    ////  # +---------+-------+
    ////  # |       1 |     3 |
    ////  # |       2 |     6 |
    ////  # +---------+-------+
    //
    println("Top viewd page_id:")
    flatDF.where("date > to_date(\"2017-09-13\")")
      .groupBy($"page_id")
      .agg(functions.countDistinct($"user_ids").alias("USERS_COUNT"))
      .orderBy($"USERS_COUNT".desc).show()
    //
    ////  # +-------+-----------+
    ////  # | page_id | USERS_COUNT |
    ////  # +-------+-----------+
    ////  # | 2 | 5 |
    ////  # | 1 | 3 |
    ////  # +-------+-----------+
    //
    println("users with Top page_views count:")
    flatDF.where("date > to_date(\"2017-09-13\")")
      .groupBy($"user_ids")
      .agg(functions.countDistinct($"page_id").alias("PAGE_COUNT"))
      .orderBy($"PAGE_COUNT".desc).show()
    //  # +--------+----------+
    //  # | user_ids | PAGE_COUNT |
    //  # +--------+----------+
    //  # | c | 2 |
    //  # | a | 2 |
    //  # | b | 2 |
    //  # | d | 1 |
    //  # | e | 1 |
    //  # +--------+----------+
  }

  case class PageViews(date: Date, page_id: Int, user_ids: Array[String])

  case class PageView(date: Date, page_id: Int, user_id: String)

  def page_view_dataset_practice(spark: SparkSession): Unit = {
    import spark.implicits._

    //    val data = Seq((0, "Lorem ipsum dolor", 1.0, Array("prp1", "prp2", "prp3")))
    //    val ds = spark.createDataset(data).as[(Integer, String, Double, List[String])]
    //    ds.printSchema()
    //    ds.show()
    //    val flat_ds = ds.flatMap{ record =>
    //      record._4.map { record_4 =>
    //        (record._1, record._2, record._3, record_4)
    //      }
    //    }
    //    flat_ds.printSchema()
    //    flat_ds.toDF("id", "text", "value", "properties").show()

    val data = Seq(
      PageViews(Date.valueOf("2017-09-15"), 1, Array("a", "b", "c")),
      PageViews(Date.valueOf("2017-09-15"), 2, Array("a", "d", "e")),
      PageViews(Date.valueOf("2017-09-14"), 2, Array("a", "b", "c"))
    )
    //implicit val encodeDate = org.apache.spark.sql.Encoders.DATE
    val ds = spark.createDataset(data)
    ds.printSchema()
    ds.show()
    //V1:
//        val flatDS = ds.flatMap(
//          pv => pv.user_ids.map(
//            user_id => PageView(pv.date, pv.page_id, user_id)
//          )
//        )
    //V2:
    val flatDS = for {
      pv <- ds
      user_id <- pv.user_ids
    } yield PageView(pv.date, pv.page_id, user_id)

    flatDS.printSchema()
    flatDS.show()

    println("Top viewd page_id:")
    flatDS.where("date > to_date(\"2017-09-13\")")
      .groupBy($"page_id")
      .agg(functions.countDistinct($"user_id").alias("USERS_COUNT"))
      .orderBy($"USERS_COUNT".desc).show()

    println("Users with Top page_views count:")
    flatDS.where("date > to_date(\"2017-09-13\")")
      .groupBy($"user_id")
      .agg(functions.countDistinct($"page_id").alias("PAGE_COUNT"))
      .orderBy($"PAGE_COUNT".desc).show()
  }

  case class record(id: Int, value: String)

  def regular_join(spark: SparkSession): Unit ={

    import spark.implicits._
    val r1 = spark.createDataset(Seq(record(1, "A1"), record(2, "A2"), record(3, "A3"), record(4, "A4")))
    val r2 = spark.createDataset(Seq(record(3, "A3"), record(4, "A4"), record(4, "A4_1"), record(5, "A5"), record(6, "A6")))

    val joinTypes = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")

    joinTypes foreach {joinType =>
      println(s"${joinType.toUpperCase()} JOIN")
      r1.join(right = r2, usingColumns = Seq("id","value"), joinType = joinType).orderBy("id").show()
      //r1.join(r2, r1.col("id")===r2.col("id"), joinType).orderBy("id").show() doesn't work
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("spark_sql")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //top_market_for_region(spark)
    //page_view_dataframe_practice(spark)
    //page_view_dataset_practice(spark)
    regular_join(spark)
    spark.stop()
  }

}
