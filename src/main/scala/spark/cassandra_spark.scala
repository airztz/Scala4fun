package spark
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
object cassandra_spark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application")
      //.config("spark.master", "local")
      .config("spark.cassandra.connection.host", "sink1")
      .getOrCreate()
    import spark.implicits._
    val ratingsDF = spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", true)
      .options(Map("keyspace"->"bittiger" , "table"->"movielens_movie"))
      .load()

    val ratingsDF_cast_type = ratingsDF.withColumn("users", ratingsDF("users").cast(types.IntegerType))

    val userRecsK = 3 // top k recommended movies for a user
    val movieRecsK = 3 // top k recommended users for a movie

    val Array(training, test) = ratingsDF_cast_type.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("users").setItemCol("movie_id").setRatingCol("rating")
    val model = als.fit(training.filter(training("users").isNotNull))

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(userRecsK)
    userRecs.show(10, false)

    //    sent to cassandra

    val arrayToMap = udf[Map[Integer, Float], Seq[Row]] {
      array => array.map { case Row(key: Integer, value: Float) => (key, value) }.toMap
    }

    val result = userRecs.withColumnRenamed("users", "user_id").withColumn("recommendations", arrayToMap($"recommendations"))
    result.printSchema()
    result.show(10, false)
    //spark.stop();
    result.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"bittiger" , "table"->"recommendation"))
      .mode(SaveMode.Append)
      .save()

    // Generate top 10 user recommendations for each movie
    //val movieRecs = model.recommendForAllItems(movieRecsK)
    //movieRecs.show(20, false)

    //val RMSE = evaluator.evaluate(predictions) // Root Mean Square Error
    spark.stop();
  }
}
