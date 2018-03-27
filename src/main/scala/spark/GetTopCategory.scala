package spark

import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import com.snowplowanalytics.maxmind.iplookups.IpLookups
import org.apache.spark.SparkFiles
import java.io.File
import java.net.InetAddress

//Question: Find the most frequently
//visited apps category in US

//○ From access log: grab all US google apps store visits
//○ From crawler metadata: get category for each app
//○ Join them together by app id
//○ Aggregation with category

//spark-submit --class spark.GetTopCategory --files ../resource/GeoLiteCity.dat Scala4fun-assembly-0.1.jar

object GetTopCategory {

  case class accessLog(ip:String, country:String, address:String)

  def parseAccessLog(spark:SparkSession): Dataset[accessLog] ={

    //val location = new LookupService(SparkFiles.get("GeoLiteCity.dat"), LookupService.GEOIP_MEMORY_CACHE)

    //val GeoIPLookup = new DatabaseReader.Builder(new File(SparkFiles.get("GeoLite2-City.mmdb"))).build()



    import spark.implicits._
    //Let's define udf function object
    val udf = (line: String, ipLookups: IpLookups) => {
          import java.util.regex.Pattern
          val linePattern1 = Pattern.compile("(.*?) .*?\\[(.*?)\\].*?&url=(.*?)(?:&|%26).*")
          val linePattern2 = Pattern.compile(".*?id(?:=|%3D)(.*)?")
          val m1 = linePattern1.matcher(line)
          var ip = "None"
          var address = "None"
          var dt = "None"
          var url = "None"
          if (m1.find) {
            ip = m1.group(1)
            dt = m1.group(2)
            url = m1.group(3)
            val m2 = linePattern2.matcher(url)
        if (m2.find) address = m2.group(1)
      }
      val lookupResult = ipLookups.performLookups(ip)._1
      val countryCode = lookupResult.map(_.countryCode).getOrElse("None")
      accessLog(ip, countryCode, address)
    }

    val accessLogRDD = spark.read.textFile("access_log_sample")
     .filter(line => line.matches(".*&url=(https:|http:|https%3A|http%3A)//play.google.com/store/apps/details.*"))
        //Here we should use mapPartitions instead of map
      .mapPartitions { lines =>
          val ipLookups = IpLookups(geoFile = Some(SparkFiles.get("GeoLiteCity.dat")))
          lines.map(line => udf(line, ipLookups))
        }
    accessLogRDD.distinct().where("country = 'US'")
  }

  case class appmetadata(address:String, category:String)
  def parseAppMetaData(spark:SparkSession): Dataset[appmetadata] ={
    import spark.implicits._
    val inputRDD = spark.read.textFile("appmetadata.txt.gz")
    val appMetaRDD = inputRDD.map(line=> {
        val items = line.split("\t")
        val url = if (items.length > 0) items(0)
        else null
        val category = if (items.length > 5) items(5)
        else null
      import java.util.regex.Pattern
      val linePattern2 = Pattern.compile(".*?id=(.*)?")
      val m = linePattern2.matcher(url)
      var address = "None"
      if (m.find) address = m.group(1)
      appmetadata(address, category)
    })
    appMetaRDD
  }



  def main(args: Array[String]){
    val spark = SparkSession
      .builder
      .appName("get_top_category")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val AccessLogDS = parseAccessLog(spark)
    val AppMetaDataDS = parseAppMetaData(spark)
    AccessLogDS.join(AppMetaDataDS, "address")
      .groupBy("category")
      .agg(functions.count("*").alias("category_count"))
      .orderBy($"category_count".desc)
      .coalesce(1)
      .write.csv("output");
    spark.stop()
  }
}
