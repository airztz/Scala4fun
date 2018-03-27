package spark
import org.apache.spark.sql.{Row, SQLContext, SparkSession, expressions, functions}
import org.apache.spark.sql.types._
//spark-submit --class spark.spark_hive --master yarn --deploy-mode cluster --properties-file spark-defaults.conf target/scala-2.11/Scala4fun-assembly-0.1.jar
object spark_hive {
  case class Student(name: String, age: Int, gpa: Double)
  case class Voter(name: String, age: Int, registration: String, contributions:
  Double)
  def hive_practice(spark: SparkSession): Unit = {
    import spark.implicits._
    import spark.sql
   // sql("drop table if exists tmp3")
    sql("show tables").show()
    sql("""select v.registration, AVG(ROUND(s.gpa)) avg from student_part s
       join voter v on s.name = v.name
       where s.age > 18
       group by v.registration
       order by avg desc""").show()
    //sql("create table tmp3 (name1 string, age int, name string)")
  }
  def hdfs_practice(spark: SparkSession): Unit = {
    import spark.implicits._
    val a = spark.sparkContext.textFile("hdfs://Joy4funCluster/user/ec2-user/studenttab10k")
      .map(line => line.split("\t"))
      .map { case Array(s0, s1, s2) => Student(s0, s1.toInt, s2.toDouble) }
      .toDS()
    val a1 = a.filter("age > 18")
    val a2 = a1.withColumn("gpa", functions.round($"gpa"))
    val b = spark.sparkContext.textFile("hdfs://Joy4funCluster/user/ec2-user/votertab10k")
      .map(line => line.split("\t"))
      .map { case Array(s0, s1, s2, s3) => Voter(s0, s1.toInt, s2, s3.toDouble) }
      .toDS()
    val c = a2.join(b, a2.col("name").equalTo(b("name")))
    val d = c.groupBy(b.col("registration"))
    val e = d.agg(functions.avg(a2.col("gpa")).alias("avg"))
    val f = e.orderBy($"avg".desc)
    f.collect.foreach(println)
  }
  def main(args: Array[String]) {
    //in remote client's /etc/hosts   set xxx.xxx.xxx(namenode's public ip) Joy4funCluster
    //spark will use Joy4funCluster as nameservice
    val spark = SparkSession
      .builder
      .appName("spark_hive")
      //.config("spark.master", "local")
      .config("spark.sql.warehouse.dir","hdfs://Joy4funCluster/user/hive/warehouse")
      .config("hive.metastore.uris","thrift://hdfsHA2:9083,thrift://hdfsHA5:9083")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")
    //hdfs_practice(spark)
    hive_practice(spark)
    spark.stop()
  }
}
