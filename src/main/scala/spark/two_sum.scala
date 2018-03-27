package spark

import org.apache.spark.{SparkConf, SparkContext}

object two_sum {

  def main(args: Array[String]){
    val nums = Array(-9, -7, -5, -3, -1, 1, 3, 5, 7, 9)
    val sparkConf = new SparkConf().setAppName("spark_scala_practice")
    .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val sum = 4
    val rdd_nums = sc.parallelize(nums).map(num=>(num,1))
    val rdd_nums_2 = rdd_nums.map{case(key: Int, count:Int)=>(sum-key,count)}
    print (rdd_nums.join(rdd_nums_2).count()>0)
    sc.stop()
  }

}
