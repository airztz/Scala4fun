package spark

import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.{Row, SQLContext, SparkSession, expressions, functions}
import org.apache.spark.sql.types._

object multiplication_matrix {

  def dot_product(vector1: List[Double],vector2: List[Double]): Double ={
    var result: Double = 0
    for(index <- 0 until vector1.length){
      result+=vector1(index)*vector2(index)
    }
    result
  }

  def rdd_matrix_production(spark: SparkSession): Unit ={
    import spark.implicits._
    val M1 = spark.sparkContext.parallelize(Seq(
      (0L, List(1.0, 0.0, 0.0)),
      (1L, List(0.0, 1.0, 0.0)),
      (2L, List(0.0, 0.0, 1.0)))
    )
    //          .foreach(record=>{
    //          //print(record._1+",")
    //          record._2.foreach(element=>print(element+" "))
    //          //println()
    //        })

    //transpose M2
    val M2 = spark.sparkContext.parallelize(Seq(
      (0L, List(1.0, 4.0, 7.0, 10.0)),
      (1L, List(2.0, 5.0, 8.0, 11.0)),
      (2L, List(3.0, 6.0, 9.0, 12.0)))
    )
      .flatMap {
        case (key: Long, vector: List[Double]) => {
          var index = 0L
          //println("\nKey:" + key)
          vector.map {
            element => {
              //print("Index: " + index + " Elements:" + element + " ")
              val new_element = (index, element)
              index += 1
              new_element
            }
          }
        }
      }.groupByKey().mapValues(_.toList)
    //    println("\n"+M1.collect().mkString("|"))
    //    println("\n"+M2.collect().mkString("|"))
    val production_RDD = M1.cartesian(M2)
      .map{case((row: Long, vector1: List[Double]),(col: Long, vector2: List[Double]))=>((row,col),dot_product(vector1,vector2))}
    //
    production_RDD.collect().foreach(println)
  }

  def main(args: Array[String]) {

    // Create a dense vector (1.0, 0.0, 3.0).
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    //println(sv1)
    //(3,[0,2],[1.0,3.0])

    // Create a dense matrix (
    // (1.0, 2.0, 7.0),
    // (3.0, 4.0, 8.0),
    // (5.0, 6.0, 9.0)
    val dm: Matrix = Matrices.dense(3, 3, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0, 7.0, 8.0, 9.0))
    val denseM: DenseMatrix = new DenseMatrix(3, 3, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0, 7.0, 8.0, 9.0))
    //println(denseM)
    // Create a sparse matrix (
    // (9.0, 0.0),
    // (0.0, 8.0),
    // (0.0, 6.0))
    // colPtrs: 0, 1, 3  <-----  \9,\ 8, 6\
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 1, 2), Array(9, 8, 6))
    //https://stackoverflow.com/questions/44825193/how-to-create-a-sparse-cscmatrix-using-spark
    //println(sm)
    //    3 x 2 CSCMatrix
    //    (0,0) 9.0
    //    (1,1) 8.0
    //    (2,1) 6.0
    val product_dens_dens = denseM.multiply(denseM)
    val product_dm_dv = dm.multiply(dv)


    val spark = SparkSession
      .builder
      .appName("spark_sql")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    rdd_matrix_production(spark)
    spark.stop()
  }
}
