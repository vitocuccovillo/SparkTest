import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MovieAverage {

  def main(args : Array[String]) : Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieAverage")
    val sc = new SparkContext(conf)

    val moviesDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/movies.csv")
    val ratingsDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/ratings.csv")

    val movieRDD:RDD[(String,String)] = moviesDatasetRDD.map(row => (row.split(",")(0), row.split(",")(1)))
    //movieRDD.foreach(println)

    val ratingsRDD:RDD[(String,Double)] = ratingsDatasetRDD.map(row => (row.split(",")(1), row.split(",")(2).toDouble))
    //ratingsRDD.foreach(println)

    val ds = movieRDD.join(ratingsRDD).map{case (_,(m,r)) => (m,(r,1))}
                                      .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
                                      .map(a => (a._1,a._2._1 / a._2._2))
    val sorted = ds.sortBy(_._2,true)
    sorted.foreach(println)

  }

}
