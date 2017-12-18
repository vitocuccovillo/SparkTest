import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MovieRatingsOptimized {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
    val sc = new SparkContext(conf)

    val moviesDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/movies.csv")
    val ratingsDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/ratings.csv")

    val moviesWithGen:RDD[(String,String)] = moviesDatasetRDD.map(movie => (movie.substring(0,movie.indexOf(',')),movie.substring(movie.lastIndexOf(',')+1,movie.length)))
    val ratings:RDD[(String,String)] = ratingsDatasetRDD.map(rating => (rating.split(",")(1),rating.split(",")(2)))
    val merged:RDD[(String,(String,String))] = moviesWithGen.join(ratings)
    val rd:RDD[(String,Double)] = merged.flatMap(x => x._2._1.split('|')
                                        .map(y => (y,(x._2._2.toDouble,1))))
                                        .reduceByKey((r1,r2) => ((r1._1*r1._2 + r2._1*r2._2)/(r1._2 + r2._2),(r1._2 + r2._2)))
                                        .map{case(a,(b,_)) => (a,b)}
    rd.foreach(println)

  }

}
