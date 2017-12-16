import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MovieRatings {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
    val sc = new SparkContext(conf)

    val moviesDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/moviesLight.csv")
//    val movies: RDD[(String, String)] = moviesDatasetRDD.map(x => (x.split(",")(0), x.split(",")(x.split(",").length - 1)))
//    movies.foreach(println)

    val genereWithMovies = moviesDatasetRDD
                          .map(movie => (movie.substring(movie.lastIndexOf(',')+1,movie.length).split('|')(0),Array(movie.substring(0,movie.indexOf(',')))))
                          .reduceByKey(_++_)
                          .foreach(println)

   /* var moviesDB = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/moviesLight.csv")
                     //.map(movie => (Array(movie.substring(movie.lastIndexOf(','),movie.length-1).split('|').distinct).toVector,Array(movie.split(",")(0).toVector)))
                     .map(movie => (movie.substring(movie.lastIndexOf(','),movie.length-1).split('|'),Array(movie.substring(0,movie.indexOf(',')))))
                     .reduceByKey(_++_)
                     .collect()
                     .foreach(println)*/

//    var ratingsDB = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/ratings.csv")
//                      .flatMap(movie => movie.split(",").takeRight(1)(0).split('|'))
//                      .map((_,1))
//                      .reduceByKey(_+_)
//                      .collect.toMap
//                      .foreach(println)

  }

}
