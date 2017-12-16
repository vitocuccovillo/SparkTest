import org.apache.spark.{SparkConf, SparkContext}

object MovieRatings {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
    val sc = new SparkContext(conf)

    var moviesDB = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/moviesLight.csv")
                     .map(movie => (Array(movie.split(",").takeRight(1)(0).split('|')).toVector,Array[String](movie.split(",")(0))))
                     .reduceByKey(_++_)
                     .collect()
                     .foreach(println)

//    var ratingsDB = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/ratings.csv")
//                      .flatMap(movie => movie.split(",").takeRight(1)(0).split('|'))
//                      .map((_,1))
//                      .reduceByKey(_+_)
//                      .collect.toMap
//                      .foreach(println)

  }

}
