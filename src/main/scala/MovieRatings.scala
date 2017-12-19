import org.apache.spark.{SparkConf, SparkContext}

object MovieRatings {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
    val sc = new SparkContext(conf)

    val moviesDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/movies.csv")
    val ratingsDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/ratings.csv")

    val moviesWithGen = moviesDatasetRDD.map(movie => (movie.substring(0,movie.indexOf(',')),movie.substring(movie.lastIndexOf(',')+1,movie.length)))
    val ratings = ratingsDatasetRDD.map(rating => (rating.split(",")(1),rating.split(",")(2)))
    val merged = moviesWithGen.join(ratings)
    //val bygen = merged.flatMap{ case(f,(r,g)) => (g,r)}
    // QUESTO MANIPOLA LE COPPIE! da coppia di coppie a coppia singola
    //val rd = merged.map{ case (film, (gen,rat)) => (gen,rat) }.reduceByKey(_+_)

    val rd = merged.flatMap(x => x._2._1.split('|').map(y => (y,(x._2._2.toDouble,1)))).reduceByKey{case ((r1,c1),(r2,c2)) => (r1 + r2,c1 + c2)}
    val result = rd.map{ case (gen,(sum,count)) => (gen, sum / count)}
    result.foreach(println)

  }

}
