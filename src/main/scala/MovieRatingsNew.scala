import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MovieRatingsNew {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieRatingsMulti")
    val sc = new SparkContext(conf)

    val moviesDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/movies.csv")
    val ratingsDatasetRDD = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/ratings.csv")

    //coppie (FILMID,GENERI(con pipe))
    val moviesWithGen:RDD[(String,String)] = moviesDatasetRDD.map(movie => (movie.substring(0,movie.indexOf(',')),movie.substring(movie.lastIndexOf(',')+1,movie.length)))
    //coppie MOVIEID,Genere (tenendo il map all'interno, mappo ogni pezzo dello split con il suo movieid)
    val mg = moviesWithGen.flatMap(x => x._2.split('|').map(y=> (x._1,y)))
    //coppie (FILMID,VOTO)
    val ratings:RDD[(String,String)] = ratingsDatasetRDD.map(rating => (rating.split(",")(1),rating.split(",")(2)))
    //RDD con coppie (FILMID, (listageneri, voto))
    val merged:RDD[(String,(String,String))] = mg.join(ratings)

    val genAvg = merged.map({case(_,(gen,rat)) => (gen, (rat.toDouble,1))})
                       .reduceByKey({case (r1,r2) => ((r1._1*r1._2 + r2._1*r2._2)/(r1._2 + r2._2),(r1._2 + r2._2))})
    var result = genAvg.foreach(println)

  }

}