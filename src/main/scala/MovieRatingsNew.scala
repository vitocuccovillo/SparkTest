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

    val genAvg:RDD[(String,(Double,Int))] = merged.map({case(_,(gen,rat)) => (gen, (rat.toDouble,1))})
                                                  .reduceByKey((a,b) => ((a._1*a._2 + b._1*b._2)/(a._2 + b._2),(a._2 + b._2)))
    val result:RDD[(String,Double)] = genAvg.map({case (gen,(rat,_)) => (gen,rat)})
    result.foreach(println)

  }

}