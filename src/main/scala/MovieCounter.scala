import org.apache.spark.{SparkConf, SparkContext}

object MovieCounter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
    val sc = new SparkContext(conf)

    var moviesDB = sc.textFile("file:///C:/Users/vitoc/Desktop/Materiale Tesi/Esercitazione/moviesLight.csv")
                     .flatMap(movie => movie.split(",").takeRight(1)(0).split('|'))
                     .map((_,1))
                     .reduceByKey(_+_)
                     .collect.toMap
                     .foreach(println)

  }

}

