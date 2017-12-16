import org.apache.spark.{SparkConf, SparkContext}

object WordCounter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
    val sc = new SparkContext(conf)

    sc.textFile("file:///C:/Users/vitoc/Desktop/text.txt")
        .flatMap(_.split(",").drop(1))
        .map((_,1))
        .reduceByKey(_+_) //cosa fare con i valori, in questo caso li somma perchè ha già raggruppato per chiave
        .collect()
        .foreach(println)

  }

}
