import org.apache.spark.{SparkConf, SparkContext}

object WordCounter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
    val sc = new SparkContext(conf)

    sc.textFile("file:///C:/Users/vitoc/Desktop/text.txt")
        .flatMap(_.split(",").drop(1))
        .map((_,1))
        .reduceByKey(_+_)
        .collect()
        .foreach(println)


  }

}
