import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("myApp")
    val sc = new SparkContext(conf)
    var myList = List((1,'c'),(2,'b'))
    var newList = (1 to 10).toList

    val myRDD = sc.parallelize(newList)

    var lst = myRDD.map(a => a*a).filter(a => a > 10)
    lst.foreach(println)
  }

}