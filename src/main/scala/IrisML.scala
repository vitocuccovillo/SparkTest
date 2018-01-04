import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}

object IrisML {

  def main(args:Array[String]) : Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("Bank").getOrCreate()
    val _iris = spark.read.option("header",true).option("inferSchema",true).csv("data/iris.csv")
    //_iris.printSchema()
    _iris.show(5)

    import spark.implicits._

    val iris = _iris.rdd.map{case Row(sl:Double,sw:Double,pl:Double,pw:Double,s:String) =>
                                      (Vectors.dense(sl,sw,pl,pw),s)
                            }.toDF("features","species")
    iris.show()


  }

}
