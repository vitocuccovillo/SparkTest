import org.apache.spark.sql.SparkSession

object DecisionTree {

  def main(args:Array[String]) : Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("DecisionTree").getOrCreate()
    val adv = spark.read.option("header",true).option("inferSchema",true).csv("data/cuse_binary.csv")

    adv.show(5)


  }

}
