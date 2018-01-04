import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}

object IrisML {

  // FROM: https://github.com/MingChen0919/learning-apache-spark/blob/master/naive-bayes-classification.ipynb

  def main(args:Array[String]) : Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("Bank").getOrCreate()
    val _iris = spark.read.option("header",true).option("inferSchema",true).csv("data/iris.csv")

    import spark.implicits._

    val iris = _iris.rdd.map{case Row(sl:Double,sw:Double,pl:Double,pw:Double,s:String) =>
                                      (Vectors.dense(sl,sw,pl,pw),s)
                            }.toDF("features","species")
    iris.show()

    val stringIndexer = new StringIndexer().setInputCol("species").setOutputCol("label")
    val stages = Array(stringIndexer)
    val pipeline = new Pipeline().setStages(stages)

    val iris_df = pipeline.fit(iris).transform(iris)
    iris_df.show(5)
    iris_df.describe().show(5)

    val Array(train, test) = iris_df.randomSplit(Array(0.8, 0.2), seed=1234)

    val model = new NaiveBayes().setModelType("multinomial").setSmoothing(1.0).fit(train)
    val prediction = model.transform(test)
    prediction.show()

    val acc = 1.0*prediction.filter(x => x.getAs[Double](2) == x.getAs[Double](5)).count() / test.count()
    println("---------------------------------------")
    println()
    println("ACCURATEZZA: " + acc)
    println
    println("---------------------------------------")
    model.write.overwrite.save("data/iris_model")

  }

}
