import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Row, SparkSession}

object LinearRegressionTest {

  def main(args:Array[String]) : Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("Advertising").getOrCreate()
    val adv = spark.read.option("header",true).option("inferSchema",true).csv("data/Advertising.csv")
    import spark.implicits._

    adv.show(5)
    val advDF = adv.rdd.map{ case Row(tv:Double, radio:Double, news:Double, sales:Double)  =>
                            (Vectors.dense(tv,radio,news),sales)}.toDF("features","label")

    advDF.show(5)

    val linearRegr = new LinearRegression().setFeaturesCol("features").setLabelCol("label")
    val model = linearRegr.fit(advDF)
    val pred = model.transform(advDF)
    pred.show(5)

    val evaluator = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("label")
    val pp = evaluator.setMetricName("r2").evaluate(pred)
    println(pp)
  }

}
