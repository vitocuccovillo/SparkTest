import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
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
    println("Coefficients: " + model.coefficients + " Intercept: " + model.intercept)
    pred.show(5)

    val evaluator = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("label")
    val pp = evaluator.setMetricName("r2").evaluate(pred)
    println(pp)


    //CROSS FOLD VALIDATION

    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("label")

    val param_grid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0, 0.5, 1))
      .addGrid(lr.elasticNetParam, Array(0, 0.5, 1))
      .build()

    val cv_evaluator = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("label").setMetricName("r2")

    val Array(train,test) = advDF.randomSplit(Array(0.8,0.2),123)

    val cv = new CrossValidator().setEstimator(lr).setEstimatorParamMaps(param_grid).setEvaluator(cv_evaluator).setNumFolds(4)
    val cv_model = cv.fit(train)
    println(cv_model.bestModel.explainParams())

    val pred_training_cv = cv_model.transform(train)
    val pred_test_cv = cv_model.transform(test)

    println(evaluator.setMetricName("r2").evaluate(pred_training_cv))
    println(evaluator.setMetricName("r2").evaluate(pred_test_cv))

    val bb = cv_model.bestModel.asInstanceOf[LinearRegressionModel]
    println("CV COEFF: " + bb.coefficients)
    println("CV INTER: " + bb.intercept)

  }

}
