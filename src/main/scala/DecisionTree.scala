import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

object DecisionTree {

  def main(args:Array[String]) : Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("DecisionTree").getOrCreate()
    val cuse = spark.read.option("header",true).option("inferSchema",true).csv("data/cuse_binary.csv")

    cuse.show(5)

    val categorical_cols = cuse.columns.toSeq.take(4)

    val stringIdxStage = for (c <- categorical_cols) yield new StringIndexer().setInputCol(c).setOutputCol(c + "_idx")
    stringIdxStage.toArray :+ new StringIndexer().setInputCol("y").setOutputCol("label")

    println(stringIdxStage)

    val onehotencoderStages = for (c <- categorical_cols) yield new OneHotEncoder().setInputCol(c + "_idx").setOutputCol(c + "_onehot")

    val features = for (c <- categorical_cols) yield "onehot_" + c

    println(onehotencoderStages)
    println(features)

/*    val vectorassembler_stage = new VectorAssembler().setInputCols(features.toArray).setOutputCol("features")

    val pipeline = new Pipeline().setStages(Array(stringIdxStage, onehotencoderStages,  vectorassembler_stage))

    val pipeline_model = pipeline.fit(cuse)
    val final_columns = features +: Array("features", "label")

    val cuse_df = pipeline_model.transform(cuse)

    cuse_df.show(5)*/

/*    val Array(training, test) = cuse_df.randomSplit(Array(0.8, 0.2), 1234)

    val dt = new DecisionTreeClassifier().setFeaturesCol("features").setLabelCol("label")
    val param_grid = new ParamGridBuilder().addGrid(dt.maxDepth, Array(2,3,4,5)).build()
    val evaluator = new BinaryClassificationEvaluator().setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
    val cv = new CrossValidator().setEstimator(dt).setEstimatorParamMaps(param_grid).setEvaluator(evaluator).setNumFolds(4)
    val cv_model = cv.fit(cuse_df)

    val show_columns = Array("features", "label", "prediction", "rawPrediction", "probability")

    val pred_training_cv = cv_model.transform(training)
    val pred_training_cv.select(show_columns).show(5, false)
    val pred_test_cv = cv_model.transform(test)
    val pred_test_cv.select(show_columns).show(5, false)

    val label_and_pred = cv_model.transform(cuse_df).select("label", "prediction")
    label_and_pred.rdd.zipWithIndex().countByKey()

    println("The best MaxDepth is:", cv_model.bestModel)*/

  }

}
