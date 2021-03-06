
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext, ml}


object MLLibTest {

  val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
  val sc = new SparkContext(conf)
  val sparkSession = SparkSession.builder.master("local[*]").appName("Bank").getOrCreate()

  def main(args:Array[String]):Unit = {

    val runType = 2

    if (runType == 0) {
      KMeansRun()
    }
    else if (runType == 1) {
      NaiveBayesRun()
    }
    else {
      Bank()
    }



  }

  def KMeansRun() = {

    val v: Vector = Vectors.dense(1.0,2.0,3.0)

    val data = sc.textFile("data/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData,numClusters,numIterations)

    val squaredError = clusters.computeCost(parsedData)

    println("Squared error: " + squaredError)

    clusters.save(sc, "data/kmeans")

    //val kmeans_model = KMeansModel.load(sc,"data/kmeans")

  }

  def NaiveBayesRun() = {

    val data = MLUtils.loadLibSVMFile(sc, "data/sample_libsvm_data.txt")
    val Array(training, test) = data.randomSplit(Array(0.6,0.4))

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    val predictedLabel = test.map(t => (model.predict(t.features),t.label))
    val accuracy = 1.0 * predictedLabel.filter(x => x._1 == x._2).count() / test.count()

    println("Accuratezza: " + accuracy)

    model.save(sc, "data/NB_model")

    val sameModel = NaiveBayesModel.load(sc, "data/NB_model")

  }

  def Bank() = {

    val toInt = udf[Int, String]( _.toInt)

    val data = sparkSession.read.option("header","true").csv("data/bank.csv")
    val dataFixed = data.withColumn("age", toInt(data("age"))).withColumn("balance",toInt(data("balance")))
                      .withColumn("day",toInt(data("day"))).withColumn("duration",toInt(data("duration"))).withColumn("previous",toInt(data("previous")))


    // ottenere tutti gli attributi stringa
    val categoricals = dataFixed.dtypes.filter (_._2 == "StringType") map (_._1)

    // Conversione in numerico delle features categoriche
    val indexers = dataFixed.schema.fieldNames.map(header => new StringIndexer().setInputCol(header).setOutputCol(header +"_idx"))
/*    val encoders = categoricals.map (
      c => new OneHotEncoder().setInputCol(s"${c}_indexed").setOutputCol(s"${c}_enc")
    )*/
    val pipeline = new Pipeline().setStages(indexers)
    val pipelineModel = pipeline.fit(dataFixed)
    val modifiedDF = pipelineModel.transform(dataFixed)

    var cleanDF = modifiedDF

    for (i <- 0 until 17) {
      cleanDF = cleanDF.drop(cleanDF.columns(0))
    }

    cleanDF.printSchema()
    cleanDF.show(100,false)

    val bankRdd = cleanDF.rdd.map{r => (r.getAs[Double]("label_idx"),
                                        {
                                          var fields:Seq[Double] = Seq()
                                          for (c <- 0 to r.length - 2) { //metto -2 per escludere l'ultima colonna che contiene le label
                                            fields = fields :+ r.getAs[Double](c)
                                          }
                                          val fArray = fields.to[scala.Vector].toArray
                                          ml.linalg.Vectors.dense(fArray)
                                        })}

    val bankDF = sparkSession.createDataFrame(bankRdd).toDF("label", "features")
    bankDF.show(false)
    val Array(training,test) = bankDF.randomSplit(Array(0.7,0.3))
    val model = new ml.classification.NaiveBayes().setModelType("multinomial").setSmoothing(1.0).fit(training)
    val prediction = model.transform(test)
    prediction.show()

    val acc = 1.0*prediction.filter(x => x.getAs[Double](0) == x.getAs[Double](4)).count() / test.count()
    println("Accuratezza: " + acc)
    model.write.overwrite.save("data/bank_model")

  }

}
