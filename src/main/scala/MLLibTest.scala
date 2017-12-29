
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

    //val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").load("data/bank-full.csv")
    //df.printSchema()
    val data = sparkSession.read.option("header","true").csv("data/bank-full.csv")
    val dataFixed = data.withColumn("age", toInt(data("age"))).withColumn("balance",toInt(data("balance")))
                      .withColumn("day",toInt(data("day"))).withColumn("duration",toInt(data("duration"))).withColumn("previous",toInt(data("previous")))
    dataFixed.show()

    val bankRdd = dataFixed.rdd.map{r => ({if (r.getAs[String]("label") == "yes") 1 else 0},
                                          {val testDouble = Seq(r.getAs[Int]("age"),
                                            r.getAs[Int]("duration"),
                                            r.getAs[Int]("day"),
                                            r.getAs[Int]("previous"))
                                            .map(x=>x.toDouble).to[scala.Vector].toArray
                                            ml.linalg.Vectors.dense(testDouble)}
                                          )}

    //catMaps.foreach(println)

    //val ppp =  catMaps.map { case (id, catMap) => id -> Row.fromSeq(catMap) }
    val bankDF = sparkSession.createDataFrame(bankRdd).toDF("label", "features")

    bankDF.show()
    val Array(training,test) = bankDF.randomSplit(Array(0.7,0.3))

    val model = new ml.classification.NaiveBayes().fit(training)
    val prediction = model.transform(test)
    prediction.show()
    model.save("data/bank_model")

  }

}
