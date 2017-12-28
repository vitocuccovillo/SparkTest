import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object MLLibTest {

  val conf = new SparkConf().setMaster("local[*]").setAppName("myApp")
  val sc = new SparkContext(conf)

  def main(args:Array[String]):Unit = {

    val runType = 1

    if (runType == 0) {
      KMeansRun()
    }
    else {
      NaiveBayesRun()
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
}
