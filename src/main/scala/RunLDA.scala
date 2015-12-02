import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.rogach.scallop._
import splash.clustering.WordToken

/**
  * Run-specific parameters
  * @param numTopics
  * @param kappa
  * @param dateStr
  */
case class RunParams(numTopics: Int, miniBatchFraction: Double, kappa: Double, dateStr: String)

/**
  * Application parameters
  * @param args
  */
class RunConfig(args: Array[String]) extends ScallopConf(args) {
  val dataset = opt[String](required = true)
  val dataPath = opt[String](default = Some("data"))
  val logPath = opt[String](default = Some("log"))
  val master = opt[String](default = Some("local[4]"))
  val numPartitions = opt[Int](default = Some(4))
  val numIterations = opt[Int](default = Some(1000))
  val maxTrainingTime = opt[Int](default = Some(100000))  // in seconds
  val tau0 = opt[Int](default = Some(1024))
  val allNumTopics = opt[List[Int]](default = Some(List(20)))
  val allMinibatchFractions = opt[List[Double]](default = Some(List(0.01)))
  val allKappa = opt[List[Double]](default = Some(List(0.5)))
}

object RunLDA {
  def runOne(conf: RunConfig,
             runParams: RunParams,
             corpus: RDD[(Int, Array[WordToken])],
             vocab: Array[String]): Unit = {

    def logPathFor(logType: String): String =
      s"""${conf.logPath()}/
         |${logType}-
         |${conf.dataset()}-
         |${runParams.numTopics}-
         |${"%.3f" format runParams.miniBatchFraction}-
         |${"%.2f" format runParams.kappa}-
         |${runParams.dateStr}.log""".stripMargin.replace("\n", "")
    val logFile = LogFile(logPathFor("iter"), logPathFor("topics"))

    // LDA parameters
    val params = SVILDAParams(
      D = corpus.count(),
      N = vocab.length,
      K = runParams.numTopics,
      tau0 = conf.tau0(),
      kappa = runParams.kappa,
      miniBatchFraction = runParams.miniBatchFraction,
      numIterations = conf.numIterations(),
      maxTrainingTime = conf.maxTrainingTime()
    )
    println(s"Training LDA model with params:\n${params.toString()}")
    val topics = new LDAModel().train(params, corpus, logFile, vocab)
    topics.zipWithIndex foreach { case (topWords, topicId) =>
      println(s"Topic ${topicId+1}: ${topWords.mkString(",")}")
    }
  }

  // Read data in UCI bag-of-words format and train LDA model
  def main(args: Array[String]) {
    val conf = new RunConfig(args)

    val dataset = conf.dataset()
    val dateStr = DateTimeFormat.forPattern("yyyyMMdd-HHmmss").print(DateTime.now())

    val sc = new SparkContext(new SparkConf().setMaster(conf.master()).setAppName(s"SVI-LDA-${dataset}"))
    val docwordPath = s"${conf.dataPath()}/${dataset}/docword.${dataset}.txt"
    val vocabPath = s"${conf.dataPath()}/${dataset}/vocab.${dataset}.txt"

    // read the document-word table from file
    val corpus = sc.textFile(docwordPath).flatMap( x => {
      try {
        val tokens = x.split(" ")
        val docId = tokens(0).toInt - 1
        val wordId = tokens(1).toInt - 1
        val wordFreq = tokens(2).toInt
        Some((docId, new WordToken(wordId, wordFreq, Array.empty)))
      }
      catch { case e: Throwable =>
        // some lines in docword are badly formatted, e.g in nytimes corpus
        None
      }
    }).groupByKey().mapValues(x => x.toArray).repartition(conf.numPartitions()).cache()
    println(s"Number of documents: ${corpus.count()}")

    // read the vocabulary from file
    val vocab = sc.textFile(vocabPath).collect()
    println(s"Vocabulary size: ${vocab.length}")

//    val allNumTopics = List(20, 50, 100)
//    val allMinibatchFractions = List(0.001, 0.01, 0.1)
//    val allKappa = List(0.5, 0.8, 1.0)
    val allNumTopics = conf.allNumTopics()
    val allMinibatchFractions = conf.allMinibatchFractions()
    val allKappa = conf.allKappa()

    allNumTopics foreach {numTopics =>
      allMinibatchFractions foreach {miniBatchFraction =>
        allKappa foreach { kappa =>
          val runParams = RunParams(
            numTopics = numTopics,
            miniBatchFraction = miniBatchFraction,
            kappa = kappa,
            dateStr = dateStr)
          runOne(conf, runParams, corpus, vocab)
        }
      }
    }

    sc.stop()
  }
}
