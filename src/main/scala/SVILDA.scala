import breeze.linalg.{*, DenseMatrix, DenseVector, sum}
import breeze.numerics._
import breeze.stats.distributions.Gamma
import org.apache.spark.rdd.RDD
import splash.clustering.WordToken
import splash.core.{LocalVariableSet, SharedVariableSet, SplashConf, ParametrizedRDD}

case class SVILDAParams(D: Long,  // corpus size
                        N: Int,  // vocab size
                        K: Int,  // number of topics
                        tau0: Double,  // delay
                        kappa: Double,  // forgetting rate
                        miniBatchFraction: Double,  // fraction of documents to sample every iteration
                        numIterations: Int,
                        maxTrainingTime: Int,
                        gammaShape: Double = 100) {

  val alpha: DenseVector[Double] = DenseVector.fill(K){1.0 / K}  // doc concentration is exchangeable Dirichlet prior
  val eta: Double = 1.0 / K  // topic concentration is exchangeable Dirichlet prior

  override def toString() =
    s"""corpus size D=${D}
       |vocab size N=${N}
       |number of topics K=${K}
       |delay tau0=${tau0}
       |forgetting rate kappa=${kappa}
       |minibatch fraction=${miniBatchFraction}
       |number of iterations=${numIterations}
       |max training time (s)=${maxTrainingTime}
       |gamma shape param=${gammaShape}
       |""".stripMargin
}

/**
  * Adaptation of Spark OnlineLDAOptimizer to work with Splash
  */
class LDAModel(maxThreadNum: Int = 0) {

  def train(params: SVILDAParams,
            data: RDD[(Int, Array[WordToken])],
            logFile: LogFile,
            vocab: Array[String]    // only used in driver for looking up top words
           ): Seq[Array[String]] = {
    val paramRdd = new ParametrizedRDD(data)
      .setProcessFunction(LDAModel.process(params))

    initialize(params, paramRdd)

    // Run SVI via Splash
    val spc = (new SplashConf)
      .set("auto.thread", false)
      .set("max.thread.num", maxThreadNum)
      .set("data.per.iteration", params.miniBatchFraction)

    var i = 1
    while (i <= params.numIterations && paramRdd.totalTimeEllapsed < params.maxTrainingTime) {
      paramRdd.run(spc)
      paramRdd.syncSharedVariable()    // TODO: necessary?

      val sv = paramRdd.getSharedVariable()
      val logLik = LDAModel.logLikelihoodBound(params, data, sv)
      val currTopics = getCurrentTopics(params, paramRdd, vocab)

      println(s"Iter ${i} (time ${"%5.3f" format paramRdd.totalTimeEllapsed}): Log likelihood = ${"%5.8f" format logLik}")
      logFile.appendIterLog(i, paramRdd.totalTimeEllapsed, logLik)
      logFile.writeTopics(currTopics)

      i += 1
    }

    paramRdd.syncSharedVariable()    // TODO: necessary?
    val sv = paramRdd.getSharedVariable()

    // Sanity check: for some reason we must have # partitions <= # cores, otherwise we get fewer iterations than requested.
    val finalIter = sv.get(LDASharedVariable.iteration)
    println(s"Ran ${finalIter} iterations")

    getCurrentTopics(params, paramRdd, vocab)
  }

  def getCurrentTopics(params: SVILDAParams, paramRdd: ParametrizedRDD[(Int, Array[WordToken])], vocab: Array[String]): Seq[Array[String]] = {
    val sv = paramRdd.getSharedVariable()
    val topics = (1 to params.K) map { k =>
      sv.getArray(LDASharedVariable.lambda(k))
    }
    topics.zipWithIndex map { case (topic, topicId) =>
      val topicSorted = topic.zipWithIndex.sortWith((a, b) => a._1 > b._1)
      val topWords = topicSorted.take(20).map { case (prob, wordId) => vocab(wordId) }
      topWords
    }
  }

  def initialize(params: SVILDAParams, paramRdd: ParametrizedRDD[(Int, Array[WordToken])]) = {
    // Initialize shared variables
    paramRdd.foreachSharedVariable { sharedVar =>
      sharedVar.set(LDASharedVariable.iteration, 0)
      (1 to params.K) foreach {k =>
        sharedVar.setArray(LDASharedVariable.lambda(k), Util.randomGammaArray(params.N, params.gammaShape))
      }
    }

    paramRdd.syncSharedVariable()
  }

  def lossFunction() = {
    (doc: (Int, Array[WordToken]), sharedVar : SharedVariableSet, localVar: LocalVariableSet) => {
      0.0
    }
  }
}

object LDAModel {
  // make all these static to avoid serializing LDAModel class, which throws an error

  def rho(params: SVILDAParams, iter: Int): Double = {
    math.pow(params.tau0 + iter, -params.kappa)
  }

  def updateLambda(params: SVILDAParams,
                   k: Int, sharedVar: SharedVariableSet, iter: Int, weight: Double, stat: DenseVector[Double]) = {
    // Splash weight is applied for add operations, but not multiply
    val r = rho(params, iter)
    sharedVar.multiplyArray(LDASharedVariable.lambda(k), 1 - r)
    val newContribution = (stat :*= params.D.toDouble :+= params.eta) :*= (r * weight)
    sharedVar.addArray(LDASharedVariable.lambda(k), newContribution.toArray)
  }

  def process(params: SVILDAParams)
             (doc: (Int, Array[WordToken]), weight: Double, sharedVar: SharedVariableSet, localVar: LocalVariableSet) = {
    val ids = doc._2.map(_.wordId).toList
    val ctsVector = DenseVector(doc._2.map(_.wordCount.toDouble))

    // retrieve current iteration and global variables
    val iter = sharedVar.get(LDASharedVariable.iteration).toInt
    val expElogBeta_byK = (1 to params.K) map {k =>
      val lambda = DenseVector(sharedVar.getArray(LDASharedVariable.lambda(k)))
      exp(Util.dirichletExpectation(lambda))    // N
    }

    // local updates
    val expElogBeta_full = DenseMatrix.horzcat(expElogBeta_byK.map(_.toDenseMatrix.t): _*)    // N x K
    // restrict attention to words in doc (ids)
    // TODO: just retrieve lambda for ids from shared variable
    val expElogBeta_ids = expElogBeta_full(ids, ::).toDenseMatrix    // ids x K
    val stat_ids = localUpdates(params, expElogBeta_ids, ctsVector)._2    // ids x K
    // expand to entire vocab for global update
    val stat = DenseMatrix.zeros[Double](params.N, params.K)    // N x K
    stat(ids, ::) := stat_ids

    // global updates
    (1 to params.K) foreach {k =>
      updateLambda(params, k, sharedVar, iter, weight, stat(::, k-1))
    }

    // increment iteration
    sharedVar.add(LDASharedVariable.iteration, weight)
  }

  def localUpdates(params: SVILDAParams,
                   expElogBeta_ids: DenseMatrix[Double],    // ids x K
                   ctsVector: DenseVector[Double]    // ids
                  ): (DenseVector[Double], DenseMatrix[Double]) = {
    var gamma_d = Util.randomGammaVector(params.K, params.gammaShape)    // K
    var lastGamma_d = gamma_d

    var meanGammaChange = 1D
    var expElogTheta_d = DenseVector.zeros[Double](params.K)    // K
    var phiNorm = DenseVector.zeros[Double](expElogBeta_ids.rows)    // ids

    var iter = 0
    // Until phi and gamma converge
    while (meanGammaChange > 1e-3 && iter < 1000) {
      expElogTheta_d = exp(Util.dirichletExpectation(gamma_d))    // K
      phiNorm = expElogBeta_ids * expElogTheta_d :+= 1e-100    // ids
      val expElogBetad_part = expElogBeta_ids.t * (ctsVector :/ phiNorm)    // K
      lastGamma_d = gamma_d
      gamma_d = expElogTheta_d :* expElogBetad_part :+ params.alpha    // K
      meanGammaChange = sum(abs(gamma_d - lastGamma_d)) / params.K
      iter += 1
    }

    val sstats_d: DenseMatrix[Double] = (ctsVector :/ phiNorm).asDenseMatrix.t * expElogTheta_d.asDenseMatrix    // ids x K
    (gamma_d, sstats_d :* expElogBeta_ids)
  }

  /**
    * compute lower bound on log likelihood
    * based on Eq 3 in Hoffman, Blei, Bach (2010) and MLLib LDAModel
    * @return
    */
  def logLikelihoodBound(params: SVILDAParams,
                         data: RDD[(Int, Array[WordToken])], sharedVar: SharedVariableSet): Double = {
    val lambda_byK = (1 to params.K) map {k =>
      DenseVector(sharedVar.getArray(LDASharedVariable.lambda(k)))
    }
    val lambda = DenseMatrix.horzcat(lambda_byK.map(_.toDenseMatrix.t): _*)    // N x K

    val ElogBeta_byK = lambda_byK map Util.dirichletExpectation
    val ElogBeta_full = DenseMatrix.horzcat(ElogBeta_byK.map(_.toDenseMatrix.t): _*)    // N x K
    val ElogBeta_bc = data.sparkContext.broadcast(ElogBeta_full)

    val corpusPart = data.map { case (docId, words) =>
      val ids = words.map(_.wordId).toList
      val ctsVector = DenseVector(words.map(_.wordCount.toDouble))

      val ElogBeta_local = ElogBeta_bc.value
      val ElogBeta_ids = ElogBeta_local(ids, ::).toDenseMatrix    // ids x K
    val gamma_d = localUpdates(params, exp(ElogBeta_ids), ctsVector)._1    // K
    val ElogTheta_d = Util.dirichletExpectation(gamma_d)    // K

      // Eq 28 in Hoffman et al (2013)
      val Pz_dnk = exp(ElogBeta_ids.t(::, *) + ElogTheta_d)    // K x ids
    // sum columns (over K), take log, sum row (over ids)
    // E[log p(doc | theta, beta)]
    val ElogPdoc_given_thetaBeta = sum(log(sum(Pz_dnk(::, *))))

      // Eq (14)-(15) in Blei, Ng, Jordan (2003)
      // E[log p(theta | alpha) - log q(theta | gamma)]
      ElogPdoc_given_thetaBeta +
        sum((params.alpha - gamma_d) :* ElogTheta_d) +
        sum(lgamma(gamma_d) - lgamma(params.alpha)) +
        lgamma(sum(params.alpha)) - lgamma(sum(gamma_d))
    }.sum()

    ElogBeta_bc.unpersist()

    // E[log p(beta | eta) - log q(beta | lambda)]
    val sumEta = params.eta * params.N
    val topicsPart = sum((params.eta - lambda) :* ElogBeta_full) +
      sum(lgamma(lambda) - lgamma(params.eta)) +
      sum(lgamma(sumEta) - lgamma(sum(lambda(::, *))))

    corpusPart + topicsPart
  }
}

object LDASharedVariable {
  val iteration = "iteration"
  def lambda(k: Int) = s"lambda${k}"
}

object Util {
  /**
    * Get a random array to initialize lambda.
    * Adapted from OnlineLDAOptimizer
    */
  def randomGammaArray(dim: Int, gammaShape: Double): Array[Double] = {
    randomGammaVector(dim, gammaShape).toArray
  }

  def randomGammaVector(dim: Int, gammaShape: Double): DenseVector[Double] = {
    new Gamma(gammaShape, 1.0 / gammaShape).samplesVector(dim)
  }

  def dirichletExpectation(vec: DenseVector[Double]): DenseVector[Double] = {
    digamma(vec) - digamma(sum(vec))
  }
}
