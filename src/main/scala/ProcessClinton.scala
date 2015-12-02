import java.io.{FileOutputStream, PrintWriter}
import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object ProcessClinton {
  def main(args: Array[String]): Unit = {
    val emailPath = "email-head.csv"
    val stopwordsPath = "data/english-stop-words-large.txt"
    val vocabPath = "data/clinton/vocab.clinton.txt"
    val docwordPath = "data/clinton/docword.clinton.txt"

    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("Clinton"))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._    // TODO: necessary?

    val df = sqlContext.read.jdbc(
      url = "jdbc:sqlite:data/clinton/database.sqlite",
      table = "Emails",
      properties = new Properties())
    val rawCorpus = df.select("RawText")

    println("Sample email: " + rawCorpus.first.mkString("|"))

    println(s"Number of emails: ${rawCorpus.count()}")

    // read standard set of stopwords and add dataset-specific stopwords
    val standardStopwords = Source.fromFile(stopwordsPath).getLines().toSet
    val emailStopwords = Set(
      "subject", "unclassified", "confidential", "date", "case", "doc", "importance"
    ) union
      """STATE DEPT PRODUCED TO HOUSE SELECT BENGHAZI COMM
        |SUBJECT TO AGREEMENT ON SENSITIVE INFORMATION REDACTIONS NO FOIA WAIVER
        |RELEASE IN""".toLowerCase.split("[\\s\\n]+").toSet
    val stopwords = standardStopwords union emailStopwords
    val stopwordsBc = sc.broadcast(stopwords)

    val tokenRdd = rawCorpus.rdd.filter(!_.isNullAt(0)).mapPartitions { partition =>
      val stopwordsLocal = stopwordsBc.value
      partition.map { row =>
        val rawEmail = row.getAs[String](0)

        // clean email - remove metadata, try to preserve body of all emails in thread
        rawEmail
          .split("\\n")
          .filterNot(line => isMetadata(line))
          .mkString(" ")

        // tokenize
        val tokens = rawEmail
          .toLowerCase
          .split("[\\s-\\.]+")    // TODO: more comprehensive punctuation list?
          .map(token => token
          .replaceAll("[^a-z ]", "")
          .trim())
          .filterNot(_.isEmpty)

        // remove stop words
        tokens.filterNot(token => stopwordsLocal.contains(token))
      }
    }

    // To construct vocab, only keep words that have appeared at least twice
    val vocabCounts = tokenRdd.flatMap(_.toSeq).groupBy(word => word).mapValues(_.size)
    val vocab = vocabCounts.filter { case (_, count) => count > 1 }.keys.collect()
    val orderedVocab = vocab.zipWithIndex

    val vocabWriter = new PrintWriter(new FileOutputStream(vocabPath))
    orderedVocab foreach { case (word, _) =>
      vocabWriter.println(word)
    }
    vocabWriter.close()

    val vocabBc = sc.broadcast(orderedVocab.toMap)
    val docwordRdd = tokenRdd.mapPartitions { partition =>
      val vocabLocal = vocabBc.value
      partition.map { emailTokens =>
        // assemble token counts
        val tokenCount = mutable.Map[String, Int]()
        emailTokens foreach { token =>
          tokenCount.put(token, tokenCount.get(token).getOrElse(0) + 1)
        }
        // combine word id and token count
        // ignore words that don't appear in vocab
        tokenCount flatMap { case (token, count) =>
          vocabLocal.get(token).map { wordId => (wordId, count) }
        } toSeq
      }
    }

    val docwordWriter = new PrintWriter(new FileOutputStream(docwordPath))
    var docId = 1
    docwordRdd.collect() foreach { doc =>
      doc foreach { case (wordId, wordCount) =>
        docwordWriter.println(Seq(docId, wordId, wordCount).mkString(" "))
      }
      docId += 1
    }
    docwordWriter.close()

    sc.stop()
  }

  val metadataIndicators = Seq(
    "From", "Sent", "To", "Cc"
  )

  def isMetadata(line: String): Boolean = {
    metadataIndicators.exists(ind => line.startsWith(ind))
  }
}
