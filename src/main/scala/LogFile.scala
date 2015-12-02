import java.io.{FileOutputStream, PrintWriter}

case class LogFile(iterPath: String, topicsPath: String) {
  initIterLog()

  def initIterLog(): Unit = {
    val writer = new PrintWriter(new FileOutputStream(iterPath))
    writer.println("Iter,Time,LogLik")
    writer.close()
  }

  def appendIterLog(iter: Int, time: Double, logLik: Double): Unit = {
    val writer = new PrintWriter(new FileOutputStream(iterPath, true))
    writer.println(s"${iter},${"%5.3f" format time},${"%5.8f" format logLik}")
    writer.close()
  }

  /**
    * Write file of topics, overwriting if necessary.
    * @param topics
    */
  def writeTopics(topics: Seq[Array[String]]): Unit = {
    val writer = new PrintWriter(new FileOutputStream(topicsPath))
    topics.foreach { words =>
      writer.println(words.mkString(","))
    }
    writer.close()
  }
}