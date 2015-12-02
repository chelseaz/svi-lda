name := "svi-lda"

version := "1.0"

libraryDependencies ++= {
  val sparkVersion = "1.5.2"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "org.rogach" %% "scallop" % "0.9.5",
    "org.xerial" % "sqlite-jdbc" % "3.8.11.2" % Runtime,
    "joda-time" % "joda-time" % "2.9.1",
    "org.joda" % "joda-convert" % "1.8.1"
  )
}

mainClass in assembly := Some("RunLDA")