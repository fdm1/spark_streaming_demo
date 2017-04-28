lazy val root = (project in file(".")).settings(
    name := "SparkStreaming1",
    version := "1.0",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(

    // base spark packages
    "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-tags" % "2.1.0" % "provided",

    // https://mvnrepository.com/artifact/org.apache.bahir/spark-streaming-twitter_2.11
    "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",

    // https://mvnrepository.com/artifact/com.google.code.gson/gson
    "com.google.code.gson" % "gson" % "2.8.0",

    // https://mvnrepository.com/artifact/org.twitter4j/twitter4j-core
    "org.twitter4j" % "twitter4j-core" % "4.0.6",
    // https://mvnrepository.com/artifact/org.twitter4j/twitter4j-stream
    "org.twitter4j" % "twitter4j-stream" % "4.0.6"


    ),
    assemblyMergeStrategy in assembly := {
      case PathList(xs @ _*) if xs.last == "UnusedStubClass.class" => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
