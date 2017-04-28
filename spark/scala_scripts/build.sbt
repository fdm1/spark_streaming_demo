lazy val root = (project in file(".")).settings(

    name := "SparkStreaming1",
    version := "1.0",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(

    "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-tags" % "2.1.0" % "provided",

    "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
    "com.google.code.gson" % "gson" % "2.8.0"

    ),
    assemblyMergeStrategy in assembly := {
      case PathList(xs @ _*) if xs.last == "UnusedStubClass.class" => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },

    unmanagedBase := baseDirectory.value / "external_jars"
  )

