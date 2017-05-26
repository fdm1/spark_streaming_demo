lazy val commonSettings = Seq(
	name := "twitterstreamer",
	scalaVersion := "2.11.8",
  version := "0.1",
  organization := "com.frankmassi"
)

lazy val projectDependencies = Seq(
    libraryDependencies ++= Seq(

    "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.1.1" % "provided",
    // "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided",
    // "org.apache.spark" %% "spark-mllib" % "2.1.1" % "provided",
    // "org.apache.spark" %% "spark-tags" % "2.1.1" % "provided",

    "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
    "com.google.code.gson" % "gson" % "2.8.0",
    "org.twitter4j" % "twitter4j-core" % "4.0.4"
    )

)

lazy val testDependencies = Seq(
  libraryDependencies ++= Seq(

	  "org.mockito" % "mockito-all" % "1.9.5" % "test",
    "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "junit" % "junit" % "4.10" % "test"
    // ,
    // // used for style-checking submissions
    // "org.scalastyle" %% "scalastyle" % "0.8.0"
  )
)

lazy val assemblySettings = Seq(
    assemblyMergeStrategy in assembly := {
      case PathList(xs @ _*) if xs.last == "UnusedStubClass.class" => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },

    unmanagedBase := baseDirectory.value / "external_jars",
		target in assembly := baseDirectory.value / "compiled_jars"
)

lazy val twitterstreamer = (project in file(".")).
	settings(commonSettings: _*).
	settings(projectDependencies: _*).
	settings(testDependencies: _*).
	settings(assemblySettings: _*)
