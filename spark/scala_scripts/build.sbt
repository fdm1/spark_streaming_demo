lazy val commonSettings = Seq(
	name := "SparkStreaming",
	scalaVersion := "2.11.8",
  version := "0.1",
  organization := "com.frankmassi"
)

lazy val projectDependencies = Seq(
    libraryDependencies ++= Seq(

    "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-tags" % "2.1.0" % "provided",

    "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
    "com.google.code.gson" % "gson" % "2.8.0",
    "org.twitter4j" % "twitter4j-core" % "4.0.4"

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

lazy val root = (project in file(".")).
	settings(commonSettings: _*).
	settings(projectDependencies: _*).
	settings(assemblySettings: _*)
