name := "cncptgraph"

version := "0.1"

organization := "com.cncpt"

scalaVersion := "2.10.4"

//Managed dependencies

libraryDependencies  ++= Seq(
            "org.scala-lang" % "scala-reflect" % "2.10.4",
            "org.scalatest" %% "scalatest" % "1.9.1" % "test",
            "org.apache.spark" %% "spark-core" % "1.2.0",
            "org.apache.spark" %% "spark-graphx" % "1.2.0",
            "com.github.scopt" %% "scopt" % "3.2.0",
            "edu.stanford.nlp" % "stanford-corenlp" % "3.4",
            "edu.stanford.nlp" % "stanford-corenlp" % "3.4" classifier "models",
            "edu.stanford.nlp" % "stanford-parser" % "3.4",
            "com.typesafe.play" %% "play-json" % "2.2.1"
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

//libraryDependencies ++= Seq(
//  "org.digimead" %% "stopwatch-core" % "1.0-SNAPSHOT",
//  "org.digimead" %% "stopwatch-web" % "1.0-SNAPSHOT"
//)

//resolvers += "stopwatch" at "http://sbt-android-mill.github.com/stopwatch/releases"
//resolvers += Resolver.sonatypeRepo("public")

// Set initial commands when entering console

initialCommands in console := "import org.apache.spark.SparkContext; import org.apache.spark.SparkContext._; import org.apache.spark.SparkConf; import org.apache.spark.rdd.RDD; import org.apache.spark.graphx._; import com.sics.cgraph._"

// JVM Options

fork in run := true

javaOptions in run += "-Xmx4G"
