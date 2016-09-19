name := "concepts"

version := "0.1"

organization := "se.sics"

scalaVersion := "2.10.4"

val sparkVersion = "1.5.0"

//Managed dependencies

libraryDependencies  ++= Seq(
            "org.scala-lang" % "scala-reflect" % "2.10.4" % "provided",
            "org.scalatest" %% "scalatest" % "2.2.4" % "test",
            ("org.apache.spark" %% "spark-core" % sparkVersion % "provided").
              exclude("org.mortbay.jetty", "servlet-api").
              exclude("commons-beanutils", "commons-beanutils-core").
              exclude("commons-collections", "commons-collections").
              exclude("commons-logging", "commons-logging").
              exclude("com.esotericsoftware.minlog", "minlog").
              exclude("jline", "jline").
              exclude("org.apache.commons", "commons-math3"),
            "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
            "com.github.scopt" %% "scopt" % "3.2.0",
            "edu.stanford.nlp" % "stanford-corenlp" % "3.4",
            "edu.stanford.nlp" % "stanford-corenlp" % "3.4" classifier "models",
            "edu.stanford.nlp" % "stanford-parser" % "3.4",
            "com.typesafe.play" %% "play-json" % "2.3.1"
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

//libraryDependencies ++= Seq(
//  "org.digimead" %% "stopwatch-core" % "1.0-SNAPSHOT",
//  "org.digimead" %% "stopwatch-web" % "1.0-SNAPSHOT"
//)

//resolvers += "stopwatch" at "http://sbt-android-mill.github.com/stopwatch/releases"
//resolvers += Resolver.sonatypeRepo("public")

// Set initial commands when entering console

initialCommands in console := """
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import se.sics.concepts._
// Configure and initialize Spark
val conf = new SparkConf()
  .setMaster("local")
  .setAppName("CNCPT Console")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
  .set("spark.kryoserializer.buffer.mb", "128")
  .set("spark.shuffle.consolidateFiles", "true")
  .set("spark.eventLog.enabled", "true")
  .set("spark.eventLog.compress", "true")
  .set("spark.default.parallelism", (16).toString)
  .set("spark.shuffle.memoryFraction", (0.4).toString)
  .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig
  // .set("spark.storage.memoryFraction", (0.4).toString)
val sc = new SparkContext(conf)
"""

excludeFilter in unmanagedSources := HiddenFileFilter || "*clique*"

test in assembly := {}

// JVM Options

fork in run := true

javaOptions in run += "-Xmx8G"
