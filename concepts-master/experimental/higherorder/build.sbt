name := "concepts"

version := "0.4"

organization := "se.sics"

homepage := Some(url("https://www.sics.se/"))

licenses := Seq("Apache License" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

description := """A library and utilities for object similarity calculations and higher order concept discovery"""

scalaVersion := "2.10.5"

// Main framework versions
val sparkVersion = "1.6.0"
val hadoopVersion = "2.4.0"

// Compiler options
scalacOptions ++= Seq("-Xlint","-deprecation", "-feature", "-optimise")

// Scaladoc compiler options
autoAPIMappings := true
scalacOptions in (Compile,doc) := Seq("-doc-title", "Concepts", "-doc-root-content", "rootdoc.txt",
                                      "-groups", "-implicits")

// Managed dependencies
libraryDependencies  ++= Seq(
            "org.scala-lang" % "scala-reflect" % scalaVersion.value,
            "org.scalatest" %% "scalatest" % "2.2.4" % "test",
            ("org.apache.spark" %% "spark-core" % sparkVersion % "provided").
              exclude("org.apache.hadoop", "hadoop-client").
              exclude("org.mortbay.jetty", "servlet-api").
              exclude("commons-beanutils", "commons-beanutils-core").
              exclude("commons-collections", "commons-collections").
              exclude("commons-logging", "commons-logging").
              exclude("com.esotericsoftware.minlog", "minlog").
              exclude("jline", "jline").
              exclude("org.apache.commons", "commons-math3"),
            "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
            "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
            "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
            "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),
            "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
            "org.apache.commons" % "commons-math3" % "3.5",
            "com.github.scopt" %% "scopt" % "3.2.0",
            "com.typesafe.play" %% "play-json" % "2.3.1"
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

// Use / include all libraries for the run task, even those marked with provided
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// Do not run tests when assembling fat jar
test in assembly := {}

// Print warning if JDK != 1.7 is being used.
initialize := {
  val required = "1.7"
  val current  = sys.props("java.specification.version")
  if (current.toDouble < required.toDouble) {
    scala.Console.err.println("\nWARNING: Unsupported JDK, use JDK >= 1.7 to ensure compatibility!\n")
  }
}

// Do not perform parallel execution while testing
parallelExecution in Test := false

// Fork in run, test, and console
fork in run := true
fork in test := true
fork in console := true

javaOptions in run := Seq("-Xmx8G", "-server")

// Set initial and cleanup commands when entering console
initialCommands in console := """
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import se.sics.concepts._
import se.sics.concepts.graphs._
import se.sics.concepts.higherorder._
import se.sics.concepts.util._
import se.sics.concepts.io._
import se.sics.concepts.clustering._
import se.sics.concepts.classification._
import se.sics.concepts.measures._
// Configure and initialize Spark
val conf = new SparkConf()
  .setMaster("local")
  .setAppName("CNCPTS Console")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
  .set("spark.kryoserializer.buffer.mb", "128")
  .set("spark.shuffle.consolidateFiles", "true")
  .set("spark.eventLog.enabled", "true")
  .set("spark.eventLog.compress", "true")
  .set("spark.default.parallelism", (8).toString)
  .set("spark.shuffle.memoryFraction", (0.4).toString)
  .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig
val sc = new SparkContext(conf)
"""

cleanupCommands in console := """
sc.stop
"""
