package se.sics.concepts.examples

// # Copy first 10 files from . to ../tweets_sample
// find . -maxdepth 1 -type f |head -10|xargs cp -t ../tweets_sample

import java.io._

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import se.sics.concepts.core.CKryoRegistrator

// TODO: Modify to allow for multiple benchmarks being run at once.

object WordPairBenchmarks {

  // Easy way to print to file. Source: http://stackoverflow.com/a/4608061/209882
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  case class Config(inputPath: String = "", experimentName: String = "",
    localDir: String = "", noCores: Int = 4, benchmarkName: String = "",
    maxDegree: Long = Long.MaxValue,
    vtxRange: (Double, Double) = (1.0E-4, 1.0E-3),
    edgeRange: (Double, Double) = (0.01, 1.0))

  val argParser = new scopt.OptionParser[Config]("CNCPT") {
    head("Concepts WordPairBenchmarks", "")

    opt[String]('p', "input-path") valueName ("<execution-name>") required () action { (x, c) =>
      c.copy(inputPath = x)
    } text ("input-path should point to the base directory with the input files, i.e. [output]/<execution-name>")

    opt[String]('b', "benchmark-name") required () valueName ("WS-353, MTURK-771, SIMLEX-999") action { (x, c) =>
      c.copy(benchmarkName = x)
    } text ("The name of the benchmark.")

    opt[String]('e', "experiment-name") optional () action { (x, c) =>
      c.copy(experimentName = x)
    } text ("Provide a name for the experiment, [output]/<execution-name>/[<experiment-name>] (optional)")

    opt[String]('l', "local-dir") valueName ("<path-to-tmp>") optional () action { (x, c) =>
      c.copy(localDir = x)
    } text ("Provide a path for where Spark should generate temp files (optional)")

    // TODO: Indicate the master here
    opt[Int]('c', "no-cores") optional () action { (x, c) =>
      c.copy(noCores = x)
    } text ("Indicate the number of cores to use in the experiment (optional)")

    help("help")
  }

  def main(args: Array[String]) {

    val arguments = argParser.parse(args, Config())

    val path = arguments.get.inputPath
    val benchmarkName = arguments.get.benchmarkName
    val experimentName = arguments.get.experimentName
    val localDir = arguments.get.localDir
    val noCores = arguments.get.noCores

    // Assuming execution from <repo-root>/code, and output placed in code/output
    // dirPath is "output/<execution-name>"
    val dirPath = new File("output", path)
    // inputPath is "output/<execution-name>/<execution-name>"
    val inputPath = new File(dirPath, path)
    // experimentPath is "output/<execution-name>/<experiment-name>"
    val experimentPath = {
      if (experimentName == "") dirPath
      else new File(dirPath, experimentName)
    }
    // simPath is the path to the similarity graph files
    val simPath = {
      if (experimentName == "") inputPath + "-string-sim-edges"
      else new File(experimentPath, "string-sim-edges").toString
    }

    val conf = new SparkConf()
      .setMaster("local[" + noCores.toString + "]")
      .setAppName("CNCPT WordPairBenchmarks")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.compress", "true")
      .set("spark.local.dir", localDir)
      .set("spark.eventLog.dir", localDir + "/spark-events")
      .set("spark.default.parallelism", (noCores * 3).toString)
      .set("spark.shuffle.memoryFraction", (0.4).toString)
      .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig

    val sc = new SparkContext(conf)

    println("Reading graph from file")
    val similarityGraph: RDD[(Set[String], Double)] = sc.textFile(simPath)
      .map(s => s.replaceAll("\\(|\\)", "").split(","))
      .map(s => ((s(0).trim, s(1).trim), s(2).trim.toDouble))
      .map{case ((i, j), s) => (Set(i, j), s)}

    println("Reading benchmark file")
    val benchmarkRDD: RDD[(Set[String], Double)] = readBencmark(benchmarkName, sc)
      .map{case ((i, j), s) => (Set(i.toLowerCase, j.toLowerCase), s)}

    println("Finding common pairs")
    // TODO: Which is the fastest way to join these two, given that benchmarkRDD is tiny
    // compared to similarityGraph?
    val commonPairs = similarityGraph
      .rightOuterJoin(benchmarkRDD)
      .map{case (k, vals) => (k.mkString(","), vals)}

    // Create dirs fpr output
    // TODO: Use modified version of createOutputDir to better handle failures to create dirs
    val benchPath = new File(experimentPath, "wordBenchmarks")
    benchPath.mkdirs
    printToFile(new File(benchPath,  "commonPairs-" + benchmarkName))(p => {
      commonPairs.collect.foreach(p.println)
    })

    // Remove cases for which we couldn't find a similarity value
    val existingPairs = commonPairs
     .filter{case (k, (s1, s2)) => !s1.isEmpty}
     .map{case (k, (s1, s2)) => (s1.get, s2)}

     printToFile(new File(benchPath,  "existingPairs-" + benchmarkName))(p => {
       existingPairs.collect.foreach(p.println)
     })

    sc.stop
  }

  def readBencmark(benchmarkName: String, @transient sc: SparkContext): RDD[((String, String), Double)] = {
    if (benchmarkName == "WS-353") {
      parseWS(sc)
    } else if (benchmarkName == "WS-353-REL") {
      parseWSRel(sc)
    } else if (benchmarkName == "MTURK-771") {
      parseMT(sc)
    } else if (benchmarkName == "SIMLEX-999") {
      parseSIM(sc)
    } else if (benchmarkName == "MEN") {
      parseMEN(sc)
    }
    else {
      //TODO: Throw exception?
      println("Bad benchmark name")
      sc.emptyRDD[((String, String), Double)]
    }
  }

  //Code duplication below, but these are specialized functions anyway

  def parseWS(@transient sc: SparkContext) = {
    // Assuming execution from <repo-root>/code
    val filepath = "data/benchmarks/WS-353/combined.csv"

    val benchInter = sc.textFile(filepath)

    val header = benchInter.first

    benchInter.filter(line => line != header)
      .map(s => s.split(","))
      .map(s => ((s(0).trim, s(1).trim), s(2).trim.toDouble))
  }

  def parseMEN(@transient sc: SparkContext) = {
    // Assuming execution from <repo-root>/code
    val filepath = "data/benchmarks/MEN/MEN_dataset_natural_form_full"

    val benchInter = sc.textFile(filepath)

    benchInter
      .map(s => s.split(" "))
      .map(s => ((s(0).trim, s(1).trim), s(2).trim.toDouble))
  }

  def parseWSRel(@transient sc: SparkContext) = {
    // Assuming execution from <repo-root>/code
    val filepath = "data/benchmarks/WS-353/wordsim_relatedness_goldstandard.txt"

    val benchInter = sc.textFile(filepath)

    benchInter
      .map(s => s.split("\t"))
      .map(s => ((s(0).trim, s(1).trim), s(2).trim.toDouble))
  }

  def parseSIM(@transient sc: SparkContext) = {
    // Assuming execution from <repo-root>/code
    val filepath = "data/benchmarks/SIMLEX-999/SimLex-999.txt"

    val benchInter = sc.textFile(filepath)

    val header = benchInter.first

    benchInter.filter(line => line != header)
      .map(s => s.split("\t"))
      .map(s => ((s(0).trim, s(1).trim), s(3).trim.toDouble))
  }

  def parseMT(@transient sc: SparkContext) = {
    // Assuming execution from <repo-root>/code
    val filepath = "data/benchmarks/MTURK-771/MTURK-771.csv"

    val benchInter = sc.textFile(filepath)

    benchInter
      .map(s => s.split(","))
      .map(s => ((s(0).trim, s(1).trim), s(2).trim.toDouble))
  }

}

