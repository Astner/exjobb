package se.sics.concepts.examples

// # Copy first 10 files from . to ../tweets_sample
// find . -maxdepth 1 -type f |head -10|xargs cp -t ../tweets_sample

import java.io._

import org.apache.spark.SparkContext._
import org.apache.spark._
import se.sics.concepts.core.{RDDBigramParser, CGraph, CKryoRegistrator, GraphBuilder}

import scala.util.Random

/**
 * Parse Tweets dataset, create intermediate correlation graph files and similarity graph
 */
object TweetBigrams {

  case class Config(inputPath: String = "", experimentName: String = "",
                    subsequent: Boolean = true, localDir: String = "", maxDegree: Long = Long.MaxValue,
                    noCores: Int = 4, vtxRange: (Double, Double) = (1.0E-5, 1.0E-3),
                    edgeRange: (Double, Double) = (0.01, 1.0),
                    createIntermediateFiles: Boolean = true, ngramSize: Int = 2)

  def argParser = new scopt.OptionParser[Config]("CNCPT") {
    head("concepts", "")

    opt[String]('p', "input-path") valueName("<path-to-tweets>") required() action { (x, c) =>
      c.copy(inputPath = x)
    } text ("input-path should point to the directory with the input files")

    opt[String]('e', "experiment-name") optional() action { (x, c) =>
      c.copy(experimentName = x)
    } text ("Provide a name for the experiment (optional)")

    opt[Boolean]('s', "subsequent") valueName("<true/false>") optional() action { (x, c) =>
      c.copy(subsequent = x)
    } text ("If true, subsequent token is context, else preceeding token is context (optional)")

    opt[Boolean]('i', "intermediate-files") valueName("<true/false>") optional() action { (x, c) =>
      c.copy(createIntermediateFiles = x)
    } text ("If true, intermediate files for vertices and edges are created (optional)")

    opt[Int]('m', "max-degree") optional() action { (x, c) =>
      c.copy(maxDegree = x)
    } text ("Indicate the maximum number of incoming edges for vertices in the graph (optional)")

    opt[String]('l', "local-dir") valueName("<path-to-tmp>")optional() action { (x, c) =>
      c.copy(localDir = x)
    } text ("Provide a path for where Spark should generate temp files (optional)")

    opt[(Double, Double)]("vtx-limits") optional() keyValueName("<min>", "<max>") action {case ((low, high), c) =>
      c.copy(vtxRange = (low, high))
    } text ("Indicate the upper and lower limits for the number of word appearances. Format: \"low=high\" (optional)")

    opt[(Double, Double)]("edge-limits") optional() keyValueName("<min>", "<max>") action {case ((low, high), c) =>
      c.copy(edgeRange = (low, high))
    } text ("Indicate the upper and lower limits for the edge weights. Format: \"low=high\" (optional)")

    // TODO: Indicate the master here
    opt[Int]('c', "no-cores") optional() action { (x, c) =>
      c.copy(noCores = x)
    } text ("Indicate the number of cores to use in the experiment (optional)")

    opt[Int]('n', "ngram-size") optional() action { (x, c) =>
      c.copy(ngramSize = x)
    } text ("Indicate the ngram window size (optional)")

    help("help")
  }
  // parser.parse returns Option[C]


  def main(args: Array[String]) {

    val arguments =  argParser.parse(args, Config())

    val inputPath = arguments.get.inputPath
    val experimentNameOpt = arguments.get.experimentName
    val localDirOpt = arguments.get.localDir
    val noCores = arguments.get.noCores
    val vtxRange = arguments.get.vtxRange
    val edgeRange = arguments.get.edgeRange
    val maxDegree = arguments.get.maxDegree
    val createIntermediateFiles = arguments.get.createIntermediateFiles
    val ngramSize = arguments.get.ngramSize

    // (Randomized) name for the experiment, to avoid overwriting older output files
    val experimentName = {
      if(experimentNameOpt == "") Random.alphanumeric.take(5).mkString
      else experimentNameOpt
    }

    val path = createOutputDir(experimentName)

    // Configure and initialize Spark
    val conf = new SparkConf()
      .setMaster("local["+noCores.toString+"]")
      .setAppName("CNCPT TweetBigrams")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.compress", "true")
      .set("spark.default.parallelism", (noCores*3).toString)
      .set("spark.shuffle.memoryFraction", (0.4).toString)
      .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig
      // .set("spark.storage.memoryFraction", (0.4).toString)


    // local.dir = "/lhome/tvas/tmp"
    if(localDirOpt != "") {
      // Avoid running out of inodes
      conf.set("spark.local.dir", localDirOpt)
      conf.set("spark.eventLog.dir", localDirOpt + "/spark-events")
    }


    val sc = new SparkContext(conf)

    val files = sc.textFile(inputPath)
    val only_text = files.filter(_.startsWith("Text:")).map(_.drop(6).trim) // Drops hashtags, links and mentions
    val complete_tweets = files.filter(_.startsWith("Origin:")).map(_.drop(8)) // Original tweet

    // Choose input
    val source = only_text
    println("Parsing input")
    // TODO: No need for count here, just for testing
    // val noTweets = source.count
    // println(s"Number of tweets: ${noTweets}")


    // Parse source
    val parser = RDDBigramParser(source, windowSize = ngramSize)

    //Create bigram output file
    // parser.pairCounts.saveAsTextFile(path + "-bigrams")
    val bigrams = parser.pairCounts.cache


    val similarityGraph: CGraph = {
      val graphBuilder = GraphBuilder(sc)
      println("Building co-occurrence graph")
      if (createIntermediateFiles) {
        // Builds co-occurrence graph from bigram counts and saves outputfiles
        graphBuilder.adjacentFromBigrams(bigrams, path)
        // Map co-occurrence graph to similarity graph
        CGraph(path + "-vertices", path + "-edges", maxDegree, vtxRange, edgeRange, sc)
      }
      else {
        // Builds co-occurrence graphs and returns edges and vertex RDDs
        val (vertices, edges) = graphBuilder.vtxEdgesFromBigram(bigrams)
        CGraph(path + "-vertices", path + "-edges", maxDegree, vtxRange, edgeRange, sc)
      }

    }

    bigrams.unpersist(blocking=false)
    // Save similarity graph and top-10k similarity pairs
    println("Building similarity graph")
    simGraphOutput(similarityGraph, path)

    val settingsFile = new PrintWriter(new File(path + "-parameters.txt"))
    // settingsFile.write(s"uniqueWordsPruned: ${similarityGraph.prunedVertices.count}\n") // noTweets: ${noTweets},
    settingsFile.write(s"max-degree: ${maxDegree}\n")
    settingsFile.write(s"vtxWRange: ${vtxRange}, edgWRange: ${edgeRange}\n")
    settingsFile.close()

    sc.stop
  }

  // TODO(tvas): Move the following functions to a utils object
  // Easy way to print to file. Source: http://stackoverflow.com/a/4608061/209882
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def simGraphOutput(similarityGraph: CGraph, path: String) = {

    val simEdges = similarityGraph.similarityEdgesWithLabels
    simEdges.saveAsTextFile(path + "-string-sim-edges")


    println("Saving sorted similarity pairs")
    val reversedPairs = simEdges.map(pair => (pair._2, pair._1))
    val simsRanked = reversedPairs.sortByKey(ascending = false)
    val topSims = simsRanked.take(10000)

    val filename = path + "-simsRanked.txt"

    printToFile(new File(filename))(p => {
      topSims.foreach(p.println)
    })
  }

  def createOutputDir(experimentName: String) = {
    import java.io.File
    val path: String = "output/" + experimentName
    val fpath = new File(path)
    if (!fpath.exists()) {
      val ok = fpath.mkdirs
      if(ok) path + "/" + experimentName
      else {
        println("Could not create directory, saving to output/")
        path
      }
    }
    else path + "/" + experimentName
  }


}
