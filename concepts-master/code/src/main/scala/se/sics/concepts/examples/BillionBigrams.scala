package se.sics.concepts.examples

import java.io._

import org.apache.spark._
import se.sics.concepts.core.{CGraph, CKryoRegistrator, GraphBuilder, RDDBigramParser}

import scala.util.Random

object BillionBigrams {

  def main(args: Array[String]) {

    val arguments =  TweetBigrams.argParser.parse(args, TweetBigrams.Config())

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

    val path = TweetBigrams.createOutputDir(experimentName)

    // Configure and initialize Spark
    val conf = new SparkConf()
//      .setMaster("local["+noCores.toString+"]")
      .setAppName("CNCPT BillionBigrams")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.shuffle.consolidateFiles", "true") // Avoid running out of inodes
//      .set("spark.eventLog.enabled", "true")
//      .set("spark.eventLog.compress", "true")
//      .set("spark.default.parallelism", (noCores*3).toString)
//      .set("spark.shuffle.memoryFraction", (0.4).toString)
      .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig
      // .set("spark.storage.memoryFraction", (0.4).toString)


    // local.dir = "/lhome/tvas/tmp"
//    if(localDirOpt != "") {
//      conf.set("spark.local.dir", localDirOpt)
//      conf.set("spark.eventLog.dir", localDirOpt + "/spark-events")
//    }


    val sc = new SparkContext(conf)

    val files = sc.textFile(inputPath)


    // Choose input
    val source = files
    println("Parsing input")
    // TODO: No need for count here, just for testing
    // val noTweets = source.count
    // println(s"Number of tweets: ${noTweets}")



    // Parse source
    // TODO: splitSentences depends on the source. For the 1b set sentences have already been split,
    // but if using the adjusted tweets file, splitSentences should be set to true
    val parser = RDDBigramParser(source, splitSentences = false, windowSize = ngramSize)

    //Create bigram output file
    // parser.pairCounts.saveAsTextFile(path + "-bigrams")
    val bigrams = parser.pairCounts.cache().setName("bigrams")


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
        CGraph(vertices, edges, maxDegree, vtxRange, edgeRange, sc)
      }

    }

    bigrams.unpersist(blocking=false)
    // Save similarity graph and top-10k similarity pairs
    println("Building similarity graph")
    val simEdges = similarityGraph.simEdges.setName("simEdges")
    simEdges.saveAsTextFile(new File(path, "id-sim-edges").toString)

    val settingsFile = new PrintWriter(new File(path + "-parameters.txt"))
    // settingsFile.write(s"uniqueWordsPruned: ${similarityGraph.prunedVertices.count}\n") // noTweets: ${noTweets},
    settingsFile.write(s"max-degree: $maxDegree\n")
    settingsFile.write(s"ngramSize: $ngramSize\n")
    settingsFile.write(s"vtxWRange: $vtxRange, edgWRange: $edgeRange\n")
    settingsFile.close()

    sc.stop()
  }
}
