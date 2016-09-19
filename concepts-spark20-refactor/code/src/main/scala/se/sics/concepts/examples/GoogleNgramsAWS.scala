package se.sics.concepts.examples

import java.io._

import org.apache.spark._
import org.apache.spark.SparkContext._

import se.sics.concepts.core.{CGraph, CKryoRegistrator, GraphBuilder, RDDBigramParser}

import scala.util.Random


object GoogleNgramsAWS {

  def main(args: Array[String]) {

    // TODO: How to handle args? Prolly simplest to use built-in args() than scopt
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
      .setMaster("local[" + noCores.toString + "]")
      .setAppName("CNCPT GoogleNgramsAWS")
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

    val sc = new SparkContext(conf)

    var start = System.currentTimeMillis / 1000

    // Build bigrams RDD from S3 input
    // TODO: Figure out a way to use textFile with compressed text file. See https://aws.amazon.com/datasets/8172056142375670 for data format
    val bigrams = sc.textFile(inputPath)
      .map(_.split("\t"))
      .filter(a => !a(0).contains("_")) // Remove all lines containing an "_" as they most likely contain POS tag TODO: Improve with re: (_ followed by a number of caps)
      .map(a => (a(0).replaceAll("[^a-zA-Z0-9 ]", " ").toLowerCase.split(" +"), a(2).toLong))
      // At this point we have (ngrams: Array[String], countForYear)
      // Remove singletons
      .filter{case (ar, countForYear) => ar.size > 1}
      // Create word co-oc pairs from defined context, can prolly be done with one flatMap instead
      .map{case (ar, countForYear) => (RDDBigramParser.context(ar, ngramSize), countForYear)}
      // At this point we have (ngrams: Array[Array[String]], countForYear)
      .flatMap{case (arAr, countForYear) => arAr.map(ar => ((ar(0), ar(1)), countForYear))}
      .reduceByKey(_ + _)
      .cache

    val bigramsTime = System.currentTimeMillis / 1000 - start

    var then = System.currentTimeMillis / 1000

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

    val coocTime = System.currentTimeMillis / 1000 - then
    then = System.currentTimeMillis / 1000

    bigrams.unpersist(blocking = false)

    // Save similarity graph and top-10k similarity pairs
    println("Building similarity graph")
    TweetBigrams.simGraphOutput(similarityGraph, path)

    val simTime = System.currentTimeMillis / 1000 - then
    val totalTime = System.currentTimeMillis / 1000 - start

    val setttingsFile = new PrintWriter(new File(path + "-parameters.txt"))
    setttingsFile.write(s"max-degree: ${maxDegree}, ngramSize: ${ngramSize}\n")
    setttingsFile.write(s"vtxWRange: ${vtxRange}, edgWRange: ${edgeRange}\n")
    setttingsFile.write(s"sim-time: ${simTime}, bigrams-time: ${bigramsTime}\n")
    setttingsFile.write(s"cooc-time: ${coocTime}, total-time: ${totalTime}\n")
    setttingsFile.close()

    sc.stop
  }
}
