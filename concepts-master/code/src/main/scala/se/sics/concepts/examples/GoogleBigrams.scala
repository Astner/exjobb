package se.sics.concepts.examples

import java.io._

import org.apache.spark._
import se.sics.concepts.core.{CGraph, CKryoRegistrator, GraphBuilder}

import scala.util.Random


object GoogleBigrams {

  def main(args: Array[String]) {

    val arguments =  TweetBigrams.argParser.parse(args, TweetBigrams.Config())

    val inputPath = arguments.get.inputPath
    val experimentNameOpt = arguments.get.experimentName
    val localDirOpt = arguments.get.localDir
    val noCores = arguments.get.noCores
    val vtxRange = arguments.get.vtxRange
    val edgeRange = arguments.get.edgeRange
    val maxDegree = arguments.get.maxDegree

    // (Randomized) name for the experiment, to avoid overwriting older output files
    val experimentName = {
      if(experimentNameOpt == "") Random.alphanumeric.take(5).mkString
      else experimentNameOpt
    }

    val path = TweetBigrams.createOutputDir(experimentName)

    // Configure and initialize Spark
    val conf = new SparkConf()
      .setMaster("local["+noCores.toString+"]")
      .setAppName("CNCPT GoogleBigrams")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.shuffle.consolidateFiles", "true")// Avoid running out of inodes
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.compress", "true")
      .set("spark.default.parallelism", (noCores*3).toString)
      .set("spark.shuffle.memoryFraction", (0.4).toString)
      .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig
      // .set("spark.storage.memoryFraction", (0.4).toString)


    // local.dir = "/lhome/tvas/tmp"
    if(localDirOpt != "") {
      conf.set("spark.local.dir", localDirOpt)
      conf.set("spark.eventLog.dir", localDirOpt + "/spark-events")
    }


    val sc = new SparkContext(conf)

    val similarityGraph: CGraph = {
      println("Building co-occurrence graph")
      val graphBuilder = GraphBuilder(sc)
      // Builds co-occurrence graph from bigram counts and saves outputfiles
      graphBuilder.adjacentFromBigramsFile(inputPath, path)
      // Map co-occurrence graph to similarity graph
      CGraph(path + "-vertices", path + "-edges", maxDegree, vtxRange, edgeRange, sc)
    }

    // Save similarity graph and top-10k similarity pairs
    println("Building similarity graph")
    TweetBigrams.simGraphOutput(similarityGraph, path)

    val setttingsFile = new PrintWriter(new File(path + "-parameters.txt"))
    setttingsFile.write(s"max-degree: ${maxDegree}\n")
    setttingsFile.write(s"vtxWRange: ${vtxRange}, edgWRange: ${edgeRange}\n")
    setttingsFile.close()

    sc.stop
  }
}
