package se.sics.concepts.examples

import org.apache.spark._
import se.sics.concepts.core.{Analysis, CGraph, CKryoRegistrator}

object CMain {
  def main(args: Array[String]) {

    // Configure and initialize Spark
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("CGraph")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.kryoserializer.buffer.max.mb", "1024")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.compress", "true")
    val sc = new SparkContext(conf)

    // File paths
    val bigramFile = "data/enwiki-081008-bigrams-min-3.txt"
    val graphFilePrefix = "data/enwiki-081008-subsequent"

    // Allowed ranges of edge and vertex weights
    val vtxWRange = (0.0, 1.0)
    val edgWRange = (0.0, 1.0)

    // Maximum in-degree
    val maxDegree: Long = 1000

    def exportVertexStat = {
      val analysis = Analysis(sc)
      val graph = CGraph(graphFilePrefix + "-vertices.txt", graphFilePrefix + "-edges.txt", maxDegree, vtxWRange, edgWRange, sc)
      analysis.exportVertexStatistics(graph, "output/vertex-stat-wiki-max-in-degree-1000.txt")
    }

    exportVertexStat

    sc.stop
  }
}