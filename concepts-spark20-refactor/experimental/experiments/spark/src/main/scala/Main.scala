package com.sics.cgraph

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object CMain {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("CGraph")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.kryoserializer.buffer.max.mb", "1024")
      .set("spark.eventLog.enabled", "false")
      .set("spark.eventLog.compress", "true")
    val sc = new SparkContext(conf)

    val dataPath = "../data/"
    val bigramPath = dataPath + "bigrams/"
    val graphPath = dataPath +  "relation-graphs/" 
    val experimentPath = dataPath + "experiments/"
    val benchmarkPath = dataPath + "benchmarks/"
    
    val wordPairs = "benchmark-word-pairs.txt"
    val wikipediaBigrams = "enwiki-081008-bigrams-min-3.txt"
    val googleBooksBigrams = "google-books-bigrams.txt"
    val lastFMBigrams = "last-fm-artist-bigrams-min-3.txt"
    val relationGraph = "wikipediaPMI54"

    val experiment = Experiments(sc)
    
    experiment.inDegreeVsErrorAndRuntime(
      description = "Test of effect of in-degree threshold",
      inPath = graphPath + relationGraph,
      outPath = experimentPath + relationGraph,
      benchmarkPairsPath = benchmarkPath + wordPairs,
      maxThresholdRange = (100L, 1000L, 100L),
      vtxWRange = (Double.MinValue, Double.MaxValue),
      edgWRange = (Double.MinValue, Double.MaxValue),
      numberOfBins = 1024)

    sc.stop
  }
}