package com.sics.cgraph

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import play.api.libs.json._
import scala.io.Source
import java.io._
import Statistics._

/** Class for performing various experiments */
class Experiments(@transient sc: SparkContext) extends Serializable {

  // Set experiment id as current timestamp in seconds
  val id: Long = System.currentTimeMillis/1000

  /** Evaluates how different properties depend on the in-degree threshold.
    * Stores parameter settings and results in JSON file.
    */
  def inDegreeVsErrorAndRuntime(
    // Short description of experiment
    description: String,
    // Path to prefix of relation graph
    inPath: String,
    // Path to folder where to store results
    outPath: String,
    // Path to file with concept pairs that occur in benchmarks                 
    benchmarkPairsPath: String,
    // Min, max and delta of thresholds used       
    maxThresholdRange: (Long, Long, Long),
    // Vertex weight range
    vtxWRange: (Double, Double),
    // Edge weight range
    edgWRange: (Double, Double),
    // Number of bins in histograms
    numberOfBins: Int = 1000): Unit = {

    val experimentStartTime = System.currentTimeMillis()
    val storageLevel = storage.StorageLevel.MEMORY_AND_DISK
    val (dMin, dMax, dDelta) = maxThresholdRange
    val maxInDegrees: List[Long] = List.range(dMin, dMax + dDelta, dDelta)
  
    // Concept pairs that occur in benchmarks
    val benchmarkIndexPairs = {
      val conceptPairs = sc.textFile(benchmarkPairsPath).map(_.split(",")).map(p => (p(0), p(1)))
      val stringToIndex = CGraph.readVtxFile(sc, inPath + "-vertices").map{case (i, si, wi) => (si, i)}
      stringToIndex.persist(storageLevel)
      // Associate concept pairs with vertex indices
      val indexed = conceptPairs
        .join(stringToIndex).map{case (si, (sj, i)) => (sj, (si, i))}
        .join(stringToIndex).map{case (sj, ((si, i), j)) => if(i < j) ((i, j), (si, sj)) else ((j, i), (sj, si))}
      stringToIndex.unpersist()
      indexed
    }
    benchmarkIndexPairs.persist(storageLevel)

    val parameters = JsObject(Seq(
      "source" -> JsString(inPath),
      "number of bins" -> JsNumber(numberOfBins),
      "in-degree threshold range" -> JsArray(Seq(JsNumber(dMin), JsNumber(dMax), JsNumber(dDelta))),
      "vertex weight range" -> JsArray(Seq(JsNumber(vtxWRange._1), JsNumber(vtxWRange._2))),
      "edge weight range" -> JsArray(Seq(JsNumber(edgWRange._1), JsNumber(edgWRange._2)))))

    def result(maxInDegree: Long): JsObject = {
      val prefix = outPath + id.toString + "-" + maxInDegree.toString
      val graph = CGraph(inPath + "-vertices", inPath + "-edges", maxInDegree, vtxWRange, edgWRange, sc)
      // Enforce evaluation so that we get the timing right   
      graph.prunedEdges.count
      val startTime = System.currentTimeMillis()
      val normsAndErrors = graph.relativeL1NormWithErrorBound
      normsAndErrors.persist(storageLevel)
      val numberOfRelations = normsAndErrors.count
      val runtime = (System.currentTimeMillis() - startTime).toFloat/1000.0
      val normStats = JSONStatistics(normsAndErrors.map(_._2), numberOfBins)
      val errorStats = JSONStatistics(normsAndErrors.map(_._3), numberOfBins)
      val numberOfVertices = graph.prunedVertices.count
      val numberOfEdges = graph.prunedEdges.count

      val selectedRelations = normsAndErrors
        .map{case ((i, j), l, e) => if(i < j) ((i, j), (l, e)) else ((j, i), (l, e))}
        .join(benchmarkIndexPairs)
        .map{case ((i, j), ((l, e), (si, sj))) => JsObject(Seq(
          "pair" -> JsString(si + "," + sj),
          "relative l1 norm" -> JsNumber(l),
          "error bound" -> JsNumber(e)))}
        .collect

      val runInfo = JsObject(Seq(
          "max in-degree" -> JsNumber(maxInDegree),
          "runtime" -> JsNumber(runtime),
          "number of vertices" -> JsNumber(numberOfVertices),
          "number of edges" -> JsNumber(numberOfEdges),
          "number of relations" -> JsNumber(numberOfRelations),
          "relative l1 norm" -> normStats,
          "max error" -> errorStats,
          "selected concept relations" -> JsArray(selectedRelations))
      )

      // Enforce evaluation so that we can unpersist (otherwise stats calculated twice)
      val eval = runInfo.fields
      normsAndErrors.unpersist()
      graph.unpersistAll

      runInfo
    }

    val output = JsObject(Seq(
        "id" -> JsNumber(id),
        "date" -> JsString((new java.util.Date).toString),
        "description" -> JsString(description),
        "parameters" -> parameters,
        "results" -> JsArray(maxInDegrees.map(result(_))),
        "total runtime" -> JsNumber((System.currentTimeMillis() - experimentStartTime).toFloat/1000.0)))

    val file = new PrintWriter(new File(outPath + "-" + id.toString + ".json"))
    file.write(Json.prettyPrint(output))
    file.close()

    benchmarkIndexPairs.unpersist()
  }

  /** Writes aggregate sizes for different saturation exponents to CSV file
    * for further post-processing (e.g. using R or Python) to determine how
    * robust the clustering algorithm is with regard to the saturation parameter.
    */
  def clusterSizesPerSaturation(
    graph: CGraph, 
    numberOfIterations: Int, 
    minSaturation: Int, 
    maxSaturation: Int,
    outPath: String): Unit = {

    val saturationExponents = List.range(minSaturation, maxSaturation + 1).map(_.toDouble)

    def aggregateSizes(s: Double) =
      graph.clustersBySimilarity(numberOfIterations, true, s)
      .map{case (k, v) => (v, k)}.groupByKey
      .map{case (k, iter) => (s, iter.size)}
      .collect
 
    val file = new PrintWriter(new File(outPath + "aggregate-sizes-" + id.toString + ".txt"))
    saturationExponents.foreach(s => aggregateSizes(s).foreach{case (s, size) => file.write(s"${s},${size}\n")})
    file.close() 
  }
}

object Experiments {
  def apply(@transient sc: SparkContext) = new Experiments(sc)
}





