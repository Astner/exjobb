package com.sics.cgraph

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io._

// Todo: Currently duplicates a lot of code - reduce
class GraphBuilder(@transient sc: SparkContext) extends Serializable {

  def pointwiseMutualInfoFromBigrams(bigramsRDD: RDD[((String, String), Long)], subsequent: Boolean, graphFilePrefix: String = "", normalize: Boolean = false) = {
    // Token indices: ("token", "index")
    val indices = bigramsRDD.flatMap{case ((w, v), c) => Iterable(w, v)}.distinct.zipWithIndex

    // Map bigram count tokens to indices
    val adjacencyCounts = bigramsRDD
      .map{case ((w, v), c) => (w, (v, c))}.join(indices)
      .map{case (w, ((v, c), i)) => (v, (i, c))}.join(indices)
      .map{case (v, ((i, c), j)) => ((i, j), c)}

    // ("token index", "count") tuples
    val vertexFirstCounts = adjacencyCounts.map{case ((v, w), c) => (v, c)}.reduceByKey(_ + _)
    val vertexSecondCounts = adjacencyCounts.map{case ((v, w), c) => (w, c)}.reduceByKey(_ + _)

    // Total number of token co-occurrences
    val totalAdjacencyCount = adjacencyCounts.map{case ((v, w), c) => c}.reduce(_ + _).toDouble

    // ("token index", "fraction") tuples Note: Count both sources and sinks so that we do not exclude 
    // vertices that lack outgoing edges
    val vertexFrequencies = adjacencyCounts
      .flatMap{case ((w, v), c) => Iterable((w, c), (v, c))}.reduceByKey(_ + _)
      .map{case (vw, csum) => (vw, csum.toDouble/(2.0 * totalAdjacencyCount))}

    def mutualInformation(firstC: Double, secC: Double, cc: Double): (Double, Double, Double, Double) = {
      val px = firstC / totalAdjacencyCount
      val py = secC /  totalAdjacencyCount
      val pxy = cc / totalAdjacencyCount
      val mi = math.log(pxy / (px * py))
      if(normalize) (px, py, pxy, -mi/math.log(pxy)) else (px, py, pxy, mi)
    }

    // (("focus token index", "context token index"), "probability") tuples
    val interm: RDD[((Long, Long), (Double, Double, Double, Double))] = adjacencyCounts
      .map{case ((v, w), cc) => (v, (w, cc))}
      .join(vertexFirstCounts).map{case (v, ((w, cc), firstC)) => (w, (v, firstC, cc))}
      .join(vertexSecondCounts).map{case (w, ((v, firstC, cc), secC)) => ((v, w), mutualInformation(firstC, secC, cc))}

    if(graphFilePrefix != ""){
      interm.saveAsTextFile(graphFilePrefix + "-mi")
    }

    val edges: RDD[((Long, Long), Double)] = interm.map{case ((v, w), (px, py, pxy, mi)) => ((v, w), mi)}

    // Create vertices RDD
    val vertices: RDD[(Long, String, Double)] =
      indices
      .map{case (s, i) => (i, s)}
      .join(vertexFrequencies)
      .map{case (i, (s, w)) => (i, s, w)}

    (vertices, edges)
  }

  def conditionalProbabilitiesFromBigrams(bigramsRDD: RDD[((String, String), Long)], subsequent: Boolean) = {
    // Token indices: ("token", "index")
    val indices = bigramsRDD.flatMap{case ((w, v), c) => Iterable(w, v)}.distinct.zipWithIndex

    // Map bigram count tokens to indices
    val adjacencyCounts = bigramsRDD
      .map{case ((w, v), c) => (w, (v, c))}.join(indices)
      // Map first token to index
      .map{case (w, ((v, c), i)) => (v, (i, c))}.join(indices)
      // Map second token to index
      .map{case (v, ((i, c), j)) => ((i, j), c)}

    // ("token index", "count") tuples
    val vertexCounts = adjacencyCounts.map{case ((v, w), c) => if(subsequent) (v, c) else (w, c)}.reduceByKey(_ + _)

    // Total number of token occurrences
    val totalVertexCount = vertexCounts.map{case (v, c) => c}.reduce(_ + _).toDouble
    val totalAdjacencyCount = adjacencyCounts.map{case ((i, j), c) => c}.reduce(_ + _).toDouble

    // ("token index", "fraction") tuples
    // Count both sources and sinks so that we do not exclude vertices that lack outgoing edges
    val vertexFrequencies = adjacencyCounts
      .flatMap{case ((w, v), c) => Iterable((w, c), (v, c))}.reduceByKey(_ + _)
      .map{case (vw, csum) => (vw, csum.toDouble/(2.0 * totalAdjacencyCount))}

    // (("focus token index", "context token index"), "probability") tuples
    val edges: RDD[((Long, Long), Double)] = adjacencyCounts
      .map{case ((v, w), c) => if(subsequent) (v, (w, c)) else (w, (v, c))}
      .join(vertexCounts).map{case (v, ((w, c), cc)) => ((v, w), c.toDouble/cc.toDouble)}

    // Create vertices RDD
    val vertices: RDD[(Long, String, Double)] =
      indices
      .map{case (s, i) => (i, s)}
      .join(vertexFrequencies)
      .map{case (i, (s, w)) => (i, s, w)}

    (vertices, edges)
  }

  def pruneByVertexWeight(bigramsRDD: RDD[((String, String), Long)], vtxWRange: (Double, Double)) = {
    val totalAdjacencyCount = bigramsRDD.map{case ((v, w), c) => c}.reduce(_ + _).toDouble
    val vertexFrequencies = bigramsRDD
      .flatMap{case ((v, w), c) => Iterable((v, c), (w, c))}.reduceByKey(_ + _)
      .map{case (vw, csum) => (vw, csum.toDouble/(2.0 * totalAdjacencyCount))}

    bigramsRDD
      .map{case ((v, w), c) => (v, (w, c))}.join(vertexFrequencies)
      .map{case (v, ((w, c), fv)) => (w, (v, c, fv))}.join(vertexFrequencies)
      .filter{case (w, ((v, c, fv), fw)) => 
        fv >= vtxWRange._1 && fv <= vtxWRange._2 &&
        fw >= vtxWRange._1 && fw <= vtxWRange._2}
      .map{case (w, ((v, c, fv), fw)) => ((v, w), c)}
  }

  // Builds a co-ocurrence graph from bigrams RDD and writes it to files
  def adjacentFromBigrams(
    bigramsRDD: RDD[((String, String), Long)], 
    graphFilePrefix: String, 
    subsequent: Boolean, 
    vtxWRange: (Double, Double), 
    minOutDegree: Long,
    correlationMeasure: Int) {

    val prunedBigrams = 
      if(vtxWRange == (Double.MinValue, Double.MaxValue)) bigramsRDD else pruneByVertexWeight(bigramsRDD, vtxWRange)

    val (vertices, edges) = correlationMeasure match {
      case 0 => conditionalProbabilitiesFromBigrams(prunedBigrams, subsequent)
      case 1 => pointwiseMutualInfoFromBigrams(prunedBigrams, subsequent, graphFilePrefix = "", normalize = false)
      case 2 => pointwiseMutualInfoFromBigrams(prunedBigrams, subsequent, graphFilePrefix = "", normalize = true)
    }

    // Write edges and vertices to text file
    edges.saveAsTextFile(graphFilePrefix + "-edges")
    vertices.saveAsTextFile(graphFilePrefix + "-vertices")
  }

  // Builds a co-ocurrence graph from a bigrams file and writes it to files
  def adjacentFromBigramsFile(
    bigramsFile: String, 
    graphFilePrefix: String, 
    subsequent: Boolean, 
    vtxWRange: (Double, Double) = (Double.MinValue, Double.MaxValue), 
    minOutDegree: Long = 0,
    correlationMeasure: Int) = { // 0 => conditional probability, 1 => pointwise mutual information, 2 => normalized pointwise mutual information
    // Assumes a comma separated file and generates (("first token", "second token"), "count") tuples
    // Note: filter(s => s.length == 3) works as basic format checking, should not be necessary if input file correct
    val bigramCounts = sc
      .textFile(bigramsFile)
      .map(s => s.split(",").map(_.trim)).filter(s => s.length == 3)
      .map(s => ((s(1), s(2)), s(0).toLong))

    adjacentFromBigrams(bigramCounts, graphFilePrefix, subsequent, vtxWRange, minOutDegree, correlationMeasure)
  }

}

object GraphBuilder {
  def apply(@transient sc: SparkContext) = new GraphBuilder(sc)
}