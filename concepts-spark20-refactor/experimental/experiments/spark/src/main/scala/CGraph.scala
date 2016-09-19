package com.sics.cgraph

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import scala.util.Random
import play.api.libs.json._
import java.io._
import scala.math._

class CGraph(
    vertices:  RDD[(Long, String, Double)],
    edges: RDD[((Long, Long), Double)],
    inDegreeThreshold: Long,
    vtxWRange: (Double, Double),
    edgWRange: (Double, Double),
    @transient sc: SparkContext) extends Serializable {

  // Persistence type (see http://spark.apache.org/docs/1.2.0/programming-guide.html#rdd-persistence)
  val storageLevel = storage.StorageLevel.MEMORY_AND_DISK

  // Cache unpruned graph
  edges.persist(storageLevel)
  vertices.persist(storageLevel)

  // Discard vertices
  val prunedVertices = vertices.filter{case (i, s, wi) => wi >= vtxWRange._1 && wi <= vtxWRange._2}
  prunedVertices.persist(storageLevel)

  // Key-value pairs from indices to weights
  val vertexIndexWeights = vertices.map{case (i, s, wi) => (i, wi)}
  vertexIndexWeights.persist(storageLevel)

  val edgeWeightTuples = edges
    .map{case ((i, j), wij) => (i, (j, wij))}.join(vertexIndexWeights)
    .map{case (i, ((j, wij), wi)) => (j, ((i, wi), wij))}.join(vertexIndexWeights)
    .map{case (j, (((i, wi), wij), wj)) => (i, j, wi, wj, wij)}
  edgeWeightTuples.persist(storageLevel)

  val prunedEdges = {
    // Top in-edges w r t weight
    def edgesToKeep(indexAndEdges: (Long, Iterable[(Long, Double)])) = {
      val j = indexAndEdges._1
      indexAndEdges._2
        // Sort in reverse order
        .toArray.sortBy(e => -math.abs(e._2))
        // Keep top edges
        .take(inDegreeThreshold.toInt)
        // Set j as sink
        .map{case (i, w) => ((i, j), w)}
    }

    edgeWeightTuples
    // Keep edges with valid vertices
    .filter{case (i, j, wi, wj, wij) =>
      wi >= vtxWRange._1 && wi <= vtxWRange._2 &&
      wj >= vtxWRange._1 && wj <= vtxWRange._2 &&
      // Note: prune with respect to edge weight as well
      math.abs(wij) >= edgWRange._1 && math.abs(wij) <= edgWRange._2}
    // Collect incoming edges per vertex
    // Note: Here we exlude vertices without incoming edges
    .map{case (i, j, wi, wj, wij) => (j, (i, wij))}.groupByKey
    // Bound in-degree
    .flatMap(edgesToKeep(_))
  }
  prunedEdges.persist(storageLevel)

  val remainingEdgeWeightSums: RDD[(Long, Double)] = prunedEdges
    .map{case ((i, j), wij) => (i, math.abs(wij))}.reduceByKey(_+_)
  remainingEdgeWeightSums.persist(storageLevel)

  val discardedEdgeWeightSums: RDD[(Long, Double)] = edges
    // Keep edges that have valid source vertices
    .map{case ((i, j), wij) => (i, (j, wij))}.join(vertexIndexWeights)
    .filter{case (i, ((j, wij), wi)) => wi >= vtxWRange._1 && wi <= vtxWRange._2}
    // Calculate weight sums of all edges (prior to pruning)
    .map{case (i, ((j, wij), wi)) => (i, math.abs(wij))}.reduceByKey(_+_)
    // Subtract weight sums of kept edges
    .leftOuterJoin(remainingEdgeWeightSums)
    // Note
    .map{case (i, (all, remaining)) => (i, all - remaining.getOrElse(0.0))}
  discardedEdgeWeightSums.persist(storageLevel)

  lazy val inDegrees: RDD[(Long, Int)] = edges.map{case ((i, j), wij) => (j, 1)}.reduceByKey(_+_)
  lazy val outDegrees: RDD[(Long, Int)] = edges.map{case ((i, j), wij) => (i, 1)}.reduceByKey(_+_)
  lazy val prunedInDegrees: RDD[(Long, Int)] = prunedEdges.map{case ((i, j), wij) => (j, 1)}.reduceByKey(_+_)
  lazy val prunedOutDegrees: RDD[(Long, Int)] = prunedEdges.map{case ((i, j), wij) => (i, 1)}.reduceByKey(_+_)

  // (i, j, sij, ri, rj, di, dj}
  lazy val sharedTermsWithSums: RDD[(Long, Long, Double, Double, Double, Double, Double)] = {
      // Collect incoming edges per vertex
    val inEdgesPerVertex = prunedEdges
      .map{case ((i, j), w) => (j, (i, w))}
      // TODO: Perhaps the number of partitions should be proportional to the number of vertices.
      // We see that for maxDegree 1k we get better performance with 10k partitions than with 1k
      // For maxDegree 600 however performance is better with 1k partitions vs. 10k.
      //.partitionBy(new HashPartitioner(1000))
    inEdgesPerVertex.persist(storageLevel)
    
    // Get all pairs of incoming edges through self-join
    val edgePairs = inEdgesPerVertex.join(inEdgesPerVertex)
      // Discard duplicates and self-referentials
      .filter{case (k, ((i, wi), (j, wj))) => i < j}

    // Contribution to L1 norm where i and j share neighbours
    val sharedTerms = edgePairs
      .map{case (k, ((i, wi), (j, wj))) => ((i, j), math.abs(wi - wj) - math.abs(wi) - math.abs(wj))}
      .reduceByKey(_+_)

    // Join with sums of kept and discarded weights per vertex
    val joinedWithSums = sharedTerms
      // First add i's sums
      .map{case ((i, j), sij) => (i, (j, sij))}
      // Note: Uses leftOuterJoin instead of join since otherwise we may loose vertices that
      // do not have vertex weights below threshold
      .join(remainingEdgeWeightSums).leftOuterJoin(discardedEdgeWeightSums)
      // Add j's sums (ri = "remaining weight sum of i", di = "discarded weight sum of i")
      .map{case (i, (((j, sij), ri), di)) => (j, (i, sij, ri, di.getOrElse(0.0)))}
      .join(remainingEdgeWeightSums).leftOuterJoin(discardedEdgeWeightSums)
      // Reformat and fetch optional sums in dj
      .map{case (j, (((i, sij, ri, di), rj), dj)) => (i, j, sij, ri, rj, di, dj.getOrElse(0.0))}

    inEdgesPerVertex.unpersist()
    joinedWithSums
  }
  
  // (("vertex i", "vertex j"), "max relative L1 norm", "max error")
  lazy val relativeL1NormWithErrorBound: RDD[((Long, Long), Double, Double)] = {
    // Sum terms and normalize with respect to maximum possible L1 norm
    // The maximum possible error is given by the sum of discarded edge weighs of i and j
    // Note: We now also incorporate edges from discarded vertices
    sharedTermsWithSums
      .map{case (i, j, sij, ri, rj, di, dj) => {
        val tot = ri + rj + di + dj
        ((i, j), (sij + tot)/tot, (di + dj)/tot)
        }
      }
  }
  relativeL1NormWithErrorBound.persist(storageLevel)

  // Similarities, where S(i, j) is one subtracted by the relative L1 norm
  // Todo: incorporate error bound here as well
  lazy val simEdges: RDD[((Long, Long), Double)] =
    relativeL1NormWithErrorBound.map{case ((i, j), relL1, maxError) => ((i, j), 1.0 - relL1)}

  lazy val directedSimilarityEdges = simEdges.flatMap{case ((i, j), w) =>  Iterable(((i, j), w), ((j, i), w))}

  // Todo: Duplicates some code here - implement separate index to label function instead
  lazy val relativeL1NormWithLabels: RDD[((String, String), Double, Double)] = {
    // Key-value pairs from indices to strings
    val vertexIndexStrings = prunedVertices.map{case (i, si, wi) => (i, si)}
    // Map indices to labels
    relativeL1NormWithErrorBound
      .map{case ((i, j), wij, d) => (i, (j, wij, d))}
      .join(vertexIndexStrings)
      .map{case (i, ((j, wij, d), si)) => (j, (si, wij, d))}
      .join(vertexIndexStrings)
      .map{case (j, ((si, wij, d), sj)) => ((si, sj), wij, d)}
  }

  // Key-value pairs from indices to strings
  lazy val vertexIndexStrings = prunedVertices.map { case (i, si, wi) => (i, si) }

  // Similarities with labels
  lazy val similarityEdgesWithLabels: RDD[((String, String), Double)] = {
    // Map indices to labels
    simEdges
      .map { case ((i, j), wij) => (i, (j, wij)) }
      .join(vertexIndexStrings)
      .map { case (i, ((j, wij), si)) => (j, (si, wij)) }
      .join(vertexIndexStrings)
      .map { case (j, ((si, wij), sj)) => ((si, sj), wij) }
  }

  /** Test of community detection using GraphX' label propagation algorithm
    * Note: Current GraphX implementation does not take edge weights into consideration
    */
  def clustersBySimilarityGraphX(numberOfIterations: Int): RDD[(Long, Long)] = {
    val edges: RDD[Edge[Double]] = directedSimilarityEdges.map{case ((i, j), w) => Edge(i, j, w)}
    val similarityGraph: Graph[String, Double] = Graph(vertexIndexStrings, edges)
    LabelPropagation.run(similarityGraph, numberOfIterations).vertices
  }

  /** Label propagation clustering */
  def clustersBySimilarity(numberOfIterations: Int, useWeights: Boolean, saturation: Double = 1.0): RDD[(Long, Long)] = {
    // Sigmoid for saturating edge weights. Assumes input in [0, 1]
    // Used to counteract that all vertices ends up in one cluster
    // s gives the strength of saturation (s = 1 => none)
    def saturate(w: Double, s: Double): Double = {
      val p = pow(w, s)
      val q = pow(1.0 - w, s)
      p / (p + q) 
    }

    // Initialize vertices as singleton clusters and saturate weights
    var newCommunities = directedSimilarityEdges.map{case ((i, j), w) => ((i, i), (j, j), saturate(w, saturation))}
    newCommunities.persist(storageLevel)

    for(iterator <- 0 until numberOfIterations){
      val previousCommunities = newCommunities
      previousCommunities.persist(storageLevel)
      newCommunities.unpersist()

      val communityAssignments = previousCommunities
        // Sum up weights or number of instances per community
        .map{case ((i, ic), (j, jc), w) => if(useWeights) ((j, ic), w) else ((j, ic), 1.0)}
        .reduceByKey(_+_)
        // Get communities with largest weight sums. If same sum, pick one at random
        // Note that the ordering of the reduction effects the result
        .map{case ((j, ic), ws) => (j, (ic, ws))}
        .reduceByKey{case ((ic1, ws1), (ic2, ws2)) =>
            if(ws1 > ws2 || (ws1 == ws2 && Random.nextBoolean)) (ic1, ws1) else (ic2, ws2)}
        // Discard weight sums
        .map{case (j, (ic, ws)) => (j, ic)}
      communityAssignments.persist(storageLevel)

      // Set new community assignments
      newCommunities = previousCommunities
        .map{case ((i, ic), (j, jc), w) => (i, (j, w))}.join(communityAssignments)
        .map{case (i, ((j, w), ic)) => (j, (i, ic, w))}.join(communityAssignments)
        .map{case (j, ((i, ic, w), jc)) => ((i, ic), (j, jc), w)}
      newCommunities.persist(storageLevel)

      communityAssignments.unpersist()
      previousCommunities.unpersist()
    }

    // Discard duplicates and return ("vertex id", "community id") tuples    
    val result = newCommunities.map{case ((i, ic), (j, jc), w) => (j, jc)}.distinct
    newCommunities.unpersist()
    result
  }

  /** Exports similarities to JSON for D3 visualization
    * Note: Edges are weighted with 1 - L, where L is the relative L_1 norm
    * Edges with weights below weightThreshold are discarded
    * Vertices without edges are discarded
    */
  def exportSimilarityGraphToJSON(fileName: String, weightThreshold: Double, clusterAssignments: RDD[(Long, Long)]): Unit = {
    // Discard pairs with similarity below threshold
    val prunedSimilarityEdges = relativeL1NormWithErrorBound
      .filter{case ((iOld, jOld), n, e) => 1.0 - n > weightThreshold}
      .map{case ((iOld, jOld), n, e) => (iOld, (jOld, n))}
    prunedSimilarityEdges.persist(storageLevel)

    val prunedSimilarityVertices = prunedSimilarityEdges
      // Collect remaining vertex indices
      .flatMap{case (iOld, (jOld, n)) => Iterable(iOld, jOld)}.distinct
      // Join with labels (note: possible to skip tmp string here?)
      .map((_, "")).join(vertexIndexStrings).map{case (iOld, (tmp, s)) => (iOld, s)}
      // Add cluster assignments
      .join(clusterAssignments)
      // Re-index and sort (note: possibly already sorted)
      .zipWithIndex.map{case ((iOld, (s, c)), iNew) => (iNew, (iOld, s, c))}.sortBy(_._1)
    prunedSimilarityVertices.persist(storageLevel)

    val oldToNewIndices = prunedSimilarityVertices.map{case (iNew, (iOld, s, c)) => (iOld, iNew)}
    oldToNewIndices.persist(storageLevel)

    // Map to new indices
    val indexEdges = prunedSimilarityEdges.join(oldToNewIndices)
      .map{case (iOld, ((jOld, n), iNew)) => (jOld, (iNew, n))}.join(oldToNewIndices)
      // Set similarity as one subtracted by relative L1 norm
      .map{case (jOld, ((iNew, n), jNew)) => (iNew, jNew, 1.0 - n)}

    // Convert to JSON
    val jsonVertices = prunedSimilarityVertices
      .map{case (iNew, (iOld, s, c)) => JsObject(Seq("name" -> JsString(s), "group" -> JsNumber(c.toInt)))}.collect
    val jsonEdges = indexEdges
      .map{case (i, j, n) => JsObject(Seq("source" -> JsNumber(i), "target" -> JsNumber(j), "value" -> JsNumber(n)))}.collect
    val jsonGraph = JsObject(Seq("nodes" -> JsArray(jsonVertices), "links" -> JsArray(jsonEdges)))

    prunedSimilarityEdges.unpersist()
    prunedSimilarityVertices.unpersist()
    oldToNewIndices.unpersist()

    // Explicitly set encoding
    val file = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(fileName)), "UTF-8"), false)
    file.write(Json.prettyPrint(jsonGraph))
    file.close() 
  }

  def printClusters(clusterAssignments: RDD[(Long, Long)]): Unit = {
    clusterAssignments
    // Map from indices to labels
    .join(vertexIndexStrings).map{case (i, (ic, l)) => (ic, l)}
    // Group by cluster assignment and discard singleton clusters
    .groupByKey.filter(_._2.size > 1)
    .collect.foreach(println(_))
  }

  def printInfo: Unit = {
    println("\nedges:")
    edges.collect.foreach(println)
    println("\nvertices:")
    vertices.collect.foreach(println)
    println("\nprunedEdges:")
    prunedEdges.collect.foreach(println)
    println("\nprunedVertices:")
    prunedVertices.collect.foreach(println)
  }

  /** Writes vertex statistics to a comma separated file */
  def exportVertexStatistics(fileName: String): Unit = {
    val file = new PrintWriter(new File(fileName))
    file.write(s"Label, index, weight, in-degree, out-degree, pruned in-degree, pruned out-degree, sum of remaining edges, sum of discarded edges\n")
    vertices.map{case (i, s, wi) => (i, (s, wi))}
      .leftOuterJoin(inDegrees)
      .leftOuterJoin(outDegrees)
      .leftOuterJoin(prunedInDegrees)
      .leftOuterJoin(prunedOutDegrees)
      .leftOuterJoin(remainingEdgeWeightSums)
      .leftOuterJoin(discardedEdgeWeightSums)
      .collect.foreach{case (i, (((((((s, w), id), od), pid), pod), rews), dews)) => 
        file.write(s"${s},${i},${w},${id.getOrElse(0)},${od.getOrElse(0)},${pid.getOrElse(0)},${pod.getOrElse(0)},${rews.getOrElse(0.0)},${dews.getOrElse(0.0)}\n")}
    file.close()
  }

  /** Writes edge statistics to a comma separated file */
  def exportEdgeStatistics(fileName: String): Unit = {
    val file = new PrintWriter(new File(fileName))
    file.write(s"Weight\n")
    edges.map{case ((i, j), wij) => wij}.collect.foreach(wij => file.write(s"${wij}\n"))
    file.close()
  }

  def unpersistAll = {
    edges.unpersist()
    vertices.unpersist()
    prunedEdges.unpersist()
    prunedVertices.unpersist()
    edgeWeightTuples.unpersist()
    discardedEdgeWeightSums.unpersist()
    remainingEdgeWeightSums.unpersist()
    vertexIndexWeights.unpersist()
    relativeL1NormWithErrorBound.unpersist()
  }
}

object CGraph {

  def readVtxFile(@transient sc: SparkContext, vtxFile: String) = {
    sc.textFile(vtxFile).map(s => s.replaceAll("\\(|\\)","").split(",")).map(s => (s(0).trim.toLong, s(1).trim, s(2).trim.toDouble))
  }

  def readEdgeFile(@transient sc: SparkContext, edgFile: String) = {
    sc.textFile(edgFile).map(s => s.replaceAll("\\(|\\)","").split(",")).map(s => ((s(0).trim.toLong, s(1).trim.toLong), s(2).trim.toDouble))
  }

  def apply(
    vtxFile: String, edgFile: String,
    vtxWRange: (Double, Double), edgWRange: (Double, Double),
    @transient sc: SparkContext) = new CGraph(readVtxFile(sc, vtxFile), readEdgeFile(sc, edgFile), Long.MaxValue, vtxWRange, edgWRange, sc)

  def apply(
    vertices:  RDD[(Long, String, Double)], edges: RDD[((Long, Long), Double)],
    vtxWRange: (Double, Double), edgWRange: (Double, Double),
    @transient sc: SparkContext) = new CGraph(vertices, edges, Long.MaxValue, vtxWRange, edgWRange, sc)

  def apply(
    vtxFile: String, edgFile: String,
    maxDegree: Long,
    vtxWRange: (Double, Double), edgWRange: (Double, Double),
    @transient sc: SparkContext) = new CGraph(readVtxFile(sc, vtxFile), readEdgeFile(sc, edgFile), maxDegree, vtxWRange, edgWRange, sc)

  def apply(
    vertices:  RDD[(Long, String, Double)], edges: RDD[((Long, Long), Double)],
    maxDegree: Long,
    vtxWRange: (Double, Double), edgWRange: (Double, Double),
    @transient sc: SparkContext) = new CGraph(vertices, edges, maxDegree, vtxWRange, edgWRange, sc)

}
