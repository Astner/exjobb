/*
 Copyright (C) 2015 Daniel Gillblad, Olof Görnerup , Theodoros Vasiloudis (dgi@sics.se,
 olofg@sics.se, tvas@sics.se).

 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package se.sics.concepts.core

import java.io._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import play.api.libs.json._
import se.sics.concepts.core.ClusteringParameters._
import se.sics.concepts.core.ConceptIDs._

import scala.collection.immutable.{Queue, TreeSet}
import scala.math._
import scala.util.Random

abstract class ClusteringAlgorithm extends WithParameters {
  def cluster(
      edges: RDD[(ConceptID, ConceptID, Weight)],
      verticesOption: Option[RDD[Long]] = None,
      runParameters: ParameterMap = ParameterMap()): RDD[(ConceptID, ClusterID)]
}

object ClusteringAlgorithm {
  /**
   * Exports similarities to JSON for D3 visualization and Gephi CSV format
   * Vertices without edges are discarded.
   * TODO: Add vertices RDD as Option argument, to avoid recomputation
   */
  def exportClusteredSimilarityGraph(
      filteredSimilarityGraph: RDD[((Long, Long), Double)],
      filteredVertices: RDD[(Long)],
      vertexIndexStrings: RDD[(Long, String)],
      clusterAssignments: RDD[(Long, Long)],
      weightThreshold: Double,
      outputPath: File,
      timestamp: String)
    : Unit = {

    val prunedSimilarityEdges = filteredSimilarityGraph
      .map{case ((iOld, jOld), w) => (iOld, (jOld, w))}
      .cache()
      .setName("prunedSimilarityEdges")

    // TODO: Have filteredVertices as Option with pattern matching, like we do in the cluster() function
    val prunedSimilarityVertices: RDD[(Long, (Long, String, Long))] = filteredVertices
      // Join with labels (note: possible to skip tmp string here?)
      .map((_, 1)).join(vertexIndexStrings).map{case (iOld, (tmp, s)) => (iOld, s)}
      // Add cluster assignments
      .join(clusterAssignments)
      // Re-index and sort (note: possibly already sorted)
      .zipWithIndex().map{case ((iOld, (s, c)), iNew) => (iNew, (iOld, s, c))}.sortBy(_._1)
      .cache().setName("prunedSimilarityVertices")

    // Export vertices with cluster and label information as csv
    prunedSimilarityVertices
      .map{case (iNew, (iOld, s, c)) => s"$iNew,$s,$c"}
      .saveAsTextFile(new File(outputPath, "cluster-vertices-" + timestamp).toString)

    val oldToNewIndices = prunedSimilarityVertices.map{case (iNew, (iOld, s, c)) => (iOld, iNew)}

    // Map to new indices
    val indexEdges: RDD[(Long, Long, Double)] = prunedSimilarityEdges.join(oldToNewIndices)
      .map{case (iOld, ((jOld, n), iNew)) => (jOld, (iNew, n))}.join(oldToNewIndices)
      // Clean columns
      .map{case (jOld, ((iNew, n), jNew)) => (iNew, jNew, n)}
      .cache().setName("indexEdges")

    // Export edges with weight information (although Gephi ignores the weight during CSV import currently)
    indexEdges
      .map{case (iNew, jNew, w) => s"$iNew,$jNew,$w"}
      .saveAsTextFile(new File(outputPath, "cluster-edges-" + timestamp).toString)

    // Convert to JSON
    val jsonVertices = prunedSimilarityVertices
      .map{case (iNew, (iOld, s, c)) => JsObject(Seq("name" -> JsString(s), "group" -> JsNumber(c.toInt)))}.collect()
    val jsonEdges = indexEdges
      .map{case (i, j, n) => JsObject(Seq("source" -> JsNumber(i), "target" -> JsNumber(j), "value" -> JsNumber(n)))}
      .collect()
    val jsonGraph = JsObject(Seq("nodes" -> JsArray(jsonVertices), "links" -> JsArray(jsonEdges)))

    // Explicitly set encoding
    val jsonFileName = new File(outputPath, "json-graph-" + timestamp + ".json")
    var file = new PrintWriter(
      new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(jsonFileName)), "UTF-8"), false)
    try {
      file.write(Json.prettyPrint(jsonGraph))
    }
    catch {
      case e: FileNotFoundException => {
        // If there was something wrong in the filename, just output locally.
        // Note: We ignore exceptions this might throw
        file.close()
        file = new PrintWriter(
          new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream("graph.json")), "UTF-8"), false)
        file.write(Json.prettyPrint(jsonGraph))
      }
      case io: IOException => println("Could not write file")
    }
    finally {
      file.close()
    }

  }
}

abstract class IterativeClusteringAlgorithm extends ClusteringAlgorithm {
  def setIterations(iterations: Int): this.type = {
    parameters.add(Iterations, iterations)
    this
  }

  def setUndirect(undirect: Boolean): this.type = {
    parameters.add(Undirect, undirect)
    this
  }
}

/** Object holding [[Parameter]] objects for clustering algorithms
  * Note: Defining all Parameters here instead of defining them as they appear in algorithms breaks encapsulation, 
  * but improves ease of use (only need to import ClusteringParameters._)
  */
object ClusteringParameters {
  case object Iterations extends Parameter[Int] {
    val defaultValue = Some(20)
  }

  case object Capacity extends Parameter[Int] {
    val defaultValue = Some(20)
  }

  case object NumOverlaps extends Parameter[Int] {
    val defaultValue = Some(20)
  }

  case object Undirect extends Parameter[Boolean] {
    val defaultValue = Some(false)
  }

  case object MinWeight extends Parameter[Weight] {
    val defaultValue = None
  }

  case object Threshold extends Parameter[Double] {
    val defaultValue = None
  }
}

/** Parallel implementation of community detection algorithm by Xie et al.
  *
  * Note that vertices are updated synchronously. Possibly better to update a sample
  * of vertices at a time. Note also that the algorithm finds overlapping communities,
  * but that only the most prominent ones are returned.
  *
  * Reference: J. Xie and B. Szymanski, "Towards Linear Time Overlapping Community
  * Detection in Social Networks", PAKDD 2012.
  * Relevant [[Parameter]] settings:
  * [[Iterations]]: maximum number of iterations
  * [[Capacity]]: memory size per vertex
  * [[NumOverlaps]]: number of cluster assignments per vertex
  * [[Undirect]]: When true, make directed graphs undirected by generating (j,i) from (i,j) edges
  * @return (i,c) tuples, where i belongs to community c
  */
class SLPA extends IterativeClusteringAlgorithm {

  override def cluster(
      edges: RDD[(ConceptID, ConceptID, Weight)],
      verticesOption: Option[RDD[Long]],
      runParameters: ParameterMap = ParameterMap())
    : RDD[(ConceptID, ClusterID)] = {

    val endParameters = parameters ++ runParameters

    val maxIterations: Int = endParameters(Iterations)
    val capacity: Int = endParameters(Capacity)
    val numOverlaps: Int = endParameters(NumOverlaps)
    val undirect: Boolean = endParameters(Undirect)
    val threshold: Double = endParameters(Threshold)

    val cacheLevel: storage.StorageLevel = MEMORY_ONLY
    var iteration = 0

    // Discard weights, collect vertices and initialize cluster id queues
    val pairs = edges.map{case (i,j,w)=>(i,j)}
    val vertices: RDD[ConceptID] = verticesOption match {
      case Some(filteredVertices) => filteredVertices
      case _ => pairs.flatMap { case (i, j) => Iterable(i, j) }.distinct()
    }
    val neighbours =
      if(undirect) pairs.flatMap{case (i,j)=>Iterable((i,j),(j,i))}
      else pairs
    neighbours.persist(cacheLevel).setName("neighbours")

    var clusterIds: RDD[(ConceptID, Queue[ConceptID])] = vertices.map(i=>(i,Queue(i))).persist(cacheLevel).setName("clusterIDs")

    while(iteration < maxIterations){
      // Select a cluster id at random for each vertex
      val randomIds = clusterIds.mapValues(c => c(Random.nextInt(c.length))).setName("randomIDs")

      // Get most common random cluster ids of neighbours for each vertex
      val newIds = neighbours.join(randomIds)
        .map{case (i, (j, r))=>((j, r), 1)} // Send cluster id to neighbours
        .reduceByKey(_ + _) // Sum the number of times r was received
        .map{case ((j, r), c)=>(j, (r, c))} // Key on the neighbor
        .reduceByKey((a, b) => if (a._2 < b._2) b else a) // For incoming nodes, select the one appearing the most times
        .map{case (j, (r, c))=>(j, r)}.cache().setName("newIds") // Remove the count

      // Add new ids to queues
      clusterIds = clusterIds.join(newIds).
        mapValues{case (q,r) =>
        val s = if(q.length >= capacity) q.dequeue._2 else q
        s.enqueue(r)
      }.persist(cacheLevel).setName("newClusterIDs")

      iteration += 1
    }

    // Assign to cluster id occurring most times per vertex
    val assignments = clusterIds.
      flatMapValues(c =>
      c.groupBy(r=>r).map{case (v,w)=>(v,w.size)}. // Count number of occurrences
        toArray.sortBy{case (v,n) => -n}. // Sort in reverse order
        take(numOverlaps).map{case (v,n) => v.toLong} // Get top assignments
      )

    // Discard singleton clusters and re-map cluster indices to {0,1,...n-1}
    val nonSingletons = assignments.map{case (i,m)=>(m,1)}.reduceByKey(_+_).filter(_._2 > 1)
    val indices = nonSingletons.keys.distinct().zipWithIndex()
    val result = assignments.map{case (i,m)=>(m,i)}.join(nonSingletons).
                             join(indices).map{case (m,((i,c),j))=>(i,j)}
    result
  }

  def setCapacity(capacity: Int): this.type = {
    parameters.add(Capacity, capacity)
    this
  }

  def setNumOverlaps(numOverlaps: Int): this.type = {
    parameters.add(NumOverlaps, numOverlaps)
    this
  }

  def setThreshold(threshold: Double): this.type = {
    parameters.add(Threshold, threshold)
    this
  }
}
object SLPA {
  def apply(): SLPA = {new SLPA()}
}

/** Returns concept id keys valued by max clique indices w r t edges with weights above thershold.
  *
  * Algorithm described at
  * https://fifthelephant.talkfunnel.com/2013/673-mapreduce-and-the-art-of-thinking-parallel
  * TODO: Find proper reference.
  * Finds max cliques of pruned graph. Clique indices follow {0,1,...,n-1}, where n is the number of cliques.
  * Relevant [[Parameter]] settings:
  * [[Iterations]]: maximum number of iterations
  * [[Undirect]]: When true, make directed graphs undirected by generating (j,i) from (i,j) edges
  * @return (i,c) tuples, where concept i belongs to max clique c
  */
class MaxCliques extends IterativeClusteringAlgorithm {
  override def cluster(
      edges: RDD[(ConceptID, ConceptID, Weight)],
      verticesOption: Option[RDD[Long]],
      runParameters: ParameterMap = ParameterMap())
    : RDD[(ConceptID, ClusterID)] = {

    val cacheLevel = MEMORY_ONLY

    val endParameters = parameters ++ runParameters

    val maxIterations: Int = endParameters(Iterations)
    val undirect: Boolean = endParameters(Undirect)

    var iteration = 0
    var hasConverged = false
    var maxCliques: RDD[TreeSet[ConceptID]] = edges.sparkContext.emptyRDD

    val prunedEdges = edges.map{case (i,j,w) => (i,j)}
    val pairs = if(undirect) prunedEdges.flatMap{case (i,j)=>Iterable((i,j),(j,i))} else prunedEdges

    var cliqueMap = pairs.aggregateByKey(TreeSet[ConceptID]())(_ + _, _ ++ _).
      map{case (i,s) => (TreeSet(i),s)}.persist(cacheLevel)

    while(iteration < maxIterations && !hasConverged){
      val newMaxCliques = maxCliques ++ cliqueMap.filter(_._2.isEmpty).map{_._1}.persist(cacheLevel)
      val newCliqueMap = cliqueMap.flatMap{case (s,t)=>t.map(v=>(s+v,t-v))}.reduceByKey(_&_).persist(cacheLevel)

      if(newCliqueMap.isEmpty) {
        hasConverged = true
      }
      // TODO: Need to verify effects of using persist and unpersist within the iterations
      maxCliques.unpersist()
      cliqueMap.unpersist()
      maxCliques = newMaxCliques
      cliqueMap = newCliqueMap
      iteration += 1
    }

    maxCliques.zipWithIndex().flatMap{case (s,c)=>s.map(i=>(i,c))}
  }
}
object MaxCliques {
  def apply(): MaxCliques  = {new MaxCliques()}
}

/** Returns vertex pairs with edges with weights above minimum strength as clusters.
  *
  * Returns concept id keys valued by cluster indices, where clusters are given by pairs of
  * vertices (i,j,w) where w >= minWeight. Accepts directed graphs.
  * Relevant [[Parameter]] settings:
  * [[MinWeight ]] the edge weight threshold
  * @return (i,c) tuples, where i belongs to cluster/pair c
  */
class EdgesToClusters extends ClusteringAlgorithm {
  override def cluster(
      edges: RDD[(ConceptID, ConceptID, Weight)],
      verticesOption: Option[RDD[Long]],
      runParameters: ParameterMap = ParameterMap())
    : RDD[(ConceptID, ClusterID)] = {

    val endParameters = parameters ++ runParameters

    val minWeight: Weight = endParameters(MinWeight)

    edges.filter { case (i, j, w) => w >= minWeight }.
      map { case (i, j, w) => if (i <= j) (i, j) else (j, i) }.
      distinct().zipWithIndex().flatMap{case ((i,j),c)=>Iterable((i,c),(j,c))}
  }
}

 /** Graph clustering functions on standard format for higher-order concept discovery.
 *
 * All functions should return an RDDs containing (cluster-identity, vertex-id) pairs.
 */
object ClusteringFunction {

   // TODO: Implement in class format
  /** Returns concept id keys valued by connected component indices.
    *
    * Note that component indices are not necessarily ordered as {0,1,...n}.
    * @param edges (i,j,w) tuples that represent edges from i to j with weight w
    * @return (i,c) tuples, where i belongs to component c
    */
  def connectedComponents(edges : RDD[(ConceptID, ConceptID, Double)]): RDD[(ConceptID, ConceptID)] = {
    var cedges = edges.flatMap{case (i,j,w)=>Iterable((i,i,j,j),(j,j,i,i))} // Initialize indices, undirect edges
    var csum = 0L // Current sum of component indices

    while(true) { // Get smallest indices of vertices and their respective neighbours
      val cindices = cedges.map{case (i,ic,j,jc)=>(i,min(ic,jc))}.reduceByKey(min(_,_))
      val nsum = cindices.values.reduce(_+_) // Sum up index assignments (monotonically decreasing)
      if(nsum == csum) return cindices // Has converged when no index decreased
      csum = nsum   // Update component index sum and assignments
      cedges = cedges.map{case (i,ic,j,jc)=>(i,(j,jc))}.join(cindices).
        map{case (i,((j,jc),ic))=>(j,(i,ic))}.join(cindices).
        map{case (j,((i,ic),jc))=>(i,ic,j,jc)}
    }

    cedges.map(v=>(0L,0L)) // Dummy return to keep compiler happy
  }

  /** Represent all clusters as a String. */
  def clustersAsString(label : String, concepts : RDD[(ConceptID, String)],
                       clusters : RDD[(ConceptID, ConceptID)]) : String =
    "="*16+"\n"+label+concepts.join(clusters).map { case (i, (s, c)) => (c, s) }.groupByKey().
                               values.map("\n"+"-"*16+"\n"+_.mkString("\n")).reduce(_+_)
}
