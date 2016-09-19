/*
 Copyright (C) 2015 Daniel Gillblad, Olof Görnerup, Theodoros Vasiloudis (dgi@sics.se,
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

package se.sics.concepts.clustering

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import se.sics.concepts.util.{ParameterMap, WithParameters, Parameter}
import se.sics.concepts.util.ConceptsUtil._
import se.sics.concepts.clustering.ClusteringParameters._
//import se.sics.concepts.util.WithParameters
import se.sics.concepts.graphs.ConceptIDs._
import se.sics.concepts.graphs.ConceptIDs

import scala.collection.immutable.{Queue, TreeSet}
import scala.math._
import scala.util.Random

abstract class ClusteringAlgorithm extends WithParameters {
  def cluster(edges: RDD[(ConceptID, ConceptID, Weight)], runParameters: ParameterMap = ParameterMap()): RDD[(ConceptID, ClusterID)]

  def setUndirect(undirect: Boolean): this.type = {
    parameters.add(Undirect, undirect)
    this
  }
}

abstract class IterativeClusteringAlgorithm extends ClusteringAlgorithm {
  def setIterations(iterations: Int): this.type = {
    parameters.add(Iterations, iterations)
    this
  }
}

/** Object holding [[Parameter]] objects for clustering algorithms.
  *
  * Note: Defining all Parameters here instead of defining them as they appear in algorithms breaks encapsulation,
  * but improves ease of use (only need to import ClusteringParameters._)
  */
object ClusteringParameters {
  case object Iterations extends Parameter[Int] {
    val defaultValue = Some(16)
  }

  case object Capacity extends Parameter[Int] {
    val defaultValue = Some(4)
  }

  case object Undirect extends Parameter[Boolean] {
    val defaultValue = Some(true)
  }

  case object MinWeight extends Parameter[Weight] {
    val defaultValue = None
  }

  case object MinClusterStrength extends Parameter[Double] {
    val defaultValue = Some(0.5)
  }

  case object UseWeights extends Parameter[Boolean] {
    val defaultValue = Some(false)
  }
}

/** Community detection algorithm using label propagation.
  *
  * Note that vertices are updated synchronously. Possibly better to update a sample
  * of vertices at a time. Note also that the algorithm finds overlapping communities,
  * but that only the most prominent ones are returned.
  *
  * The algorithm is loosely based on: 
  * J. Xie and B. Szymanski, "Towards Linear Time Overlapping Community
  * Detection in Social Networks", PAKDD 2012.
  *
  * Relevant [[Parameter]] settings:
  *
  *  - '''Iterations''' maximum number of iterations
  *  - '''Capacity''' memory size per vertex
  *  - '''MinClusterStrength''' minimum strength of a cluster
  *  - '''Undirect''' When true, make directed graphs undirected by generating (j,i) from (i,j) edges
  *  - '''UseWeights''' use edge weights when true, else edges are given weight 1
  *
  * @return (i,c) tuples, where i belongs to community c
  */
class SLPA extends IterativeClusteringAlgorithm { 

  override def cluster(edges: RDD[(ConceptID, ConceptID, Weight)], runParameters: ParameterMap = ParameterMap())
    : RDD[(ConceptID, ClusterID)] = {

    val endParameters = parameters ++ runParameters

    val maxIterations: Int = endParameters(Iterations)
    val capacity: Int = endParameters(Capacity)
    val undirect: Boolean = endParameters(Undirect)
    val minClusterStrength: Double = endParameters(MinClusterStrength)
    val useWeights: Boolean = endParameters(UseWeights)

    val cacheLevel: storage.StorageLevel = MEMORY_ONLY
    var iteration = 0

    // Collect vertices and initialize cluster id queues
    val vertices = edges.flatMap{case (i, j, w) => Iterable(i, j)}.distinct
    val neighbours = { // Neighbours w r t out-edges
      val e = if(useWeights) edges else edges.map{case (i, j, w) => (i, j, 1D)}
      if(undirect) e.flatMap{case (i, j, w) => Iterable((i, (j, w)), (j, (i, w)))} 
      else e.map{case (i, j, w) => (i, (j, w))}
    }.persist(cacheLevel).setName("neighbours")

    var clusterIds: RDD[(ConceptID, Queue[ConceptID])] = 
      vertices.map(i => (i, Queue(i))).persist(cacheLevel).setName("clusterIDs")

    println("maxIterations: " + maxIterations.toString)

    while(iteration < maxIterations){
      // Select a cluster id at random for each vertex
      val randomIds = clusterIds.mapValues(c => c(Random.nextInt(c.length))).setName("randomIDs")

      // Get cluster ids of neighbours with largest weight sum for each vertex
      val newIds = neighbours.join(randomIds).
        map{case (i, ((j, w), r)) => ((j, r), w)}.                 // Send cluster id to neighbours
        reduceByKey(_ + _).map{case ((j, r), s) => (j, (r, s))}.   // Sum up weights of incoming cluster ids
        reduceByKey((a, b) => if(a._2 > b._2) a else b).           // Get ids with largest weight sums
        map{case (j, (r, s)) => (j, r)}.cache().setName("newIds")

      // Add new ids to queue
      clusterIds = clusterIds.join(newIds).
        mapValues{case (q, r) =>
          val s = if(q.length >= capacity) q.dequeue._2 else q
          s.enqueue(r)
        }.persist(cacheLevel).setName("newClusterIDs")

      iteration += 1
    }

    // Calculate cluster strengths and discard clusters with strengths below threshold
    val candidateAssignments = clusterIds.flatMap{case (i,q) => q.map(c => ((i,c),1L))}.
                               reduceByKey(_+_).
                               mapValues(_.toDouble / capacity.toDouble).
                               filter{case (k,s) => s >= minClusterStrength}.
                               keys

     // Discard singleton clusters
    val clusterCounts = candidateAssignments.map{case (i,c)=>(c,1L)}.reduceByKey(_+_).filter{case (c,n) => n > 1L}
    val assignments = candidateAssignments.map{case (i,c)=>(c,i)}.join(clusterCounts).map{case (c,(i,n))=>(i,c)}
    
    // Remove duplicate clusters and return
    ClusteringFunction.discardDuplicateClusters(assignments)
  }

  def setCapacity(capacity: Int): this.type = {
    parameters.add(Capacity, capacity)
    this
  }

  def setMinClusterStrength(minClusterStrength: Double): this.type = {
    parameters.add(MinClusterStrength, minClusterStrength)
    this
  }

  def setUseWeights(useWeights: Boolean): this.type = {
    parameters.add(UseWeights, useWeights)
    this
  }
}

/** Companion object of class [[SLPA]] */
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
  *
  *  - '''Iterations''' Maximum number of iterations
  *  - '''Undirect''' When true, make directed graphs undirected by generating (j,i) from (i,j) edges
  *
  * @return (i,c) tuples, where concept i belongs to max clique c
  */
class MaxCliques extends IterativeClusteringAlgorithm {
  override def cluster(edges: RDD[(ConceptID, ConceptID, Weight)], runParameters: ParameterMap = ParameterMap())
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

/** Companion object of class [[MaxCliques]] */
object MaxCliques {
  def apply(): MaxCliques  = {new MaxCliques()}
}

/** Returns vertex pairs with edges with weights above minimum strength as clusters.
  *
  * Returns concept id keys valued by cluster indices, where clusters are given by pairs of
  * vertices (i,j,w) where w >= minWeight. Accepts directed graphs.
  * Relevant [[Parameter]] settings:
  *
  *  - '''MinWeight''' the edge weight threshold
  *
  * @return (i,c) tuples, where i belongs to cluster/pair c
  */
class EdgesToClusters extends ClusteringAlgorithm {
  override def cluster(edges: RDD[(ConceptID, ConceptID, Weight)], runParameters: ParameterMap)
    : RDD[(ConceptID, ClusterID)] = {

    val endParameters = parameters ++ runParameters

    val minWeight: Weight = endParameters(MinWeight)

    edges.filter { case (i, j, w) => w >= minWeight }.
      map { case (i, j, w) => if (i <= j) (i, j) else (j, i) }.
      distinct().zipWithIndex().flatMap{case ((i,j),c)=>Iterable((i,c),(j,c))}
  }
}

/** Groups vertices i and j, where edge (i, j) has the largest weight both for i and j.
  *
  * Returns concept id keys valued by cluster indices, where clusters are given by pairs of
  * vertices i and j for which edge (i, j) has the largest weight both for i and j.
  *
  * @return (i, c) tuples, where i belongs to cluster c
  */
class MutualMax extends ClusteringAlgorithm {
  override def cluster(edges: RDD[(ConceptID, ConceptID, Weight)], runParameters: ParameterMap)
    : RDD[(ConceptID, ClusterID)] = {
    val endParameters = parameters ++ runParameters
    val undirect: Boolean = endParameters(Undirect)
    val allEdges = if(undirect) edges.flatMap{case (i, j, w) => Iterable((i, j, w), (j, i, w))} else edges
    allEdges.map{case (i, j, w) => (i, (j, w))}.reduceByKey((a, b) => if(a._2 > b._2) a else b). // Max
             map{case (i, (j, w)) => (if(i < j) (i, j) else (j, i), 1)}.
             reduceByKey(_ + _).filter{case ((i, j), s) => s == 2}. // Mutual
             keys.zipWithIndex.flatMap{case ((i, j), c) => Iterable((i, c), (j, c))}
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
    *
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

  /** Remove redundant clusters
    *
    * @param assignments (i,c) tuples, where i belongs to community c
    * @return (i,c) tuples, where i belongs to community c
    */
  def discardDuplicateClusters(assignments : RDD[(ConceptID, ClusterID)]) : RDD[(ConceptID, ClusterID)] = {
    assignments.map(_.swap).                                 // Key by cluster id
                groupByKey.                                  // Group by cluster id
                values.map(_.toSet).                         // Discard keys and unorder
                distinct.                                    // Get rid of duplicates
                zipWithIndex.                                // Index unique clusters
                flatMap{case (c, i) => c.map(d => (d, i))}   // Assign to new cluster ids and flatten
  }

  /** Represent all clusters as a String. */
  def clustersAsString(label : String, concepts : RDD[(ConceptID, String)],
                       clusters : RDD[(ConceptID, ConceptID)]) : String =
    "="*16+"\n"+label+concepts.join(clusters).map { case (i, (s, c)) => (c, s) }.groupByKey().
                               values.map("\n"+"-"*16+"\n"+_.mkString("\n")).reduce(_+_)
}
