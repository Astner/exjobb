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

package se.sics.concepts.graphs

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import se.sics.concepts.util.ParameterMap
import se.sics.concepts.clustering.ClusteringAlgorithm
import ConceptIDs._
import se.sics.concepts.higherorder._

import scala.annotation.tailrec
import scala.collection.mutable.PriorityQueue
import scala.math.abs

/** The core functionality and algorithms of the concepts library. */
object Transformations {

  /** Remove edges lower than specific value in a edge triplet RDD. */
  def pruneEdgesByWeight(edgeData : EdgeData, minWeight : Double, absolute : Boolean = false) : EdgeData =
    edgeData.update(edgeData.weights.filter{case (i, j, w) => w >= minWeight || (absolute && abs(w) >= minWeight)})
 
  /** For each vertex, keep at most maxInDegree of the in-edges with the largest weights.
  *
  * Remove edges such that each vertex has at most maxInDegree in-edges.
  */
  def pruneEdgesByInDegree(edgeData : EdgeData, maxInDegree : Long) : EdgeData = {

    /** Possibly add edge to queue. */
    def updateQueue(q: PriorityQueue[(Long, Double)], v: (Long, Double)): PriorityQueue[(Long, Double)] = {
      if (q.size < maxInDegree) q+=v  // Add edge to queue if limit not reached
      else if (q.head._2 < v._2) {    // Else if weight larger than smallest weight in queue
        q.dequeue()                   // Replace edge; O(log maxInDegree) at worst
        q+=v
      } else q
    }

    /** Merge two priority queues bounded by maxInDegree; O(maxInDegree*log maxInDegree) at worst. */
    def mergeQueues(q1: PriorityQueue[(Long, Double)], q2: PriorityQueue[(Long, Double)]):
    PriorityQueue[(Long, Double)]= {
      for(v<-q2.iterator) updateQueue(q1,v)
      q1
    }

    // Collect edges with largest weighs in priority queues bounded by maxInDegree
    val remainingWeights = edgeData.weights.map{case (i,j,w)=>(j,(i,w))}.   // Key by terminal vertex
      combineByKey( // createCombiner: Init queue prioritized by smallest edge weight, and add edge
        (v)=>new PriorityQueue[(Long, Double)]()(Ordering.by[(Long, Double),Double](_._2).reverse)+=v,
        (q: PriorityQueue[(Long, Double)],v) => updateQueue(q, v),                // mergeValue
        (q1: PriorityQueue[(Long, Double)], q2: PriorityQueue[(Long, Double)]) => // mergeCombiner
          if (q1.size > q2.size) mergeQueues(q1,q2) else mergeQueues(q2,q1)).
      flatMap{case (j,q) => q.toIterable.map{case (i,w)=>(i,j,w)}}                // Collect and reformat

    edgeData.update(remainingWeights)
  }

  /** Transform correlation graph to similarity graph.
  *
  * Calculate similarities S(i,j)=1-L(i,j)/(c(i)+c(j)), where L(i,j) is the L1-norm of
  * the difference between correlations of i and j, and where c(i) and c(j) are the
  * sums of absolute correlations of i and j. TODO: Return error bounds.
    *
    * @param edgeData Correlation edge weights and weight sums.
  * @param conservative Returns conservative estimates of similarities if true, i.e.,
  * minimum possible similarities given discarded weights. Otherwise returns maximum
  * possible similarities (default).
  */
  def correlationsToSimilarities(edgeData : EdgeData) :
    RDD[(ConceptID, ConceptID, Double)] = {
    val inEdges = edgeData.weights.map{case (i,j,w)=>(j,(i,w))}.persist(MEMORY_ONLY)
    val twoHopEdges = inEdges.join(inEdges).filter{case (k,((i,wi),(j,wj)))=>i < j}
    val mutualTerms = twoHopEdges.  // L1-norm terms for joint neighbours
                      map{case (k,((i,wi),(j,wj)))=>((i,j),abs(wi-wj)-abs(wi)-abs(wj))}.
                      reduceByKey(_ + _)//.map{case ((i,j),vij)=>(i,j,vij)}
    val totalSums = edgeData.sums.mapValues{case (r,d) => r+d}.persist(MEMORY_ONLY)
    inEdges.unpersist()

    val similarities = mutualTerms.map{case ((i,j),vij)=>(i,(j,vij))}.join(totalSums).
                       map{case (i,((j,vij),vi))=>(j,(i,vij,vi))}.join(totalSums).
                       map{case (j,((i,vij,vi),vj))=>(i,j,1.0-(vi+vj+vij)/(vi+vj))}
    totalSums.unpersist()
    similarities
  }

  // def correlationsToSimilarities(edgeData : EdgeData, conservative : Boolean = false) :
  //   RDD[(ConceptID, ConceptID, Double)] = {
  //   val inEdges = edgeData.weights.map{case (i,j,w)=>(j,(i,w))}.persist(MEMORY_ONLY)
  //   val twoHopEdges = inEdges.join(inEdges).filter{case (k,((i,wi),(j,wj)))=>i < j}
  //   val mutualTerms = twoHopEdges.  // L1-norm terms for joint neighbours
  //                     map{case (k,((i,wi),(j,wj)))=>((i,j),abs(wi-wj)-abs(wi)-abs(wj))}.
  //                     reduceByKey(_ + _).map{case ((i,j),vij)=>(i,j,vij)}
  //   val totalSums = edgeData.sums.mapValues{case (r,d)=> if(conservative) r+d else r}
  //   inEdges.unpersist()
  //   joinVertexAndEdgeValues(totalSums,mutualTerms).
  //   map{case (i,j,vi,vj,vij)=>(i,j,1.0-(vi+vj+vij)/(vi+vj))}
  // }

  /** Transform correlation graph to similarity graph. */
  def correlationsToSimilarities(correlationEdges : RDD[(ConceptID, ConceptID, Double)],
                                 initialSimGraph  : RDD[(ConceptID, ConceptID, Double)]) :
  RDD[(ConceptID, ConceptID, Double)] = {
    correlationEdges // NOT IMPLEMENTED - just returns correlation graph
  }

}
