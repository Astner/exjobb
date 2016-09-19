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

package se.sics.concepts.graphs

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.math.abs

/** Class for managing edge data in concept graphs.
 *
 * @param weights (i,j,wij) triples, where wij is the weight of edge (i,j)
 * @param sums (i,(ri,di)) pairs, where ri and di are sums of remaining and discarded
 * out-edge weights, respectively.
 */
case class EdgeData(weights : RDD[(Long, Long, Double)], sums : RDD[(Long, (Double, Double))]) {
  /** Return EdgeData with updated weights and sums given by remaining edges after pruning. */
  def update(remainingWeights : RDD[(Long, Long, Double)]) : EdgeData = {
    val newSums = remainingWeights.map{case (i,j,w)=>(i,abs(w))}.reduceByKey(_+_).
                  join(sums).mapValues{case (nri,(ori,odi))=>(nri,odi+ori-nri)} // NOTE: May discard vertices
    EdgeData(remainingWeights,newSums)
  }

  /** Return merged EdgeData */
  def merge(other: EdgeData) : EdgeData = {
    val newWeights = weights ++ other.weights // NOTE: Possibly slow
    val newSums = sums.fullOuterJoin(other.sums).
        mapValues{case (st,so) =>
          val pt = st.getOrElse((0D,0D))
          val po = so.getOrElse((0D,0D))
          (pt._1+po._1,pt._2+po._2)
        }
    EdgeData(newWeights,newSums)
  }

  /** Return persisted EdgeData according to storage level.
    * NOTE: Not entirely sure this works as intended. */
  def persist(level : StorageLevel = StorageLevel.MEMORY_AND_DISK) : EdgeData = {
    EdgeData(weights.persist(level),sums.persist(level))
  }

  /** Unpersist EdgeData. */
  def unpersist() : Unit = {
    weights.unpersist()
    sums.unpersist()
  }
}

/** Companion object for [[EdgeData]] class. */
object EdgeData {
  /** Create empty EdgeData object. */
  def apply(sc : SparkContext) : EdgeData = {
    new EdgeData(sc.emptyRDD,sc.emptyRDD)
  }

  /** Create EdgeData object from weights RDD. */
  def apply(weights : RDD[(Long, Long, Double)]) : EdgeData = {
    val sums = weights.map{case (i,j,w)=>(i,abs(w))}.reduceByKey(_+_).mapValues{ri =>(ri,0D)}
    new EdgeData(weights,sums)
  }
}
