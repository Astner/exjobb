package com.sics.concepts

/*
 Copyright 2015 Daniel Gillblad and Olof Görnerup (dgi@sics.se,
 olofg@sics.se)

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

import scala.reflect._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel._
import com.sics.concepts.ConceptsUtil._
import scala.math.abs
import scala.collection.mutable.PriorityQueue

/** The core functionality and algorithms of the concepts library. */
object core {
  /** Type alias for concept IDs, used to increase readability. */
  type ConceptID = Long
  /** Type alias for data indices, used to increase readability. */
  type DataIndex = Long
  /** Type alias for concept counts, used to increase readability. */
  type ConceptCount = Long

  /** Count unique occurrences of concept id:s, possibly filter out concepts with few occurrences.
  *
  * Count unique concept occurrences in an indexed concept data set, possibly filter out concepts
  * with few occurrences, and return an RDD consisting of concept-count pairs.
  */
  def countAndFilter(indexedData : RDD[(DataIndex, ConceptID)], minCount : Long = 0) :
  RDD[(ConceptID, ConceptCount)] = {
    if (minCount > 1) indexedData.map(x=>(x._2,1L)).reduceByKey(_ + _).filter(x=>x._2>=minCount)
    else indexedData.map(x=>(x._2,1L)).reduceByKey(_ + _)
  }

  /** Count unique pairwise occurrences of concept id:s, possibly filter pairs with few occurrences.
  *
  * Count unique pairwise concept occurrences in an indexed concept data set, possibly filter out pairs
  * with few occurrences, and return an RDD consisting of (concept, concept)-count pairs. The
  * concept-concept pairs are guaranteed to be ordered, i.e. the first concept id is equal to or lower
  * than the second concept id.
  */
  def countAndFilterPairwise(indexedData : RDD[(DataIndex, ConceptID)],
                             countData : RDD[(ConceptID, Long)],
                             minCount : Long = 0) : RDD[((ConceptID, ConceptID), ConceptCount)] = {
    // Remove concepts with too few counts
    val filterd = indexedData.map(x=>(x._2,x._1)).join(countData).map(x=>(x._2._1,x._1))
    val counted = filterd.join(filterd).filter(x=>x._2._1 < x._2._2). // Find all pairs, keep only relevant
                  map(x=>(x._2,1L)).reduceByKey(_ + _)                // And count
    if(minCount > 1) counted.filter(x=>x._2>=minCount)                // Filter if necessary
    else counted
  }

  /** Count unique pairwise combinations of concept id:s, possibly filter pairs with few occurrences.
  *
  * Count unique pairwise concept combinations in two indexed concept data set, possibly filter out pairs
  * with few occurrences, and return an RDD consisting of (concept, concept)-count pairs. The
  * concept-concept pairs are guaranteed to be ordered, i.e. the first concept id is equal to or lower
  * than the second concept id.
  */
  def countAndFilterPairwiseCombined(indexedDataA : RDD[(DataIndex, ConceptID)],
                                     indexedDataB : RDD[(DataIndex, ConceptID)],
                                     countData    : RDD[(DataIndex, ConceptID)],
                                     minCount     : Long = 0) : RDD[((ConceptID, ConceptID), ConceptCount)] = {
    // Remove concepts with too few counts
    val filterda = indexedDataA.map(x=>(x._2,x._1)).join(countData).map(x=>(x._2._1,x._1))
    val filterdb = indexedDataB.map(x=>(x._2,x._1)).join(countData).map(x=>(x._2._1,x._1))
    val counted = filterda.join(filterdb).filter(x=>x._2._1 < x._2._2). // Find all pairs, keep only relevant
                  map(x=>(x._2,1L)).reduceByKey(_ + _)                  // And count
    if(minCount > 1) counted.filter(x=>x._2>=minCount)                  // Filter if necessary
    else counted
  }

  /** Incrementally count unique pairwise combinations of concept id:s, filter pairs with few occurrences.
  *
  * Count unique pairwise concept combinations from two indexed concept data set, possibly filter out pairs
  * with few occurrences, and return an RDD consisting of a pair of 1) a (concept, concept) pair, guaranteed
  * to be ordered i.e. the first concept id is lower than the second, and 2) a tuple describing pairwise counts,
  * total counts of the first concept and total counts of the second concept.
  */
  def incrementalCombinedCounts(indexedDataA : RDD[(DataIndex, ConceptID)],
                                indexedDataB : RDD[(DataIndex, ConceptID)],
                                minCount     : Long = 0) :
  RDD[((ConceptID, ConceptID), (ConceptCount, ConceptCount, ConceptCount))] = {
    val pcounts = indexedDataA.join(indexedDataB).filter(x=>x._2._1 < x._2._2).
                  map(x=>(x._2,1L)).reduceByKey(_ + _)
    val scounts = if(indexedDataA == indexedDataB) countAndFilter(indexedDataA, minCount).persist(MEMORY_ONLY)
                  else countAndFilter(indexedDataA ++ indexedDataB, minCount).persist(MEMORY_ONLY)
    val acounts = pcounts.map(x=>(x._1._1, (x._1._2, x._2))).join(scounts).
                  map(x=>(x._2._1._1, (x._1, x._2._1._2, x._2._2))).join(scounts).
                  map(x=>((x._2._1._1, x._1),(x._2._1._2, x._2._1._3, x._2._2)))
    scounts.unpersist(false)
    acounts
  }

  /** Count unique example existences of concept id:s, possibly filter out concepts with few occurrences.
  *
  * Count unique concept existence occurrences in an indexed concept data set, i.e. the number of
  * examples a concept occurs at least once in, possibly filter out concepts
  * with few occurrences, and return an RDD consisting of concept-count pairs.
  */
  def countExistences(indexedData : RDD[(DataIndex, ConceptID)], minCount : Long = 0) :
  RDD[(ConceptID, ConceptCount)] = {
    if (minCount > 1) indexedData.distinct().map(x=>(x._2,1L)).reduceByKey(_ + _).filter(x=>x._2>=minCount)
    else indexedData.distinct().map(x=>(x._2,1L)).reduceByKey(_ + _)
  }

  /** Incrementally count pairwise combinations of concept id:s existences, filter pairs with few occurrences.
  *
  * Count unique pairwise concept existence combinations from two indexed concept data set, i.e. count the
  * number of examples the concept id combination have existed in, possibly filter out pairs
  * with few occurrences, and return an RDD consisting of a pair of 1) a (concept, concept) pair, guaranteed
  * to be ordered i.e. the first concept id is lower than the second, and 2) a tuple describing pairwise counts,
  * total counts of the first concept and total counts of the second concept.
  */
  def incrementalCoexistenceCounts(indexedDataA : RDD[(DataIndex, ConceptID)],
                                   indexedDataB : RDD[(DataIndex, ConceptID)],
                                   minCount     : Long = 0,
                                   selfCount    : Boolean = true) :
  RDD[((ConceptID, ConceptID), (ConceptCount, ConceptCount, ConceptCount))] = {
    val selfCVal : Long = if (selfCount) 1L else 0L

    val jdata = if (indexedDataA == indexedDataB)
                  indexedDataA.join(indexedDataB).filter(x=>x._2._1 <= x._2._2)
                else
                  indexedDataA.join(indexedDataB).
                  map(x=>if(x._2._1 > x._2._2) (x._1, (x._2._2, x._2._1)) else x) ++
                  indexedDataB.join(indexedDataB).filter(x=>x._2._1 <= x._2._2)

    val pcounts = jdata.map{case (i,(c1,c2)) => ((i,c1,c2),1L)}.
                  reduceByKey(_ + _).map{
                    case ((i,c1,c2),n) =>
                      if (c1 == c2) {
                        if (n >= 2) ((c1,c2),selfCVal) else ((c1,c2),0L)
                      }
                      else ((c1,c2),1L)
                      }.
                  reduceByKey(_ + _).filter{case ((c1,c2),n) => n >= 1}

    val scounts = if(indexedDataA == indexedDataB) countExistences(indexedDataA, minCount).persist(MEMORY_ONLY)
                  else countExistences(indexedDataA ++ indexedDataB, minCount).persist(MEMORY_ONLY)

    val acounts = pcounts.map(x=>(x._1._1, (x._1._2, x._2))).join(scounts).
                  map(x=>(x._2._1._1, (x._1, x._2._1._2, x._2._2))).join(scounts).
                  map(x=>((x._2._1._1, x._1),(x._2._1._2, x._2._1._3, x._2._2)))

    scounts.unpersist(false)
    acounts
  }
