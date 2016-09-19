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
import se.sics.concepts.graphs.ConceptIDs._
import org.apache.spark.storage.StorageLevel._

/** The core functionality and algorithms of the concepts library. */
object Counts {

  /** Count concept id:s, possibly filter out concepts with few occurrences.
  *
  * Count concept occurrences in an indexed concept data set, possibly filter out concepts
  * with few occurrences, and return an RDD consisting of concept-count pairs.
  */
  def countConcepts(indexedData : RDD[(DataIndex, ConceptID)], minCount : Long = 0) :
  RDD[(ConceptID, ConceptCount)] = {
    if (minCount > 1) indexedData.map(x=>(x._2,1L)).reduceByKey(_ + _).filter(x=>x._2>=minCount)
    else indexedData.map(x=>(x._2,1L)).reduceByKey(_ + _)
  }

  /** Count unique example existences of concept id:s, possibly filter out concepts with few occurrences.
  *
  * Count unique concept existence occurrences in an indexed concept data set, i.e. the number of
  * examples a concept occurs at least once in, possibly filter out concepts
  * with few occurrences, and return an RDD consisting of concept-count pairs.
  * While this is typically the concept count of interest,
  * most data sets used in the library will fulfill the uniqueness guarantee by themselves
  * and we do not want to incur the (rather costly) uniqueness guarantee. In these cases, use
  * countConcepts.
  */
  def countExistences(indexedData : RDD[(DataIndex, ConceptID)], minCount : Long = 0) :
  RDD[(ConceptID, ConceptCount)] =
    countConcepts(indexedData.distinct(), minCount)

  /** Incrementally count pairwise combinations of concept id:s occurrences, filter pairs with few occurrences.
  *
  * Count pairwise concept combinations from two indexed concept data sets, possibly filter out pairs
  * with few occurrences, and return an RDD consisting of a pair of 1) a (concept, concept) pair, guaranteed
  * to be ordered i.e. the first concept id is lower than the second, and 2) a tuple describing pairwise counts,
  * total counts of the first concept and total counts of the second concept.
  */
  def incrementalPairwiseCounts(indexedDataA : RDD[(DataIndex, ConceptID)],
                                indexedDataB : RDD[(DataIndex, ConceptID)],
                                minCount     : Long = 0) :
    RDD[((ConceptID, ConceptID), (PairCount, ConceptCount, ConceptCount))] = { //TODO: Change second returned tuple to case class
    // TODO: Overload function and remove reference equality checks
    val joinedData: RDD[(DataIndex, (DataIndex, DataIndex))] = if (indexedDataA == indexedDataB) { // Why reference equality? What happens in distributed setting?
                  indexedDataA.join(indexedDataB).filter(x=>x._2._1 < x._2._2)
                }
                else {
                  indexedDataA.join(indexedDataB).filter(x=>x._2._1 != x._2._2).
                  mapValues(v=>if(v._1 > v._2) (v._2, v._1) else v) ++
                  indexedDataB.join(indexedDataB).filter(x=>x._2._1 < x._2._2)
                }

    val pairCounts: RDD[((ConceptID, ConceptID), PairCount)] = joinedData
      .map{case (i,(c1,c2)) => ((c1,c2),1L)}.reduceByKey(_ + _).filter{case ((c1,c2),c) => c >= minCount}

    val stringCounts: RDD[(ConceptID, ConceptCount)] = if(indexedDataA == indexedDataB) {
      countConcepts(indexedDataA, minCount)
    } else {
      countConcepts(indexedDataA ++ indexedDataB, minCount)
    }

    // Add counts for individual strings to counts of pairs
    // TODO: See if this is a heavy operation (2 joins), maybe optimizable through bcast join (if pairCounts >> stringCounts)
    // TODO: Change cncpt to whatever we decide we call items/objects
    val allCounts: RDD[((DataIndex, DataIndex), (PairCount, ConceptCount, ConceptCount))] = pairCounts
      .map{ case ((cncptID1, cncptID2), pairCount) =>
        (cncptID1, (cncptID2, pairCount)) }
      .join(stringCounts)
      .map{ case (cncptID1, ((cncptID2, pairCount), cncpt1Count)) =>
        (cncptID2, (cncptID1, pairCount, cncpt1Count))}
      .join(stringCounts)
      .map{ case (cncptID2, ((cncptID1, pairCount, cncpt1Count), cncpt2Count)) =>
        ((cncptID1, cncptID2), (pairCount, cncpt1Count, cncpt2Count))}

    stringCounts.unpersist(false)
    allCounts
  }

  /** Incrementally count pairwise combinations of concept id:s occurrences using contex counts.
    * Concepts and contexts with few occurrences are discarded.
    */
  def incrementalPairwiseCountsFromCounts(contextCountsA : RDD[(ConceptID, (ContextID, ContextCount))],
                                          contextCountsB : RDD[(ConceptID, (ContextID, ContextCount))],
                                          minConceptCount : Long,
                                          minContextCount : Long,
                                          storageLevel : storage.StorageLevel = MEMORY_AND_DISK) :
    RDD[((ConceptID, ConceptID), (PairCount, ConceptCount, ConceptCount))] = {

    def countConcepts(counts: RDD[(ConceptID, (ContextID, ContextCount))]) =
                      counts.mapValues{case (cx, ct) => ct}.reduceByKey(_ + _).
                             filter{case (cp, ct) => ct >= minConceptCount} // Discard to speed up later joins

    def sumUpPairCounts(p: RDD[(ContextID, ((ConceptID, ContextCount), (ConceptID, ContextCount)))]) =
                        p.map{case (cx, ((cpa, cta), (cpb, ctb))) => ((cpa, cpb), cta)}. // NOTE: cta == ctb
                          reduceByKey(_ + _).map{case ((cpa, cpb), ct) => (cpa, (cpb, ct))}

    val countsPair = {
      val frequentContextsB = contextCountsB.filter{case (cp, (cx, ct)) => ct >= minContextCount}.persist(storageLevel)
      val keyedByContextB = frequentContextsB.map{case (cp, (cx, ct)) => (cx, (cp, ct))}.persist(storageLevel)
      val joinedBB = keyedByContextB.join(keyedByContextB).filter{case (cx, ((cpa, cta), (cpb, ctb))) => cpa < cpb}                       
      val pairCountsB = sumUpPairCounts(joinedBB)
      val conceptCountsB = countConcepts(frequentContextsB)            

      if(contextCountsA.id == contextCountsB.id) 
        (pairCountsB, conceptCountsB)
      else{
        val frequentContextsA = contextCountsA.filter{case (cp, (cx, ct)) => ct >= minContextCount}.persist(storageLevel)
        val keyedByContextA = frequentContextsA.map{case (cp, (cx, ct)) => (cx, (cp, ct))}.persist(storageLevel)
        val joinedAB = keyedByContextA.join(keyedByContextB).filter{case (cx, ((cpa, cta), (cpb, ctb))) => cpa != cpb}.
                                       mapValues{case (p, q) => if(p._1 < q._1) (p, q) else (q, p)}
        val pairCountsAB = sumUpPairCounts(joinedAB).union(pairCountsB)
        (pairCountsAB, countConcepts(frequentContextsA).union(conceptCountsB))
      }
    }

    countsPair._1.join(countsPair._2).map{case (cpa, ((cpb, ctab), cta)) => (cpb, (cpa, cta, ctab))}.
                  join(countsPair._2).map{case (cpb, ((cpa, cta, ctab), ctb)) => ((cpa, cpb), (ctab, cta, ctb))}
  }

  /** Incrementally count pairwise combinations of concept id:s existences, filter pairs with few occurrences.
  *
  * Count unique pairwise concept existence combinations from two indexed concept data sets, i.e. count the
  * number of examples the concept id combination have existed in, possibly filter out pairs
  * with few occurrences, and return an RDD consisting of a pair of 1) a (concept, concept) pair, guaranteed
  * to be ordered i.e. the first concept id is lower than the second, and 2) a tuple describing pairwise counts,
  * total counts of the first concept and total counts of the second concept. Note that if examples in data are
  * already guaranteed to be unique, incrementalPairwiseCounts return the same result much faster.
  */
  def incrementalCoexistenceCounts(indexedDataA : RDD[(DataIndex, ConceptID)],
                                   indexedDataB : RDD[(DataIndex, ConceptID)],
                                   minCount     : Long = 0) :
  RDD[((ConceptID, ConceptID), (ConceptCount, ConceptCount, ConceptCount))] = {
    if(indexedDataA == indexedDataB) {
      val udata = indexedDataA.distinct()
      incrementalPairwiseCounts(udata, udata, minCount)
    }
    else incrementalPairwiseCounts(indexedDataA.distinct(), indexedDataB.distinct(), minCount)
  }

  /** Count the number of times all concepts are repeated 2 or more times in an example.
  *
  * Count the number of times a concept C occurs N times in an example, possibly filtering out
  * (C,N) combinations occurring few times. Returns an RDD consisting of a pair of pairs
  * ((C,N),(O,T)) where O is the number of times C has occurred N times in an example and T
  * the number of times C occurs in an example (thus, O <= T).
  */
  def countRecurrences(indexedData : RDD[(DataIndex, ConceptID)], minCount : Long = 0) :
  RDD[((ConceptID,Int),(ConceptCount,ConceptCount))] = {
    val icounts = indexedData.map(x=>(x,1)).reduceByKey(_ + _).persist(MEMORY_ONLY)
    val ucounts = icounts.map(x=>(x._1._2,1L)).reduceByKey(_ + _)
    val rcounts = icounts.filter(x=>x._2 >=2).
                  map{case ((i,c),n) => ((c,n),1L)}.reduceByKey(_ + _)
    icounts.unpersist()
    val fcounts = if (minCount >= 2L) rcounts.filter(x=>x._2 >= minCount) else rcounts
    fcounts.map{case ((c,n),o) => (c,(n,o))}.join(ucounts).map{case (c,((n,o),t)) => ((c,n),(o,t))}
  }

  /** Keep context counts of top minRank concepts with respect to counts */
  def pruneContextCountsByRank(contextCounts : RDD[(ConceptID, (ContextID, ContextCount))], maxRank : Int) : 
        RDD[(ConceptID, (ContextID, ContextCount))] = {
    // Sum up concept counts and get top ones
    val topConcepts = contextCounts.mapValues{case (cxt, cnt) => cnt}.reduceByKey(_ + _).
                      sortBy(_._2, ascending = false).zipWithIndex.
                      filter{case ((cxt, sum), r) => r <= maxRank}.keys
    // Return context counts with keps concepts
    contextCounts.join(topConcepts).mapValues{case ((ctx, cnt), sum) => (ctx, cnt)}
  }
}
