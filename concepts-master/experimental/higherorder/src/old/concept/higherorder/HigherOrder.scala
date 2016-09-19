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

package se.sics.concepts.higherorder

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import se.sics.concepts.util.ParameterMap
import se.sics.concepts.clustering.ClusteringAlgorithm
import se.sics.concepts.graphs.{Counts, ConceptIDs, EdgeData}
import ConceptIDs._
import Counts._
import se.sics.concepts.graphs.Transformations._
import se.sics.concepts.clustering.ClusteringParameters._
import se.sics.concepts.higherorder._
import scala.math.{min, max, log}

import scala.annotation.tailrec

/** The core higher order concept functionality and algorithms of the concepts library. */
object HigherOrder {
  /** Find commonly repeated concepts.
  *
  * Find commonly repeated concepts, i.e. concept C occurring N times in an example. Returns an RDD
  * concsisting of a pair (C,N), where C has occurred N times in an example more than minCount
  * times in the data set.
  */
  def findRecurrencentConcepts(indexedData : RDD[(DataIndex, ConceptID)], minCount : Long = 0) :
  RDD[(ConceptID,Int)] = {
    indexedData.map(x=>(x,1)).reduceByKey(_ + _).filter(x=>x._2 >=2).
    map{case ((i,c),n) => ((c,n),1L)}.reduceByKey(_ + _).
    filter(x=>x._2 >= minCount).keys
  }

  /** Find all recurrent concept activations in a concept data set.
  *
  * Find all recurrent concepts activated directly in an indexed concept data set.
  * Returns an RDD consisting of pairs of instance index and concept id.
  * Recurrent concepts are represented as pairs (r,(c,n), where r is the recurrent concept id,
  * c is the constituent concept id, and n the number of repetitions.
  */
  def activateRecurrentConcepts(indexedData  : RDD[(DataIndex, ConceptID)],
                                recconcepts  : RDD[(ConceptID, (ConceptID, Int))]) :
  RDD[(DataIndex, ConceptID)] = {
    val multcounts = indexedData.map(x=>(x,1)).reduceByKey(_ + _).filter(x=>x._2 >=2).
                     map{case ((i,c),n) => (c,(i,n))}
    recconcepts.map{case (r,(c,n)) => (c,(n,r))}.join(multcounts).
    filter(x=>x._2._1._1 <= x._2._2._2).map{case (c,((n,r),(i,m))) => (i,r)}
  }

  /** Find all direct OR concepts activations in a concept data set.
  *
  * Find all OR concepts activated directly in an indexed concept data set. The procedure
  * must be iterated to find higher-order concept activations. Returns an RDD consisting
  * of pairs of instance index and concept id. OR concepts are represented as pairs of
  * constituent concept id and higher order concept id.
  */
  def activateORConcepts(indexedData : RDD[(DataIndex, ConceptID)],
                         orconcepts  : RDD[(ConceptID, ConceptID)]) : RDD[(DataIndex, ConceptID)] = {
    indexedData.map(_.swap).join(orconcepts).values.distinct()
  }

  /** Find all direct AND concepts activations in a concept data set.
  *
  * Find all AND concepts activated directly in an indexed concept data set. The procedure
  * must be iterated to find higher-order concept activations. Returns an RDD consisting
  * of pairs of instance index and concept id. OR concepts are represented as pairs of
  * constituent concept id and higher order concept id. The procedure also requires a data set
  * consisting of pairs of higher-order concept id and the total number of its constituent
  * concepts.
  */
  def activateANDConcepts(indexedData : RDD[(DataIndex, ConceptID)],
                          andconcepts : RDD[(ConceptID, ConceptID)],
                          andcsize    : RDD[(ConceptID, Int)]) : RDD[(DataIndex, ConceptID)] = {
    indexedData.map(_.swap).join(andconcepts).map(_.swap).   // Find all activations
    aggregateByKey(Set[Long]())((s,v)=>s + v, (x,y) => x|y). // Aggregate sets by key
    mapValues(_.size).filter(_._2 >= 2).                     // Number of unique activations, filter single
    map(x=>(x._1._2, (x._1._1, x._2))).                      // Change key
    join(andcsize).filter(x => x._2._1._2>=x._2._2).         // Keep only fully activated and concepts
    map(x=>(x._2._1._1, x._1))                               // Change format, remove (now) useless info
  }

  /** Find all direct AND concepts activations in a concept data set.
  *
  * Find all AND concepts activated directly in an indexed concept data set. The procedure
  * must be iterated to find higher-order concept activations. Returns an RDD consisting
  * of pairs of instance index and concept id. OR concepts are represented as pairs of
  * constituent concept id and higher order concept id. The procedure also requires a data set
  * consisting of pairs of higher-order concept id and the total number of its constituent
  * concepts. This procedure assumes unique object occurrences in each context.
  */
  def activateANDConceptsUnique(indexedData : RDD[(DataIndex, ConceptID)],
                                andconcepts : RDD[(ConceptID, ConceptID)],
                                andcsize    : RDD[(ConceptID, Int)]) : RDD[(DataIndex, ConceptID)] = {
    indexedData.map(_.swap).join(andconcepts).               // Find all activations
    map({case (s, v) => (v, 1)}).                            // Switch keys, prepare for count
    reduceByKey(_+_).filter(_._2 >= 2).                      // Number of activations, filter single
    map(x=>(x._1._2, (x._1._1, x._2))).                      // Change key
    join(andcsize).filter(x => x._2._1._2>=x._2._2).         // Keep only fully activated and concepts
    map(x=>(x._2._1._1, x._1))                               // Change format, remove (now) useless info
  }

  /** Find all direct OR concept activations among context counts. */
  def activateORConceptsFromCounts(contextCounts : RDD[(ConceptID, (ContextID, ContextCount))],
                                   orconcepts    : RDD[(ConceptID, ConceptID)]) :
    RDD[(ConceptID, (ContextID, ContextCount))] = orconcepts.join(contextCounts).values.distinct

  /** Find all direct AND concept activations among context counts. */
  def activateANDConceptsFromCounts(contextCounts : RDD[(ConceptID, (ContextID, ContextCount))],
                                    andconcepts   : RDD[(ConceptID, ConceptID)],
                                    andcsize      : RDD[(ConceptID, Int)]) :
  RDD[(ConceptID, (ContextID, ContextCount))] = {
    andconcepts.join(contextCounts).                                           // Find all activations
    map{case (cp, (acp, (cx, ct))) => ((acp, cx, ct), 1L)}.reduceByKey(_ + _). // Count unique activations
    filter(_._2 >= 2).                                                         // Discard singletons
    map{case ((acp, cx, ct), act) => (acp, (cx, ct, act))}.                    // Change key
    join(andcsize).filter{case (acp, ((cx, ct, act), sz)) => act >= sz}.       // Keep only fully activated and concepts
    mapValues{case ((cx, ct, act), sz) => (cx, ct)}                            // Change format, remove (now) useless info
  }

  /** Find all direct AND concept activations among context counts. Replace constituents with 
    * activated AND concepts. Return AND activations and remaining prior activations.
    */
  def projectOntoANDConceptsFromCounts(contextCounts : RDD[(ConceptID, (ContextID, ContextCount))],
                                       andconcepts   : RDD[(ConceptID, ConceptID)],
                                       andcsize      : RDD[(ConceptID, Int)]) :
  RDD[(ConceptID, (ContextID, ContextCount))] = {    
    val andActivations = activateANDConceptsFromCounts(contextCounts, andconcepts, andcsize)
    // Expand AND concept activations to possible constitutent activations, e.g.
    // (A, e) => (a, e), (b, e), (c, e) where A = {a, b, c}
    val constituentActivations = andconcepts.map(_.swap).join(andActivations).values
    // Remove constituent activations where parent AND concepts are activated, and merge with AND activations
    contextCounts.subtractByKey(constituentActivations).union(andActivations)
  }

  def projectOntoORConceptsFromCounts(contextCounts : RDD[(ConceptID, (ContextID, ContextCount))],
                                      orconcepts    : RDD[(ConceptID, ConceptID)]) :
    RDD[(ConceptID, (ContextID, ContextCount))] =
    contextCounts.leftOuterJoin(orconcepts).map{case (ct, ((cx, cxt), hct)) => (hct.getOrElse(ct), (cx, cxt))}.distinct

  /** Maps concepts in indexed data onto the concepts' corresponding OR concepts.
    *
    * For each concept i that belongs to an OR concept c, replace i with c in indexed data.
    * Assumes that a concept belongs to at most one OR concept.
    *
    * @param indexedData (d,i) tuples with concept i and data instance d
    * @param orconcepts (i,c) tuples, where concept i belongs to OR concept c
    * @return (d,i) tuples where d is a data instance and i is either a concept or an OR concept
    */
  def projectOntoORConcepts(indexedData : RDD[(DataIndex, ConceptID)],
                            orconcepts  : RDD[(ConceptID, ConceptID)]) : RDD[(DataIndex, ConceptID)] =
    indexedData.map{case (d,c)=>(c,d)}.leftOuterJoin(orconcepts).map{case (c,(d,cc))=>(d,cc.getOrElse(c))}.distinct

  /** Find all higher order concept activations for a concept data set.
  *
  * Find all higher-order concept activations for in an indexed concept data set. Returns an
  * RDD consisting of all active concepts, both fundamental and higher-order, as pairs of
  * concept id and example id:s. The number of iterations to find higher order concept activations
  * can be limited, default value is 10.
  */
  @tailrec
  def activateHigherOrder(indexedData : RDD[(DataIndex, ConceptID)],
                          orconcepts  : RDD[(ConceptID, ConceptID)],
                          andconcepts : RDD[(ConceptID, ConceptID)],
                          andcsize    : RDD[(ConceptID, Int)],
                          maxIterations : Int = 10, iteration : Int = 1) : RDD[(DataIndex, ConceptID)] = {
    val oract  = activateORConcepts(indexedData, orconcepts)
    val andact = activateANDConcepts(indexedData, andconcepts, andcsize)
    val oractsize = oract.count()
    val andactsize = andact.count()
    if (oractsize <= 0 && andactsize <= 0) indexedData
    else if (iteration >= maxIterations) indexedData ++ oract ++ andact
    else {
      val cioract = oract.map(x=>(x._2,x._1))
      val ciandact = andact.map(x=>(x._2,x._1))
      val remainingor  = oract.map(x=>(x._2,x._1)).subtractByKey(cioract).map(x=>(x._2,x._1))
      val remainingand = andact.map(x=>(x._2,x._1)).subtractByKey(ciandact).map(x=>(x._2,x._1))
      activateHigherOrder(indexedData ++ oract ++ andact,
                          remainingor, remainingand, andcsize,
                          maxIterations, iteration + 1)
    }
  }

  // TODO: Rename so that we do not have two classes with the same name
  /** Storage class for concept data */
  case class ConceptData(correlationGraph  : EdgeData,
                         similarityGraph   : RDD[(ConceptID, ConceptID, Double)],
                         andConcepts       : RDD[(ConceptID, ConceptID)],
                         orConcepts        : RDD[(ConceptID, ConceptID)])

  /** Find higher order AND and OR concepts from data.
   *
   * Higher order concepts are in conjunctive normal form (CNF), i.e. as
   * conjunctions of clauses such as (a OR b OR c) AND (d OR c).
   * Returns (1) the full correlation graph, (2) the full similarity graph, (3) CNF formula
   * ids keyed by clause ids, and (4) clause ids keyed by object (atom) ids.
   * Note that the input data set initialData must contain unique examples, i.e. each
   * (example id, concept id) combination is only allowed to occur once in the data set.
   *
   * TODO 1: Look over caching - currently it's not done systematically.
   * TODO 2: Possibly use TreeSet rather than Set to keep memory footprint down.
   * TODO 3: Discard self-edges. Since we are now working with CNFs, we need to
   *         reformulate what we mean by this.
   * 
   * NOTE: Not up-to-date.
   */
  def findAndOrConceptsCNF(sc                    : SparkContext,
                        nrExamples            : Long,
                        initialData           : RDD[(DataIndex, ConceptID)],
                        correlationFunction   : (Long, Long, Long, Long) => (Double, Double),
                        clusteringAlgo        : ClusteringAlgorithm,
                        clusteringParams      : ParameterMap,
                        nrIterations          : Int  = 1,
                        conceptsMinCount      : Long = 0,
                        minCorrelation        : Option[Double] = None,
                        minSimilarity         : Option[Double] = None,
                        maxInDegree           : Option[Long] = None,
                        cacheLevel            : storage.StorageLevel = MEMORY_AND_DISK) :
  ConceptData = {

    var iteration               : Int  = 0
    var clauseIdShift           : Long = orConceptsStartID
    var cnfIdShift              : Long = andConceptsStartID
    var converged               : Boolean = false
    var totalData               : RDD[(DataIndex, ConceptID)]          = initialData
    var currentData             : RDD[(DataIndex, ConceptID)]          = initialData
    var totalSimGraph           : RDD[(Long, Long, Double)]            = sc.emptyRDD
    var clauses                 : RDD[(ConceptID, ConceptID)]          = sc.emptyRDD
    var conjunctions            : RDD[(ConceptID, ConceptID)]          = sc.emptyRDD
    var totalCNFs               : RDD[HigherOrderConcept]              = sc.emptyRDD
    var indexedCNFs             : RDD[(ConceptID, HigherOrderConcept)] = sc.emptyRDD
    var totalClauses            : RDD[Set[ConceptID]]                  = sc.emptyRDD
    var totalActivatedClauses   : RDD[(ConceptID, ConceptID)]          = sc.emptyRDD
    var totalAtomToClause       : RDD[(ConceptID, ConceptID)]          = sc.emptyRDD
    var totalClauseToCNF        : RDD[(ConceptID, ConceptID)]          = sc.emptyRDD
    var totalClauseIds          : RDD[(Set[ConceptID], ConceptID)]     = sc.emptyRDD
    var totalCorrGraph          : EdgeData                             = EdgeData(sc)

    while(iteration < nrIterations && !converged) {
      // Build correlation graph
      val counts       = incrementalPairwiseCounts(totalData, currentData, conceptsMinCount)
      val correlations = counts.mapValues{case (cij,ci,cj)=> correlationFunction(cij,ci,cj,nrExamples)}
      val corrEdges    = correlations.flatMap{case ((i,j),(coij,coji))=>Iterable((i,j,coij),(j,i,coji))}
      val corrGraph    = EdgeData(corrEdges).persist(cacheLevel)

      // Prune correlation graph w r t edge weights and indegrees
      val prunedCorrGraph = {
        val initialPrunedCorrGraph = {
          if (minCorrelation.isDefined) pruneEdgesByWeight(corrGraph, minCorrelation.get)
          else corrGraph
        }
        if (maxInDegree.isDefined) pruneEdgesByInDegree(initialPrunedCorrGraph, maxInDegree.get)
        else initialPrunedCorrGraph
      }

      // Transform to similarity graph and prune w r t edge weights
      val prunedSimGraph = {
        val simGraph = correlationsToSimilarities(prunedCorrGraph).persist(cacheLevel)
        if(minSimilarity.isDefined) simGraph.filter{case (i,j,w) => w > minSimilarity.get}
        else simGraph
      }

      // Merge pruned graphs with total graphs
      val newTotalCorrGraph = totalCorrGraph.merge(prunedCorrGraph).persist(cacheLevel)
      val newTotalSimGraph  = totalSimGraph.union(prunedSimGraph).persist(cacheLevel)

      // TODO: Check that this works as intended.
      totalCorrGraph.unpersist()
      totalSimGraph.unpersist()
      totalCorrGraph = newTotalCorrGraph
      totalSimGraph  = newTotalSimGraph

      // Initialization
      // NOTE: We do this here rather than outside the loop so that we do not store
      // vertices (atoms) that lack edges after the first pruning.
      if(iteration == 0) {
        val atoms = prunedCorrGraph.weights.flatMap { case (i, j, coij) => Iterable(i, j) }.distinct().persist(cacheLevel)
        totalCNFs = atoms.map(i => HigherOrderConcept(i,Atom(i))).persist(cacheLevel)
        totalClauses = atoms.map(Set(_)).persist(cacheLevel)
        totalClauseIds = totalClauses.zipWithIndex().mapValues(_ + clauseIdShift).persist(cacheLevel)
        totalAtomToClause = totalClauseIds.map{case (c,i) => (c.head,i)}
        totalActivatedClauses = activateORConcepts(initialData, totalAtomToClause)
        clauseIdShift = clauseIdShift + totalClauses.count
        indexedCNFs = totalCNFs.map(c => (c.id,c))
        // NOTE: Not necessary to initialize totalClauseToCNF here
      }

      // Cluster vertices
      // NOTE: If we do not cache here, concepts will be re-evaluated with new concept start id
      // shifts when added to total concepts.
      val andClusters = clusteringAlgo.cluster(prunedCorrGraph.weights, clusteringParams).
                        mapValues{c => c + cnfIdShift}.persist(cacheLevel)
      if(!andClusters.isEmpty) cnfIdShift = andClusters.values.max + 1
      val orClusters = clusteringAlgo.cluster(prunedSimGraph, clusteringParams).
                       mapValues{c => c + cnfIdShift}.persist(cacheLevel)
      if(!orClusters.isEmpty) cnfIdShift = orClusters.values.max + 1

      // Need to unpersist manually since we use own method
      corrGraph.unpersist()

      // We are done if no more concepts are found
      if(andClusters.isEmpty && orClusters.isEmpty)
        converged = true
      else{
        // Transform clusters to CNF formulas
        def buildFormulas(clusters: RDD[(ConceptID, ConceptID)], and: Boolean) =
          clusters.join(indexedCNFs).map{case (i,(cc,c)) => (cc,c.formula)}.
          reduceByKey(if(and) And(_,_) else Or(_,_)).  // Fold formulas
          map{case (cc,f) => HigherOrderConcept(cc,f)} // Convert to minimal CNF

        val andCNFs = buildFormulas(andClusters, and = true)
        val orCNFs  = buildFormulas(orClusters, and = false)

        // Merge formulas and clauses, respectively, and discard redundant and prior ones
        val newCNFs = andCNFs.union(orCNFs).distinct().subtract(totalCNFs)
        val newClauses = newCNFs.flatMap(_.cnf).distinct().subtract(totalClauses)
        totalCNFs = totalCNFs.union(newCNFs)
        totalClauses = totalClauses.union(newClauses)
        indexedCNFs = indexedCNFs.union(newCNFs.map(c => (c.id,c)))

        // Assign ids to new clauses and update id shift
        val newClauseIds = newClauses.zipWithIndex().mapValues(_ + clauseIdShift).persist(cacheLevel)
        totalClauseIds = totalClauseIds.union(newClauseIds)
        // NOTE: Beware so that lazy evaluations do not cause problems here
        clauseIdShift = clauseIdShift + newClauses.count

        // Atom - clause id pairs for new clauses
        val newAtomToClause = newClauseIds.flatMap{case (c,i) => c.map(j => (j,i))}
        totalAtomToClause = totalAtomToClause.union(newAtomToClause)

        // Activate new clauses and add to prior activations
        // NOTE: Use the initial data since clauses are flat, i.e. consist only of atoms
        val newActivatedClauses = activateORConcepts(initialData, newAtomToClause)
        totalActivatedClauses = totalActivatedClauses.union(newActivatedClauses)

        // Clause id - CNF id pairs for new CNFs
        val newClauseToCNF = newCNFs.flatMap(c => c.cnf.map(clause => (clause, c.id))).
                             join(totalClauseIds).map{case (clause, (cnfId, clauseId)) => (clauseId, cnfId)}
        totalClauseToCNF = totalClauseToCNF.union(newClauseToCNF)

        // Number of clauses per conjunction
        val newCNFLengths = newCNFs.map(c => (c.id, c.cnf.size))

        // Activated CNFs
        val activatedNewCNFs = activateANDConcepts(totalActivatedClauses, newClauseToCNF, newCNFLengths)

        currentData = activatedNewCNFs
        totalData = totalData.union(currentData)

        iteration += 1
      }
    }

    ConceptData(totalCorrGraph, totalSimGraph, totalClauseToCNF, totalAtomToClause)
  }

  /** Find higher order AND and OR concepts from data.
   *
   * Returns higher order concepts and related data. Note that the input data set initialData
   * must contain unique examples, i.e. each (example id, concept id) combination is only allowed
   * to occur once in the data set.
   *
   * NOTE: Not up-to-date.
   */
  def findAndOrConcepts(sc                    : SparkContext,
                        nrExamples            : Long,
                        initialData           : RDD[(DataIndex, ConceptID)],
                        correlationFunction   : (Long, Long, Long, Long) => (Double, Double),
                        clusteringAlgo        : ClusteringAlgorithm,
                        clusteringParams      : ParameterMap,
                        nrIterations          : Int  = 1,
                        conceptsMinCount      : Long = 0,
                        minCorrelation        : Option[Double] = None,
                        minSimilarity         : Option[Double] = None,
                        maxInDegree           : Option[Long] = None,
                        conceptThreshold      : Double,
                        cacheLevel            : storage.StorageLevel = MEMORY_AND_DISK) :
  ConceptData = {
    var totalData   = initialData
    var currentData = initialData
    var currentAndConceptStartID = andConceptsStartID
    var currentOrConceptStartID  = orConceptsStartID
    var iteration     = 1

    var totalAndConcepts : RDD[(ConceptID, ConceptID)] = sc.emptyRDD
    var totalOrConcepts  : RDD[(ConceptID, ConceptID)] = sc.emptyRDD
    var correlationGraph : EdgeData = EdgeData(sc)
    correlationGraph.weights.setName("correlationEdges")
    var similarityGraph : EdgeData = EdgeData(sc)
    similarityGraph.weights.setName("similarityEdges")

    // For debugging - remove later
    val printInfo = false

    if(printInfo) println("#examples: " + initialData.count.toString)

    def buildCorrelationGraph(): EdgeData = {
      val counts = incrementalPairwiseCounts(totalData, currentData, conceptsMinCount)
      val correlations = counts.mapValues{case (cij,ci,cj)=> correlationFunction(cij,ci,cj,nrExamples)}
      val corrEdges = correlations.flatMap{case ((i,j),(coij,coji))=>Iterable((i,j,coij),(j,i,coji))}.
                                   filter(c => c._3 > minCorrelation.get)
      val corrGraph = EdgeData(corrEdges)
      val prunedCorrGraph = {
        val initialPrunedCorrGraph = {
          if (minCorrelation.isDefined) pruneEdgesByWeight(corrGraph, minCorrelation.get)
          else corrGraph
        }
        if (maxInDegree.isDefined) pruneEdgesByInDegree(initialPrunedCorrGraph, maxInDegree.get)
        else initialPrunedCorrGraph
      }
      prunedCorrGraph
    }

    // Build initial correlation graph
    correlationGraph = buildCorrelationGraph()

    if(printInfo) println("correlationGraph: " + correlationGraph.weights.count.toString)

    // Iterate a given number of times or until no more concepts are found
    while(iteration <= nrIterations) {

      // Transform to similarity graph and prune w r t edge weights
      similarityGraph = {
        val s = EdgeData(correlationsToSimilarities(correlationGraph))
        if(minSimilarity.isDefined) pruneEdgesByWeight(s, minSimilarity.get)
        else s
      }

      if(printInfo) println("similarityGraph: " + similarityGraph.weights.count.toString)

      // Get candidate concepts
      val candidateAndConcepts = clusteringAlgo.cluster(correlationGraph.weights, clusteringParams).
                        mapValues{c => c + currentAndConceptStartID}.persist(cacheLevel).setName("andCandidates")
      val candidateOrConcepts = clusteringAlgo.cluster(similarityGraph.weights, clusteringParams).
                       mapValues{c => c + currentOrConceptStartID}.persist(cacheLevel).setName("orCandidates")

      val andConceptLengths = candidateAndConcepts.map{case (i,c)=>(c,1)}.reduceByKey(_+_).setName("andLength")

      if(printInfo) {
        println("candidateAndConcepts: " + candidateAndConcepts.count.toString)
        println("candidateOrConcepts: " + candidateOrConcepts.count.toString)
      }

      // Activate candidate concepts
      currentData = activateANDConcepts(currentData, candidateAndConcepts, andConceptLengths).
                    union(activateORConcepts(currentData, candidateOrConcepts)).setName("currentData")

      // Build candidate correlation graph
      val candidateCorrelationGraph = buildCorrelationGraph()

//      val weights = candidateCorrelationGraph.weights.map{case (i,j,w) => w}

      //println("\ncandidateCorrelationGraph range: " + weights.min.toString + ", " + weights.max.toString)

      // Min id of candidate concepts
      val minStartID = min(currentAndConceptStartID, currentOrConceptStartID)

      // Discard superfluous new concepts that are strongly correlated with previous concepts
      val validEdges = candidateCorrelationGraph.weights.filter{case (i,j,w) =>
                            !(w > conceptThreshold && (i >= minStartID || j >= minStartID))}
      val validConcepts = validEdges.flatMap{case (i,j,w) => Iterable(i,j)}.
                                     distinct.map(i => (i,None)).
                                     persist(cacheLevel)

      if(printInfo) {
        println("\ncandidateCorrelationGraph:")
        candidateCorrelationGraph.weights.take(20).foreach(println)
        println("\ncandidateAndConcepts:")
        candidateAndConcepts.take(20).foreach(println)
        println("\ncandidateOrConcepts:")
        candidateOrConcepts.take(20).foreach(println)
        println("\nvalidConcepts:")
        validConcepts.take(20).foreach(println)
        println("validEdges: " + validEdges.count.toString)
        println("validConcepts: " + validConcepts.count.toString)
      }

      // Update correlation graph with valid concepts
      correlationGraph = EdgeData(validEdges)

      // Discard invalid concepts
      val andConcepts = candidateAndConcepts.map(_.swap).join(validConcepts).map{case (c,(i,None)) => (i,c)}.setName("andConcepts")
      val orConcepts = candidateOrConcepts.map(_.swap).join(validConcepts).map{case (c,(i,None)) => (i,c)}.setName("orConcepts")
      currentData = currentData.map(_.swap).join(validConcepts).map{case (c,(d,None)) => (d,c)}

      if(printInfo) {
        println("andConcepts: " + andConcepts.count.toString)
        println("orConcepts: " + orConcepts.count.toString)
      }

      // Update ID shifts
      currentAndConceptStartID = currentAndConceptStartID + {if(andConcepts.isEmpty) 0 else andConcepts.values.max}
      currentOrConceptStartID = currentOrConceptStartID + {if(orConcepts.isEmpty) 0 else orConcepts.values.max}

      totalData = totalData.union(currentData).setName("totalData")
      totalAndConcepts = totalAndConcepts.union(andConcepts).setName("totalAndConcepts")
      totalOrConcepts  = totalOrConcepts.union(orConcepts).setName("totalOrConcepts")

      iteration += 1
    }

    ConceptData(correlationGraph, similarityGraph.weights, totalAndConcepts, totalOrConcepts)
  }

  def findAndOrConceptsFromCounts(sc                    : SparkContext,
                                  nrExamples            : Long,
                                  initialContextCounts  : RDD[(ConceptID, (ContextID, ContextCount))],
                                  correlationFunction   : (Long, Long, Long, Long) => (Double, Double),
                                  clusteringAlgo        : ClusteringAlgorithm,
                                  clusteringParams      : ParameterMap,
                                  nrIterations          : Int  = 1,
                                  minConceptCount       : Long,
                                  minContextCount       : Long,
                                  minCorrelation        : Option[Double] = None,
                                  minSimilarity         : Option[Double] = None,
                                  maxInDegree           : Option[Long] = None,
                                  conceptThreshold      : Double,
                                  cacheLevel            : storage.StorageLevel = MEMORY_AND_DISK) :
  ConceptData = {
    var iteration = 1
    var totalContextCounts   = initialContextCounts
    var currentContextCounts = initialContextCounts
    var currentAndConceptStartID = andConceptsStartID
    var currentOrConceptStartID  = orConceptsStartID    
    var totalAndConcepts : RDD[(ConceptID, ConceptID)] = sc.emptyRDD
    var totalOrConcepts  : RDD[(ConceptID, ConceptID)] = sc.emptyRDD
    var correlationGraph : EdgeData = EdgeData(sc)
    var similarityGraph : EdgeData = EdgeData(sc)
    var totalCorrelations : RDD[(ConceptID, ConceptID, Weight)] = sc.emptyRDD
    var totalSimilarities : RDD[(ConceptID, ConceptID, Weight)] = sc.emptyRDD
    // Edges between higher-order concepts and their constituents; add dummy Option to enable subtractByKey
    var selfEdges : RDD[((ConceptID, ConceptID), None.type)] = sc.emptyRDD
    // TODO: Add MinClusterStrength to input parameters
    val andClusteringParams = new ParameterMap().add(Undirect, false).
                                                 add(MinWeight, minCorrelation.get).add(MinClusterStrength, 0.8)
    val orClusteringParams = new ParameterMap().add(Undirect, true). // Add (j, i) edges for (i, j) edges
                                                add(MinWeight, minSimilarity.get).add(MinClusterStrength, 0.8)    
    // Current minimum id of (higher-order) concepts
    var minStartID = 0L
    // To facilitate monitoring and debugging
    correlationGraph.weights.setName("correlationEdges")
    similarityGraph.weights.setName("similarityEdges")

    // For debugging - remove later
    // Use e.g. in conjunction with ConceptsTest
    val printInfo = false
    def printRDD(label : String, rdd : RDD[_]) : Unit = {
      if(printInfo) {
        println(label + ":")
        rdd.take(50).foreach(println)
        println
      }
    }

    printRDD("initialContextCounts", initialContextCounts)

    def buildCorrelationGraph() : EdgeData = {     
      val counts = incrementalPairwiseCountsFromCounts(totalContextCounts, currentContextCounts,
                                                       minConceptCount, minContextCount)
      // Discard counts between higher-order concepts and their own constituents
      val keptCounts = 

    if(selfEdges.isEmpty()) counts else counts.subtractByKey(selfEdges)
      val correlations = keptCounts.mapValues{case (cij,ci,cj)=> correlationFunction(cij,ci,cj,nrExamples)}
      val corrEdges = correlations.flatMap{case ((i,j),(coij,coji))=>Iterable((i,j,coij),(j,i,coji))}.
                                   filter(c => math.abs(c._3) > minCorrelation.get)
      val corrGraph = EdgeData(corrEdges)
      val prunedCorrGraph = {
        val initialPrunedCorrGraph = {
          if (minCorrelation.isDefined) pruneEdgesByWeight(corrGraph, minCorrelation.get)
          else corrGraph
        }
        if (maxInDegree.isDefined) pruneEdgesByInDegree(initialPrunedCorrGraph, maxInDegree.get)
        else initialPrunedCorrGraph
      }

      // For debugging - remove later
      printRDD("counts", counts)
      printRDD("selfEdges", selfEdges)
      printRDD("keptCounts", keptCounts)

      prunedCorrGraph
    }

    // Build initial correlation graph
    correlationGraph = buildCorrelationGraph().persist(cacheLevel)
    totalCorrelations = totalCorrelations.union(correlationGraph.weights)

    printRDD("correlationGraph", correlationGraph.weights)

    // Iterate a given number of times
    while(iteration <= nrIterations) {

      // For debugging - remove later
      if(printInfo) {
        println("\n------------------------------")
        println("Iteration " + iteration.toString)
      }

      // Transform to similarity graph and prune w r t edge weights
      similarityGraph = {
        val simEdges = correlationsToSimilarities(correlationGraph).
                       // Discard edges between previous concepts
                       filter{case (i, j, s) => !(i < minStartID && j < minStartID)}
        val prunedSimEdges = if(selfEdges.isEmpty()) simEdges
                             else simEdges.map{case (i, j, s) => ((i, j), s)}.
                                  // Discard edges between higher-order concepts and their constituents
                                  subtractByKey(selfEdges).map{case ((i, j), s) => (i, j, s)}
        val s = EdgeData(prunedSimEdges)
        if(minSimilarity.isDefined) pruneEdgesByWeight(s, minSimilarity.get) else s
      }.persist(cacheLevel)

      totalSimilarities = totalSimilarities.union(similarityGraph.weights)

      printRDD("similarityGraph", similarityGraph.weights)
      printRDD("totalSimilarities", totalSimilarities)  

      // Get candidate concepts
      // NOTE: Inter-edges of prior concepts are not used here
      val candidateAndConcepts = clusteringAlgo.cluster(correlationGraph.weights, clusteringParams ++ andClusteringParams).
                        mapValues{c => c + currentAndConceptStartID}.persist(cacheLevel).setName("andCandidates")
      val candidateOrConcepts = clusteringAlgo.cluster(similarityGraph.weights, clusteringParams ++ orClusteringParams).
                        mapValues{c => c + currentOrConceptStartID}.persist(cacheLevel).setName("orCandidates")

      printRDD("candidateAndConcepts", candidateAndConcepts)
      printRDD("candidateOrConcepts", candidateOrConcepts)

      // ((c, h), None) pairs, where concept c is a constituent of higher-order concept h
      selfEdges = candidateAndConcepts.union(candidateOrConcepts).
                                       // Ensure that sorted (not the case when an AND concept contains an OR concept)
                                       map{case (i, j) => if(i < j) (i, j) else (j, i)}.
                                       map((_, None)). // Add dummy value to enable subtract by key
                                       persist(cacheLevel)

      val andConceptLengths = candidateAndConcepts.map{case (i,c)=>(c,1)}.reduceByKey(_+_).setName("andLength")
      val andActivations = activateANDConceptsFromCounts(totalContextCounts, candidateAndConcepts, andConceptLengths)
      val orActivations = activateORConceptsFromCounts(totalContextCounts, candidateOrConcepts)

      printRDD("andConceptLengths", andConceptLengths)
      printRDD("andActivations", andActivations)
      printRDD("orActivations", orActivations)

      // Activate candidate concepts
      // NOTE: Set name outside loop instead?
      currentContextCounts = andActivations.union(orActivations).persist(cacheLevel).setName("currentContextCounts")

      // Build candidate correlation graph
      val candidateCorrelationGraph = buildCorrelationGraph()

      printRDD("candidateCorrelationGraph", candidateCorrelationGraph.weights)

      // Min id of candidate concepts
      minStartID = min(currentAndConceptStartID, currentOrConceptStartID)

      // True if i is the index of a new higher-order concept
      def isNewConcept(index : Long): Boolean = 
        index >= currentOrConceptStartID || (index >= currentAndConceptStartID && index < orConceptsStartID)

      // Discard superfluous new concepts that are strongly correlated with previous concepts      
      val validEdges = candidateCorrelationGraph.weights.filter{case (i, j, w) =>
                            // Note that at most one concept is from previous iterations
                            !(w > conceptThreshold && (!isNewConcept(i) || !isNewConcept(j)))}                                  
      val validConcepts = validEdges.flatMap{case (i, j, w) => Iterable(i, j)}.
                                     distinct.map(i => (i, None)).
                                     persist(cacheLevel)

      printRDD("validEdges", validEdges)
      printRDD("validConcepts", validConcepts)                                     

      // Set correlation graph to only contain valid concepts
      correlationGraph = EdgeData(validEdges).persist(cacheLevel)
      totalCorrelations = totalCorrelations.union(validEdges)

      printRDD("correlationGraph", correlationGraph.weights)
      printRDD("totalCorrelations", totalCorrelations)

      // Discard invalid concepts
      val andConcepts = candidateAndConcepts.map(_.swap).join(validConcepts).
                                             map{case (c, (i, None)) => (i, c)}.setName("andConcepts")
      val orConcepts = candidateOrConcepts.map(_.swap).join(validConcepts).
                                           map{case (c, (i, None)) => (i, c)}.setName("orConcepts")
      currentContextCounts = currentContextCounts.join(validConcepts).map{case (c, (d, None)) => (c, d)}

      printRDD("andConcepts", andConcepts)
      printRDD("orConcepts", orConcepts)  

      // Update here so that we do not subtract *canditate* concepts from similarity graph in the next iteration
      //selfEdges.unpersist()
      selfEdges = andConcepts.union(orConcepts).
                              map{case (i, j) => if(i < j) (i, j) else (j, i)}.
                              map((_, None)).
                              persist(cacheLevel)

      // Update ID shifts
      currentAndConceptStartID = if(andConcepts.isEmpty) currentAndConceptStartID else andConcepts.values.max + 1
      currentOrConceptStartID =  if(orConcepts.isEmpty) currentOrConceptStartID else orConcepts.values.max + 1

      totalContextCounts = totalContextCounts.union(currentContextCounts).setName("totalContextCounts")
      totalAndConcepts = totalAndConcepts.union(andConcepts).setName("totalAndConcepts")
      totalOrConcepts  = totalOrConcepts.union(orConcepts).setName("totalOrConcepts")

      printRDD("totalContextCounts", totalContextCounts)

      iteration += 1      
    }

    ConceptData(EdgeData(totalCorrelations), totalSimilarities, totalAndConcepts, totalOrConcepts)
  }

  /** Test: Replace constituents with higher-order concepts and cluster with respect to all edges. */
  def findProjectedAndOrConceptsFromCounts(sc                    : SparkContext,
                                           nrExamples            : Long,
                                           initialContextCounts  : RDD[(ConceptID, (ContextID, ContextCount))],
                                           correlationFunction   : (Long, Long, Long, Long) => (Double, Double),
                                           clusteringAlgo        : ClusteringAlgorithm,
                                           clusteringParams      : ParameterMap,
                                           nrIterations          : Int  = 1,
                                           minConceptCount       : Long,
                                           minContextCount       : Long,
                                           minCorrelation        : Option[Double] = None,
                                           minSimilarity         : Option[Double] = None,
                                           maxInDegree           : Option[Long] = None,
                                           conceptThreshold      : Double,
                                           maxConceptRank        : Int = 5000,
                                           logSimilarities       : Boolean = false,
                                           findAndConcepts       : Boolean = true,
                                           findOrConcepts        : Boolean = true,                                          
                                           cacheLevel            : storage.StorageLevel = MEMORY_AND_DISK) :
  ConceptData = {

    var iteration = 1
    var converged = false
    var activations = pruneContextCountsByRank(initialContextCounts, maxConceptRank)
    var currentAndConceptStartID = andConceptsStartID
    var currentOrConceptStartID  = orConceptsStartID
    var totalAndConcepts : RDD[(ConceptID, ConceptID)] = sc.emptyRDD
    var totalOrConcepts  : RDD[(ConceptID, ConceptID)] = sc.emptyRDD
    var correlationGraph : RDD[(ConceptID, ConceptID, Weight)] = sc.emptyRDD
    var similarityGraph  : RDD[(ConceptID, ConceptID, Weight)] = sc.emptyRDD
    var totalSimilarityGraph  : RDD[(ConceptID, ConceptID, Weight)] = sc.emptyRDD
    
    val andClusteringParams = new ParameterMap().add(Undirect, false)
    val orClusteringParams = new ParameterMap().add(Undirect, true)

    // Builds and prunes correlation graph
    def correlations() = {
      // NOTE: We recalculate correlations between first-order concepts over iterations
      val counts = incrementalPairwiseCountsFromCounts(activations, activations, minConceptCount, minContextCount)
      val corrEdges = counts.mapValues{case (cij, ci, cj)=> correlationFunction(cij, ci, cj, nrExamples)}.
                      flatMap{case ((i,j),(coij,coji))=>Iterable((i,j,coij),(j,i,coji))}                      
      val corrGraph = EdgeData(corrEdges)     
      val prunedCorrGraph = {
        val initialPrunedCorrGraph = {
          if (minCorrelation.isDefined) pruneEdgesByWeight(corrGraph, minCorrelation.get)
          else corrGraph
        }
        if (maxInDegree.isDefined) pruneEdgesByInDegree(initialPrunedCorrGraph, maxInDegree.get)
        else initialPrunedCorrGraph
      }
      prunedCorrGraph.weights
    }

    def similarities() = {
        val s = correlationsToSimilarities(EdgeData(correlationGraph))            
        val p = if(minSimilarity.isDefined) s.filter{case (i, j, s) => s >= minSimilarity.get} else s
        if(logSimilarities) p.map{case (i, j, s) => (i, j, log(s)/log(10))} else p
    }

    while(iteration <= nrIterations && !converged) {
      correlationGraph = correlations().persist(cacheLevel)

      if(findAndConcepts) {
        val andConcepts = clusteringAlgo.cluster(correlationGraph, clusteringParams ++ andClusteringParams).
                                         mapValues{c => c + currentAndConceptStartID}.persist(cacheLevel)
        totalAndConcepts = totalAndConcepts.union(andConcepts)
        currentAndConceptStartID = if(andConcepts.isEmpty) currentAndConceptStartID else andConcepts.values.max + 1
        val andConceptLengths = andConcepts.map{case (i,c)=>(c,1)}.reduceByKey(_+_)
        activations = projectOntoANDConceptsFromCounts(activations, andConcepts, andConceptLengths)
        converged = andConcepts.isEmpty
      }
      
      if(findOrConcepts) {
        similarityGraph = similarities().persist(cacheLevel)
        totalSimilarityGraph = totalSimilarityGraph.union(similarityGraph)
        val orConcepts = clusteringAlgo.cluster(similarityGraph, clusteringParams ++ orClusteringParams).
                                        mapValues{c => c + currentOrConceptStartID}.persist(cacheLevel)
        totalOrConcepts  = totalOrConcepts.union(orConcepts)
        currentOrConceptStartID =  if(orConcepts.isEmpty) currentOrConceptStartID else orConcepts.values.max + 1
        activations = projectOntoORConceptsFromCounts(activations, orConcepts)
        converged = (!findAndConcepts || converged) && orConcepts.isEmpty
      }

      iteration += 1
    }

    ConceptData(EdgeData(correlationGraph), totalSimilarityGraph, totalAndConcepts, totalOrConcepts)
  }
}
