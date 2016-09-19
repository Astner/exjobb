
/*
 Copyright (C) 2016 Daniel Gillblad, Olof Görnerup, Theodoros Vasiloudis (dgi@sics.se,
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

package se.sics.concepts.experiments

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import play.api.libs.json._

import se.sics.concepts.main.Actions._

import se.sics.concepts.clustering.ClusteringAlgorithm
import se.sics.concepts.clustering.ClusteringParameters._
import se.sics.concepts.graphs.Transformations._
import se.sics.concepts.higherorder.HigherOrder._
import se.sics.concepts.graphs.{Counts, ConceptIDs, EdgeData}
import se.sics.concepts.classification.ClassificationMLlib._

import ConceptIDs._
import Counts._

import se.sics.concepts.io.ConceptData
import se.sics.concepts.io.ConceptsIO._
import se.sics.concepts.util.ConceptsUtil._
import se.sics.concepts.util.Statistics._
import se.sics.concepts.util.ParameterMap
import se.sics.concepts.classification._

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import scala.math.abs

/** Various experiments */
object Experiments {

  /** Measures various graph properties for different indegree thresholds. */
  def indegreeThresholdVsGraphProperties(cd : ConceptData,
                                         source : String,                                         
                                         correlationFunction : (Long, Long, Long, Long) => (Double, Double),
                                         corrFuncLabel : String,
                                         minConceptCount : Long,
                                         minContextCount : Long,
                                         minCorrelation : Double, 
                                         minSimilarity : Double,
                                         maxDegreeMin: Long,
                                         maxDegreeMax: Long,
                                         maxDegreeDelta: Long) : Unit = {  
                                          
    // Build correlation graph
    val correlations = EdgeData(contextsToCorrelations(cd, minConceptCount, 
                                minContextCount, correlationFunction, minCorrelation))
    
    // Measure properties for different indegree thresholds
    val result = {maxDegreeMin to maxDegreeMax by maxDegreeDelta}.map{case r =>
      val correlationGraph = pruneEdgesByInDegree(correlations, r)
      JsObject(Seq(
          "indegree threshold" -> JsNumber(r),
          "correlation graph" -> graphMeasures(correlationGraph.weights),
          "similarity graph" -> graphMeasures(correlationsToSimilarities(correlationGraph).
                                                filter{case (i, j, w) => w >= minSimilarity}))
      )
    }

    // Set experiment id to current timestamp
    val experimentId = System.currentTimeMillis/1000

    val parameters = JsObject(Seq(
      "source" -> JsString(source),
      "correlation function" -> JsString(corrFuncLabel),
      "min concept count" -> JsNumber(minConceptCount),
      "min context count" -> JsNumber(minContextCount),
      "min abs correlation" -> JsNumber(minCorrelation),
      "min similarity" -> JsNumber(minSimilarity),
      "max indegree min" -> JsNumber(maxDegreeMin),
      "max indegree max" -> JsNumber(maxDegreeMax),
      "max indegree delta" -> JsNumber(maxDegreeDelta))
    )

    val output = JsObject(Seq(
        "id" -> JsNumber(experimentId), 
        "date" -> JsString((new java.util.Date).toString),
        "description" -> JsString("Various graph measures for different indegree thresholds."),
        "parameters" -> parameters,
        "result" -> JsArray(result))
    )

    // Save result as JSON
    writeToFile(cd.path.get + experimentId.toString + "-indegree-vs-graph-measures.json", Json.prettyPrint(output))
  }

  /** Measures various similarity graph properties for different similarity thresholds. */
  def similarityThresholdVsGraphProperties(cd : ConceptData,
                                           source : String,                                         
                                           correlationFunction : (Long, Long, Long, Long) => (Double, Double),
                                           corrFuncLabel : String,
                                           minConceptCount : Long,
                                           minContextCount : Long,
                                           minCorrelation : Double,
                                           maxDegree : Long,
                                           minSimilarityMin: Double,
                                           minSimilarityMax: Double,
                                           minSimilarityDelta: Double) : Unit = {

    // Build correlation graph and transform to similarity graph
    val correlations = EdgeData(contextsToCorrelations(cd, minConceptCount, minContextCount, correlationFunction, minCorrelation))
    val similarities = correlationsToSimilarities(pruneEdgesByInDegree(correlations, maxDegree))

    // Vary similarity threshold and measure similarity graph properties
    val result = {minSimilarityMin to minSimilarityMax by minSimilarityDelta}.map{case r =>
      println(r)
      JsObject(Seq(
          "similarity threshold" -> JsNumber(r),
          "similarity graph" -> graphMeasures(similarities.filter{case (i, j, w) => w >= r}))
      )
    }

    // Set experiment id to current timestamp
    val experimentId = System.currentTimeMillis/1000

    val parameters = JsObject(Seq(
      "source" -> JsString(source),
      "correlation function" -> JsString(corrFuncLabel),
      "min concept count" -> JsNumber(minConceptCount),
      "min context count" -> JsNumber(minContextCount),
      "min abs correlation" -> JsNumber(minCorrelation),
      "max indegree" -> JsNumber(maxDegree),
      "min similarity min" -> JsNumber(minSimilarityMin),
      "min similarity max" -> JsNumber(minSimilarityMax),
      "min similarity delta" -> JsNumber(minSimilarityDelta))
    )

    val output = JsObject(Seq(
        "id" -> JsNumber(experimentId), 
        "date" -> JsString((new java.util.Date).toString),
        "description" -> JsString("Various similarity graph measures for different similarity thresholds."),
        "parameters" -> parameters,
        "result" -> JsArray(result))
    )

    // Save result as JSON
    writeToFile(cd.path.get + experimentId.toString + "-similarity-threshold-vs-graph-measures.json", Json.prettyPrint(output))
  }

  /**  Evalutes how first-order OR concepts affects Naive Bayes classification. */
  def firstOrderORClassification(sc : SparkContext, 
                                 cd : ConceptData, 
                                 classificationFile : String,
                                 outputPath : String,
                                 testRatio : Double = 0.2,
                                 naiveBayesType : String = "bernoulli", // "multinomial" or "bernoulli"   
                                 cacheLevel : storage.StorageLevel = MEMORY_AND_DISK) : Unit = {
    
    // TODO: Add parameters to input rather than having them hardcoded
    // Indegree threshold range
    val rMin = 5
    val rMax = 255
    val rDelta = 50
    // Correlation threshold
    val minCorrelation = 4
    // Similarity threshold
    val minSimilarity = 0.1
    // Maximum number of clustering iterations
    val maxClusteringIterations = 16
    // Minimum cluster strength
    val minClusterStrength = 0.8
    // Minimum token count
    val minTokenCount = 10

    val objects = cd.concepts.get
    // Load classification examples
    val examples = loadExamples(sc, classificationFile).persist(cacheLevel)
    // ((class label, tokens), example index) pairs
    val indexedExamples = examples.zipWithIndex.persist(cacheLevel)
    // (class label, class index) pairs
    val classIndices = examples.keys.distinct.zipWithIndex
    // (example index, class index) pairs
    val examplesToClasses = indexedExamples.
          map{case ((classLabel, tokens), exampleIndex) => (classLabel, exampleIndex)}.
          join(classIndices).values

    // Discard infrequent tokens
    val tokensToExamplesAll = indexedExamples.
          flatMap{case ((classLabel, tokens), exampleIndex) => tokens.map(token => (token, exampleIndex))}
    val tokenCounts = tokensToExamplesAll.map{case (t, e) => (t, 1L)}.reduceByKey(_ + _)
    val tokensToExamples = tokensToExamplesAll.join(tokenCounts).
                           filter{case (t, (e, c)) => c >= minTokenCount}.
                           map{case (t, (e, c)) => (t, e)}
    // (token, example index) pairs
    //val tokensToExamples = indexedExamples.
    //      flatMap{case ((classLabel, tokens), exampleIndex) => tokens.map(token => (token, exampleIndex))}

    // Shift to avoid conflict with concept indices
    val tokenIndexShift = 400000000000L
    // (token, token index) pairs    
    val tokenIndices = tokensToExamples.keys.distinct.zipWithIndex.mapValues(_ + tokenIndexShift)
    // (token, (token index, example index)) pairs
    val tokensToIndicesAndExamples = tokenIndices.join(tokensToExamples)
    // (example index, token index) pairs
    
    // Keep only tokens that are in graph
    val examplesToObjects = tokensToIndicesAndExamples.join(objects.map(_.swap)).
                            map{case (t, ((ti, ei), oi)) => (ei, oi)}
    // Use token indices for tokens that are not among objects
    //val examplesToObjects = tokensToIndicesAndExamples.leftOuterJoin(objects.map(_.swap)).
    //                        map{case (t, ((ti, ei), oi)) => (ei, oi.getOrElse(ti))}
    
    val clusteringAlgo: ClusteringAlgorithm = selectClusteringAlgo("slpa")
    val clusteringParameters: ParameterMap = new ParameterMap().
      add(Iterations, maxClusteringIterations).
      add(Undirect, true).
      add(MinWeight, minSimilarity).add(MinClusterStrength, minClusterStrength)

    def naiveBayesResult(activations: RDD[(DataIndex, ConceptID)]) =
      naiveBayes(naiveBayesType, testRatio, activations, examplesToClasses, cacheLevel)
  
    val corrGraph = pruneEdgesByWeight(EdgeData(cd.correlations.get), minCorrelation).persist(cacheLevel)

    val result: Seq[JsObject] = {rMin to rMax by rDelta}.map{case r =>
      val prunedCorrGraph = pruneEdgesByInDegree(corrGraph, r)
      val similarities = correlationsToSimilarities(prunedCorrGraph).filter(_._3 >= minSimilarity)
      val orConcepts = clusteringAlgo.cluster(similarities, clusteringParameters).mapValues{c => c + orConceptsStartID}.persist(cacheLevel)
      val activatedConcepts = activateORConcepts(examplesToObjects, orConcepts)
      val addedActivations = activatedConcepts.union(examplesToObjects)
      val projectedActivations = projectOntoORConcepts(examplesToObjects, orConcepts)
      val metrics = JsObject(Seq("tokens only" -> naiveBayesResult(examplesToObjects),
                                 "or concepts only" -> naiveBayesResult(activatedConcepts), 
                                 "or concepts added" -> naiveBayesResult(addedActivations),
                                 "tokens projected to or concepts" -> naiveBayesResult(projectedActivations)))
      JsObject(Seq("indegree threshold" -> JsNumber(r), "metrics" -> metrics))
    }

    // Set experiment id to current timestamp
    val experimentId = System.currentTimeMillis/1000

    val parameters = JsObject(Seq(
      "classification file" -> JsString(classificationFile),
      "naive bayes type" -> JsString(naiveBayesType),
      "min correlation" -> JsNumber(minCorrelation),
      "min similarity" -> JsNumber(minSimilarity),
      "max clustering iterations" -> JsNumber(maxClusteringIterations),
      "min cluster strength" -> JsNumber(minClusterStrength),      
      "test ratio" -> JsNumber(testRatio)))

    val output = JsObject(Seq(
        "id" -> JsNumber(experimentId), 
        "date" -> JsString((new java.util.Date).toString),
        "description" -> JsString("Comparison of classification performance for raw tokens, raw tokens and OR concepts, and tokens projected onto OR concepts."),
        "parameters" -> parameters,
        "result" -> JsArray(result)))

    val outputFile = outputPath + experimentId.toString + "-naive-bayes-or-projection-experiment.json"
    writeToFile(outputFile, Json.prettyPrint(output))
    println("Result written to " + outputFile + ".")
  }

  /** Evaluates how the in-degree theshold affects approximation errors. */
  def indegreeThresholdVsSimilarityError(sc : SparkContext, cd : ConceptData, minCount : Long,
                                         correlationFunction : (Long, Long, Long, Long) => (Double, Double)) : Unit = {
    // Indegree threshold used as reference
    val rRef = 1000

    // Indegree thresholds
    val rMin = 100
    val rMax = 500
    val rDelta = 100

    // Number of histogram bins
    val numBins = 500

    // Set experiment id to current timestamp
    val experimentId: Long = System.currentTimeMillis/1000

    // Calculate reference correlations
    // NOTE: Do not store sums of discarded edge weights here
    val contexts = cd.contexts.get
    val nrExamples = cd.count.get
    val counts = incrementalPairwiseCounts(contexts, contexts, minCount)
    val cRef = {
      val all = counts.mapValues{case (cij,ci,cj)=> correlationFunction(cij,ci,cj,nrExamples)}.
                       flatMap{case ((i,j),(cij,cji))=>Iterable((i,j,cij),(j,i,cji))}
      val pruned = pruneEdgesByInDegree(EdgeData(all), rRef).weights
      EdgeData(pruned)
    }

    // Reference similarity graph
    val sRef = correlationsToSimilarities(cRef).map{case (i,j,s) => ((i,j),s)}.persist(MEMORY_ONLY)

    // For every indegree threshold
    for(r <- rMin to rMax by rDelta) {
      // Pruned correlation graph
      val cPruned = pruneEdgesByInDegree(cRef, r).persist(MEMORY_ONLY)
      // Remaining and discarded correlation sums per vertex
      val cSums = cPruned.sums.persist(MEMORY_ONLY)
      // Approximate similarity graph
      val sApprox = correlationsToSimilarities(cPruned).map{case (i,j,s) => ((i,j),s)}
      // Similarity errors
      val sError = sRef.leftOuterJoin(sApprox).map{case ((i,j),(s,ss)) => ((i,j),abs(s-ss.getOrElse(0D)))}.
                                               persist(MEMORY_ONLY)
      // Similarity errors and error bounds
      val sErrorBounds = sError.map{case ((i,j),e) => (i,(j,e))}.join(cSums).
                                map{case (i,((j,e),(ri,di))) => (j,(i,e,ri,di))}.join(cSums).
                                map{case (j,((i,e,ri,di),(rj,dj))) => (e,(di+dj)/(di+dj+ri+rj))}.
                                persist(MEMORY_ONLY)
      // Calculate statistics and store as JSON
      val eStat = JsStatistics(sErrorBounds.map{case (e,b) => e}, numBins)
      val bStat = JsStatistics(sErrorBounds.map{case (e,b) => b}, numBins)
      val rStat = JsStatistics(sErrorBounds.filter{case (e,b) => b != 0D}.map{case (e,b) => e/b}, numBins)
      // Write to file
      val allStat = JsObject(Seq("indegree threshold" -> JsNumber(r),
                                 "errors" -> eStat, "bounds" -> bStat, "ratios" -> rStat))
      writeToFile(cd.path.get + experimentId.toString + "-similarity-error-statistics-" + r.toString + ".json",
                  Json.prettyPrint(allStat))
    }
  }

  /** Calculates similarities for benchmark word pairs for different in-degree thesholds. */
  def wordSimilarityBenchmark(sc : SparkContext, 
                              cd : ConceptData,
                              source : String,
                              benchmarkFile : String,                                      
                              correlationFunction : (Long, Long, Long, Long) => (Double, Double),
                              corrFuncLabel : String,
                              minConceptCount : Long,
                              minContextCount : Long,
                              minCorrelation : Double,
                              maxDegreeMin: Long,
                              maxDegreeMax: Long,
                              maxDegreeDelta: Long) : Unit = {

    // (label, concept id) pairs
    val tokenToId = cd.concepts.get.map(_.swap)
    // Load benchmark token pairs and map to object ids. NOTE: May lose tokens.
    val targetPairs = sc.textFile(benchmarkFile).map(_.split(",")).map(p => (p(0), p(1))).
                         join(tokenToId).map{case (t1, (t2, i1)) => (t2, i1)}.
                         join(tokenToId).map{case (t2, (i1, i2)) => if(i1 > i2) (i2, i1) else (i1, i2)}
    val targetObjects = targetPairs.flatMap{case (i1, i2) => Iterable(i1, i2)}.distinct
    val idToLabel = targetObjects.map{case i => (i, None)}.join(cd.concepts.get).
                    map{case (i, (None, s)) => (i, s)}.collect.toMap
    // Build correlation graph
    val correlations = EdgeData(contextsToCorrelations(cd, minConceptCount, 
                                minContextCount, correlationFunction, minCorrelation))

    // Calculate similarities for all benchmark word pairs for different indegree thresholds
    val result = {maxDegreeMin to maxDegreeMax by maxDegreeDelta}.map{case r =>
      val prunedCorrelations = pruneEdgesByInDegree(correlations, r)
      // Only consider remaining target object with outgoing edges
      val remainingTargetObjects = prunedCorrelations.weights.map{case (i, j, w) => i}.
                                   intersection(targetObjects).collect
      // Map from object to its pruned correlation vector and sum of pruned and unpruned correlations
      val corrVecDict = remainingTargetObjects.map{case obj => 
        // Kept correlations
        val c = prunedCorrelations.weights.filter{case (i, j, w) => i == obj}.map{case (i, j, w) => (j, w)}
        // Sum of kept and discarded correlations
        val e = prunedCorrelations.sums.lookup(obj).map{case (s, t) => s+t}.head
        // Key-value pair in dictionary
        (obj, (c, e))
      }.toMap

      // Relative L1 similarity
      def l1Similarity(di: (RDD[(ConceptID, Weight)], Weight), dj: (RDD[(ConceptID, Weight)], Weight)) : Double = {
        val vi = di._1
        val vj = dj._1
        val s = di._2 + dj._2
        val lambda = vi.join(vj).map{case (k, (vik, vjk)) => abs(vik - vjk) - abs(vik) - abs(vjk)}.sum
        val l1 = s + lambda
        1.0 - l1/s
      }

      val similarities = targetPairs.filter{case (i, j) => {corrVecDict contains i} && {corrVecDict contains j}}.
                         collect.map{case (i, j) => 
                            JsObject(Seq("token pair" -> JsString(idToLabel(i) + "," + idToLabel(j)),
                                         "relative l1 similarity" -> JsNumber(l1Similarity(corrVecDict(i), corrVecDict(j)))))
                         }
      JsObject(Seq("indegree" -> JsNumber(r), "similarities" -> JsArray(similarities))
      )
    }

    val experimentId = System.currentTimeMillis/1000
    val parameters = JsObject(Seq(
      "source" -> JsString(source),
      "benchmark file" -> JsString(benchmarkFile),
      "correlation function" -> JsString(corrFuncLabel),
      "min concept count" -> JsNumber(minConceptCount),
      "min context count" -> JsNumber(minContextCount),
      "min abs correlation" -> JsNumber(minCorrelation),
      "max indegree min" -> JsNumber(maxDegreeMin),
      "max indegree max" -> JsNumber(maxDegreeMax),
      "max indegree delta" -> JsNumber(maxDegreeDelta))
    )
    val output = JsObject(Seq(
        "id" -> JsNumber(experimentId), 
        "date" -> JsString((new java.util.Date).toString),
        "description" -> JsString("Relative L1 similarities for benchmark word pairs for different similarity thresholds."),
        "parameters" -> parameters,
        "result" -> JsArray(result))
    )
    // Save result as JSON
    writeToFile(cd.path.get + experimentId.toString + "-indegree-vs-benchmark-similarities.json", Json.prettyPrint(output))
  }
}
