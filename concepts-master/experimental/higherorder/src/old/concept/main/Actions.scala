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

package se.sics.concepts.main

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import play.api.libs.json._
import se.sics.concepts.util.{Statistics, ParameterMap, ConceptsUtil}

import se.sics.concepts.clustering.ClusteringAlgorithm
import se.sics.concepts.util.Statistics
import se.sics.concepts.graphs.Transformations._
import se.sics.concepts.higherorder.HigherOrder._
import se.sics.concepts.graphs.{Counts, ConceptIDs, EdgeData}
import ConceptIDs._
import Counts._
import se.sics.concepts.io.ConceptData
import se.sics.concepts.io.ConceptsIO._
import ConceptsUtil._
import Statistics._
import se.sics.concepts.classification._

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import scala.math.abs

/** Evaluation, test, and utility functions for the concepts library. */
object Actions {
  /** Builds and writes concept data to files from examples.
   *
   * From an input text file, finds and writes 1) concept labels and id 2) recurring concepts
   * and 3) complete context data. All concepts on the same line (separated by single blan spaces)
   * are assumed to belong to one single context. Optionally, these contexts can be expanded using
   * a sliding window (particularly useful with one concept per line and a sliding context window).
   *
   * @param sc Spark context
   * @param exampleFile Path to input file with examples. Assumes one or more concepts per line
   *        (separated by a single blankspace).
   * @param outputPath Path of output files
   * @param minCount Minimum allowed concept count
   * @param minRecurrentCount Minimum number of occurrences for a recurrent concept to be included
   * @param windowSize Context sliding window size (should be 1 for input files with relevant
   *        contexts already encoded by line)
   * @param findRecurrent If false, procedure of finding recurrent concept is skipped.
   * @param uniqueInputs If true, all concept occurrences are assumed to be unique per example
   *        (saves computation time if occurrences are already unique).
   * @param indexedInputs If true, all concepts are assumed to be already indexed in data
   *        (i.e. positive integer numericals). Requires that a custom dictionary is provided.
   * @param dictionaryFile If available, file is used as dictionary for translating strings to concept IDs,
   *        otherwise unique strings in data is enumerated. Must be on the form conceptID value followed
   *        by blank space and the concept string representation per line.
   */
  def textfileToContexts(sc                : SparkContext,
                         exampleFile       : String,
                         outputPath        : String,
                         minCount          : Long,
                         minRecurrentCount : Long,
                         windowSize        : Long = 1L,
                         findRecurrent     : Boolean = true,
                         uniqueInputs      : Boolean = false,
                         indexedInputs     : Boolean = false,
                         dictionaryFile    : Option[String] = None,
                         countContexts     : Boolean = false) : Unit = {
    val DEFAULT_STORAGE_LEVEL = MEMORY_AND_DISK_SER

    val examples        = sc.textFile(exampleFile)
                            .map(_.split(" +"))
                            .setName("examples")

    val indexedLabels: RDD[(String, ConceptID)]   = if(dictionaryFile.isEmpty) {
                            enumerateUniqueStrings(examples.flatMap(x=>x), minCount)
                          }
                          else {
                            sc.textFile(dictionaryFile.get).
                            map(s => {
                              val f = s.split(" ")
                              (f(1), f(0).toLong)})
                          }.setName("indexedLabels")

    val indexedLabelsBcast = sc.broadcast(indexedLabels.collectAsMap())

    val indexedExamples: RDD[(DataIndex, ConceptID)] = if (indexedInputs) {
                            examples.zipWithIndex().map{case (s,i) => (i,s)}.
                            flatMapValues(x=>x).mapValues{(x)=>x.toLong}
                          }
                          else {
                            examples.zipWithIndex().map{case (s,i)=>(i,s)}
                            .flatMapValues(x=>x).map{case (i,s)=>(s,i)}
                            // TODO: How can we reduce the footprint of examples before the join?
                            // Could we filter out low-frequency items but maintain the correctness of the windowing?
                            // e.g. do a filter after zipWithIndex(), and determine sequence by example ID difference
                            .mapPartitions({
                              iter =>
                                val stringToStringIDMap = indexedLabelsBcast.value
                                for {
                                  (str, exID) <- iter
                                  // NOTE: Discarded objects in indexedLabels can cause one-object contexts
                                  if stringToStringIDMap.contains(str)
                                } yield (exID, stringToStringIDMap(str))
                            },
                            preservesPartitioning = true)
                          }.setName("indexedExamples")

    val windowedExamples = if(windowSize >= 2) indexedDataMovingWindow(indexedExamples, windowSize)
                           else indexedExamples

    val reducedExamples  = windowedExamples.map(x => (x, 1L)).reduceByKey(_ + _)
      .setName("reducedExamples")

    val conceptDataPair = if(findRecurrent) {
      val repeatedExamples  = reducedExamples.filter(x => x._2 >= 2).persist(DEFAULT_STORAGE_LEVEL)

      val recurrentConcepts = repeatedExamples.
                              map{case ((exID,strID), repeatCount) => ((strID,repeatCount), 1L)}.reduceByKey(_ + _).
                              filter{case (strIDAndRepCount, recurrentCount) => recurrentCount >= minRecurrentCount}.
                              keys.zipWithIndex().
                              map{case (strIDAndRepCount, i) => (i + ConceptIDs.recurrentConceptsStartID, strIDAndRepCount)}.
                              persist(DEFAULT_STORAGE_LEVEL)
      val multipleCounts    = repeatedExamples.map{case ((exID,strID), repeatCount) => (strID, (exID,repeatCount))}
      // Only keep the recurrences that have a count at most as large as observed for any particular recurrence ID
      val recurrentExamples = recurrentConcepts.map{case (recurID, (strID,repeatCount)) => (strID, (repeatCount,recurID))}.
                              join(multipleCounts).
                              filter{case (strID, ((repeatCountLeft,recurID), (exID,repeatCountRight))) =>
                              repeatCountLeft <= repeatCountRight}.
                              map{case (c,((n,r),(i,m))) => (i,r)}

      (reducedExamples.keys ++ recurrentExamples, Some(recurrentConcepts))
    }
    else {
      (if(uniqueInputs && windowSize >= 2) indexedExamples else reducedExamples.keys, None)
    }

    ConceptData.write(sc, concepts = Some(indexedLabels.map{case (s,i) => (i,s)}),
                      contexts = if(countContexts) None else Some(conceptDataPair._1),
                      contextCounts = if(countContexts) Some(countConceptContexts(conceptDataPair._1)) else None,
                      recurrentConcepts = conceptDataPair._2,
                      // NOTE: May not agree when counting contexts is enabled due to removal of singelton contexts
                      count = Some(indexedExamples.count()),
                      path = Some(outputPath))
  }

  /** Count context occurrences */
  def countConceptContexts(contexts : RDD[(DataIndex, ConceptID)]) :
    RDD[(ConceptID, (ContextID, ContextCount))] = {
    // Aggregate concepts per context id
    // NOTE: Assumes that aggregates are sufficiently small for an aggregateByKey
    val aggregatedContexts = contexts.aggregateByKey(Set[ConceptID]())((s, c) => s + c, (s, t) => s ++ t).
                             filter{case (e, s) => s.size > 1} // Discard singleton contexts
    // Link concepts with unique contexts and context counts
    // NOTE: Adding counts here takes up redundant space, but saves us expensive joins later on
    aggregatedContexts.map{case (e, s) => (s, 1L)}.reduceByKey(_ + _).
                       zipWithIndex.flatMap{case ((s, c), i) => s.map(j => (j, (i, c)))}
  }

  def textFileToContextCounts(
      sc: SparkContext,
      textFile: String,
      outputPath: String,
      windowSize: Int,
      minCount: Long)
  : Unit = {
    // Initial data after basic filtering
    val initialData: RDD[Array[String]] = sc.textFile(textFile)
      // The following filtering assumes text data
      .map(ln => ln.replaceAll("[^a-zA-Z0-9 ]", " ").toLowerCase.split(" +"))
      // Remove singleton records
      .filter(_.size > 1)
      // If we have tons of memory/small dataset, cache this here, and unpersist later
      .cache().setName("initialData")

    // Pass through (possibly cached) data again to create filtered ngrams
    // Also do transformation from String to ID here
    val ngramLists: RDD[List[String]] = initialData
      .flatMap(x => x.sliding(windowSize).map(_.toList))

    // Count the appearances of ngrams
    // TODO: Could also filter out rare ngrams here, for added performance boost
    val ngramCounts: RDD[(List[String], ContextCount)] = ngramLists.map((_, 1L))
      .reduceByKey(_ + _)
      .cache().setName("ngramCounts")

    // Prepare ngrams for writing to file
    val ngramCountStrings = ngramCounts
      .map{
        case (tokens: List[String], count: ContextCount) =>
          s"$count,${tokens.mkString(",")}"

      }.setName("ngramCountStrings")

    // Write the ngrams to file
    ngramCountStrings.saveAsTextFile(outputPath + s"/$windowSize-grams")

    val objectAndContextCounts: RDD[(Set[(String, Int)], ContextCount)] = ngramCounts.map{
      case (tokens: List[String], contextCount: ContextCount) =>
        // Counts per context
        val wordCounts = tokens.groupBy(identity).mapValues(_.size).toSet
        (wordCounts, contextCount)
    }.cache().setName("objectAndContextCounts")
    ngramsToContextCounts(objectAndContextCounts, minCount, outputPath)
  }

  /**
   * Assumes a comma separated file with "count,token 1,token 2,...,token n" per line.
   * Note that we only prune by context count here. Recurrent concepts are treated as
   * regular concepts (i.e. not pruned here by minRecCount).
   */
  def ngramsFileToContextCounts(sc                : SparkContext,
                                ngramFile         : String,
                                outputPath        : String,
                                minContextCount   : Long,
                                storageLevel      : storage.StorageLevel = MEMORY_AND_DISK) : Unit = {

    // Parse and map n-grams to contexts that may contain recurrent concepts
    def parseAndCount(line : String) = {
      val fields = line.split(",")
      val contextCount = fields.head.toLong
      val tokens = fields.tail
      val wordCounts = tokens.groupBy(identity).mapValues(_.size).toSet // Counts per context
      (wordCounts, contextCount)
    }

    // Parse and count concept occurrences per context, and pair with context counts
    val objectAndContextCounts = sc.textFile(ngramFile).map(parseAndCount(_))

    ngramsToContextCounts(objectAndContextCounts, minContextCount, outputPath, storageLevel)
  }

  private def ngramsToContextCounts(
      objectAndContextCounts: RDD[(Set[(String, Int)], ContextCount)],
      minContextCount: Long,
      outputPath: String,
      storageLevel: StorageLevel = MEMORY_AND_DISK)
    : Unit = {
    // Cache level

    // Parse and count concept occurrences per context, and pair with context counts
    val parsedContextCounts = objectAndContextCounts.
      // Sum up counts of unordered n-grams
      reduceByKey(_ + _).
      // Discard infrequent contexts and singleton contexts
      filter{case (n, c) => c >= minContextCount && n.size > 1}.
      persist(storageLevel)
    // Total number of context occurrences
    val totalCount = parsedContextCounts.values.sum.toLong
    // Set context index and count per conceps
    val dualLabelCounts = parsedContextCounts.zipWithIndex.
      flatMap{case ((n, c), i) => n.map(t => (t, (i, c)))}.
      persist(storageLevel)
    // (label, index) pairs
    val labelIndices = dualLabelCounts.keys.keys.distinct.zipWithIndex
    // Context counts for individual concepts
    val individualIndexCounts = dualLabelCounts.filter{case ((s, sc), (c, cc)) => sc == 1}.
      map{case ((s, sc), (c, cc)) => (s, (sc, c, cc))}.join(labelIndices).
      map{case (s, ((sc, c, cc), si)) => (si, (c, cc))}
    // Get contexts with recurrent concepts
    val recurrentDualCounts = dualLabelCounts.filter{case ((s, sc), (c, cc)) => sc > 1}.persist(storageLevel)
    // Set recurrent concept ids
    val recurrentLabelIndices = recurrentDualCounts.keys.zipWithIndex.
      mapValues(_ + recurrentConceptsStartID).persist(storageLevel)
    // Context counts for recurrent concepts
    val recurrentIndexCounts = recurrentDualCounts.join(recurrentLabelIndices).
      map{case ((s, sct),((c, cc), ri)) => (ri, (c, cc))}
    val recurrentData = recurrentLabelIndices.map{case ((s, sc), ci) => (s, (sc, ci))}.
      join(labelIndices).map{case (s, ((sc, ci), si)) => (ci, (si, sc.toLong))}

    ConceptData.write(
      objectAndContextCounts.sparkContext,
      concepts = Some(labelIndices.map{case (s,i) => (i,s)}),
      contexts = None,
      contextCounts = Some(individualIndexCounts ++ recurrentIndexCounts),
      recurrentConcepts = Some(recurrentData),
      count = Some(totalCount),
      path = Some(outputPath))
  }

  /** Find and write correlation and similarity graph to file. */
  def contextsToCorrelations(cd : ConceptData,
                             minConceptCount : Long,
                             minContextCount : Long,
                             correlationFunction : (Long, Long, Long, Long) => (Double, Double),
                             minCorrelation : Double) : RDD[(ConceptID, ConceptID, Weight)] = {
    // Use context counts if available, othewise use context
    val counts: RDD[((ConceptID, ConceptID), (PairCount, ConceptCount, ConceptCount))] =
      if(cd.contextCounts.isEmpty) incrementalPairwiseCounts(cd.contexts.get, cd.contexts.get, minConceptCount)
      else incrementalPairwiseCountsFromCounts(cd.contextCounts.get, cd.contextCounts.get,
                                               minConceptCount, minContextCount)
    val nrExamples = cd.count.get
    counts.mapValues{case (cij,ci,cj)=> correlationFunction(cij,ci,cj,nrExamples)}.
           flatMap{case ((i,j),(cij,cji))=>Iterable((i,j,cij),(j,i,cji))}.
           filter(c => abs(c._3) > minCorrelation)
  }

  /** Find and write correlation and similarity graph to file. */
  def contextsToGraphs(sc : SparkContext,
                       cd : ConceptData,
                       minConceptCount : Long,
                       minContextCount : Long,
                       correlationFunction : (Long, Long, Long, Long) => (Double, Double),
                       minCorrelation : Double,
                       minSimilarity : Double,
                       maxInDegree : Long,
                       calculateCorrelations : Boolean = true,
                       calculateSimilarities : Boolean = true) : Unit = {
    val edges = if (calculateCorrelations) {
      contextsToCorrelations(cd, minConceptCount, minContextCount, correlationFunction, minCorrelation)
    }
    else cd.correlations.get

    if (calculateSimilarities) {
      val correlationGraph = EdgeData(edges)
      val prunedCorrelationGraph = pruneEdgesByInDegree(pruneEdgesByWeight(correlationGraph, minCorrelation), maxInDegree)
      val similarities = correlationsToSimilarities(prunedCorrelationGraph).filter(_._3 > minSimilarity)
      cd.write(sc, newSimilarities = Some(similarities))
    }

    if (calculateCorrelations) cd.write(sc, newCorrelations = Some(edges))
  }

  /** Cluster vertices in correlation and similarity graphs. */
  def clusterVertices(
      sc : SparkContext, cd : ConceptData,
      minCorr: Double,
      minSim: Double,
      clusteringAlgo: ClusteringAlgorithm,
      clusteringParameters: ParameterMap,
      calculateCorrelations : Boolean = true,
      calculateSimilarities : Boolean = true) :  Unit = {

    if(calculateCorrelations) {
      val filteredCorrEdges = cd.correlations.get.filter{case (i,j,w)=>w >= minCorr}
      val c = clusteringAlgo.cluster(filteredCorrEdges, clusteringParameters)
      cd.write(sc, newCorrClusters = Some(c))
    }
    if(calculateSimilarities) {
      val filteredSimEdges = cd.correlations.get.filter{case (i,j,w)=>w >= minSim}
      val c = clusteringAlgo.cluster(filteredSimEdges, clusteringParameters)
      cd.write(sc, newSimClusters = Some(c))
    }
  }

  /** Find and write higher order concepts to file. */
  def contextsToHigherOrderConcepts(
      sc: SparkContext,
      cd: ConceptData,
      minConceptCount : Long,
      minContextCount : Long,
      correlationFunction : (Long, Long, Long, Long) => (Double, Double),
      minCorrelation : Double = 0.0,
      minSimilarity : Double = 0.0,
      maxInDegree : Long = 100L,
      conceptThreshold : Double,
      clusteringAlgo: ClusteringAlgorithm, // Can add different algo or parameters for corr/sim if we wanted
      clusteringParams: ParameterMap,
      maxLevel : Long = 2)
    : Unit = {

    val andOrConcepts =
      findProjectedAndOrConceptsFromCounts(
          sc,
          cd.count.get,
          cd.contextCounts.get,
          correlationFunction,
          clusteringAlgo,
          clusteringParams,
          maxLevel.toInt,
          minConceptCount,
          minContextCount,
          Some(minCorrelation),
          Some(minSimilarity),
          Some(maxInDegree),
          conceptThreshold)

    cd.write(sc, newAndConcepts = Some(andOrConcepts.andConcepts), newOrConcepts = Some(andOrConcepts.orConcepts))
  
    // NOTE: Here temporarily
    val experimentId = (System.currentTimeMillis/1000).toString
    writeHigherOrderConceptsAsText(cd.recurrentConcepts.get,
                                   andOrConcepts.andConcepts,
                                   andOrConcepts.orConcepts,
                                   cd.concepts.get,
                                   cd.path.get + experimentId + "-higher-order-concepts.txt")
    /*writeAndOrConceptEdgesAsText(andOrConcepts.correlationGraph.weights,
                                 cd.concepts.get,
                                 andOrConcepts.andConcepts,
                                 andOrConcepts.orConcepts,
                                 1000000,
                                 cd.path.get + experimentId + "-higher-order-concept-correlations.txt")
    writeAndOrConceptEdgesAsText(andOrConcepts.similarityGraph,
                                 cd.concepts.get,
                                 andOrConcepts.andConcepts,
                                 andOrConcepts.orConcepts,
                                 1000000,
                                 cd.path.get + experimentId + "-higher-order-concept-similarities.txt")*/
    exportORConceptHierarchyToNewick(andOrConcepts.similarityGraph,
                                     cd.concepts.get,
                                     andOrConcepts.orConcepts,
                                     cd.path.get + experimentId + "-or-concept-hierarchies.tre")
  }

  /** Export context data on readable text format. */
  def exportContextsAsText(sc: SparkContext, cd: ConceptData) : Unit = {
    val contexts = cd.contexts.get
    val contextStrings = contexts.map{case (i,c)=>(c,i)}.
                         leftOuterJoin(cd.concepts.get).
                         map{case (c,(i,s))=>(i,s.getOrElse(c.toString))}
    val collected = contextStrings.groupByKey().sortBy(_._1)
    val writable = collected.map{case (i,s)=>i.toString+": "+s.mkString(" ")}
    val fileName = cd.path.get + "contexts.txt"
    if (fileExists(contexts.context, fileName)) deleteFile(sc, fileName)
    if (fileExists(contexts.context, fileName + ".tmp")) deleteFile(sc, fileName)
    writable.saveAsTextFile(fileName + ".tmp")
    mergeFile(sc, fileName + ".tmp", fileName, deleteOriginal = true)
  }

  /** Export higher order concepts on readable text format. */
  def exportHigherOrderConceptsAsText(sc: SparkContext, cd: ConceptData) : Unit = {
    writeHigherOrderConceptsAsText(cd.recurrentConcepts.getOrElse(sc.emptyRDD),
                                   cd.andConcepts.getOrElse(sc.emptyRDD),
                                   cd.orConcepts.getOrElse(sc.emptyRDD),
                                   cd.concepts.getOrElse(sc.emptyRDD),
                                   cd.path.get + "higherorder.txt")
  }

  /** Export higher order concepts on readable text format. */
  def exportHigherOrderConceptsAsTextScalable(sc: SparkContext, cd: ConceptData) : Unit = {
    writeHigherOrderConceptsAsTextScalable(
      cd.recurrentConcepts.get,
      cd.andConcepts.get,
      cd.orConcepts.get,
      cd.concepts.get,
      cd.path.get)
  }

  /** Export correlation and similarity graphs as readable text. */
  def exportGraphsAsText(sc: SparkContext, cd: ConceptData, num: Int) : Unit = {

    if (cd.correlations.isDefined) {
      writeTopLabeledEdges(edges = cd.correlations.get,
                           labels = cd.concepts.get,
                           num = num,
                           fileName = cd.path.get + "correlations.txt")
    }
    if (cd.similarities.isDefined) {
      writeTopLabeledEdges(edges = cd.similarities.get,
                           labels = cd.concepts.get,
                           num = num,
                           fileName = cd.path.get + "similarities.txt")
    }
  }

  /** Export correlation and similarity graphs as JSON. */
  def exportGraphsAsJSON(sc: SparkContext, cd: ConceptData,
                         minCorrelation: Double, maxNumEdges: Long, minSimilarity: Double) : Unit = {

    if (cd.correlations.isDefined) {
      exportGraphToJSON(edges = cd.correlations.get,
                        clusters = cd.corrClusters, // Include cluster assignments if available
                        concepts = cd.concepts.get,
                        weightThreshold = minCorrelation,
                        numEdges = maxNumEdges,
                        fileName = cd.path.get + "correlations.json")
    }
    if (cd.similarities.isDefined) {
      exportGraphToJSON(edges = cd.similarities.get,
                        clusters = cd.simClusters, // Include cluster assignments if available
                        concepts = cd.concepts.get,
                        weightThreshold = minSimilarity,
                        numEdges = maxNumEdges,
                        fileName = cd.path.get + "similarities.json")
    }
  }

  /** Calculate graph statistics and export to JSON. */
  def exportGraphStatistics(sc : SparkContext, cd : ConceptData, numBins : Int,
                            calculateCorrStats : Boolean = true,
                            calculateSimStats : Boolean = true) : Unit = {

    if(calculateCorrStats && cd.correlations.isDefined)
      writeToFile(cd.path.get + "correlation-statistics.json",
                  Json.prettyPrint(graphStatistics(cd.correlations.get, numBins)))

    if(calculateSimStats && cd.similarities.isDefined)
      writeToFile(cd.path.get + "similarity-statistics.json",
                  Json.prettyPrint(graphStatistics(cd.similarities.get, numBins)))
  }

  def testClassifier(cd: ConceptData, classifierName : String) : Unit = {
    // NOTE: This may conflict with MLlib's namesake
    // The following is currently wrong, but has the right type signature...
    //val classifier : ConceptClassifier = classifierName match {
    //  case "NaiveBayes" => new NaiveBayes(cd.contexts.get, cd.contexts.get, cd.count.get)
    //}
  }
}

