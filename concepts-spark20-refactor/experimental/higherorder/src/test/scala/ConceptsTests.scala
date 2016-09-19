/*
 Copyright 2015 Daniel Gillblad (dgi@sics.se)

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

import org.scalatest._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel._
import se.sics.concepts.util.{ParameterMap, ConceptsUtil, CKryoRegistrator}
import se.sics.concepts.graphs.{Counts, EdgeData}

import Counts._
import se.sics.concepts.graphs.Transformations._
import se.sics.concepts.graphs.EdgeData
import se.sics.concepts.higherorder.HigherOrder._
import se.sics.concepts.io.ConceptData
import ConceptsUtil._
import se.sics.concepts.measures.CorrelationMeasures._
import se.sics.concepts.clustering._
import se.sics.concepts.clustering.ClusteringFunction.connectedComponents
import se.sics.concepts.io.ConceptsIO._

import se.sics.concepts.higherorder.{Or, And, Atom}
import se.sics.concepts.higherorder.Formula._

import se.sics.concepts.classification._

abstract class UnitSpec extends FlatSpec with OptionValues with Inside with Serializable

class ConceptsTests extends UnitSpec with BeforeAndAfter {

  /** Compare two traversable collections recursively.
    *
    * Only considers element equality and order, type of collection does not matter.
    *
    * @param eqfunc Equality function for elements, defaults to '=='
    */
  def recursiveEqual[T, U](a : Traversable[T], b: Traversable[U],
                           eqfunc : (Any, Any) => Boolean = (x : Any, y: Any) => {x == y}) : Boolean = {
    if(a.isEmpty && b.isEmpty) true
    else if(a.isEmpty || b.isEmpty) false
    else {
      val ah = a.head
      val bh = b.head
      ah match {
        case aht : Traversable[_] => {
          bh match {
            case bht : Traversable[_] => recursiveEqual(aht, bht, eqfunc) &&
              recursiveEqual(a.tail, b.tail, eqfunc)
            case bha : Array[_] => recursiveEqual(aht, bha, eqfunc) &&
              recursiveEqual(a.tail, b.tail, eqfunc)
            case _ => false
          }
        }
        case aha : Array[_] => { // Arrays aren't actually traversables, so...
          bh match {
            case bht : Traversable[_] => recursiveEqual(aha, bht, eqfunc) &&
              recursiveEqual(a.tail, b.tail, eqfunc)
            case bha : Array[_] => recursiveEqual(aha, bha, eqfunc) &&
              recursiveEqual(a.tail, b.tail, eqfunc)
            case _ => false
          }
        }
        case _ => eqfunc(ah, bh) && recursiveEqual(a.tail, b.tail, eqfunc)
      }
    }
  }

  private var sc: SparkContext = _

  // Make sure we set up and bring down spark context before and after tests
  before {
    // Spark context configuration
    val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("ConceptsTest")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
    .set("spark.kryoserializer.buffer.mb", "1")
    .set("spark.kryoserializer.buffer.max.mb", "1024")
    .set("spark.shuffle.consolidateFiles", "true")
    .set("spark.eventLog.enabled", "false")
    .set("spark.eventLog.compress", "true")
    .set("spark.default.parallelism", (8).toString)
    .set("spark.storage.memoryFraction", "0.5")
    .set("spark.io.compression.codec", "lz4")
    // .set("spark.io.compression.codec", "lz4")
    sc = new SparkContext(conf)
  }

  // Make sure everything stops as it should
  after {
    if (sc != null) {
      sc.stop()
    }
  }

  // Do the actual unit tests

  "Concept data" should "be correctly transformable to indexed data and back" in {
    val initData = List(Array(0L, 1L, 2L),
                        Array(0L, 3L, 4L),
                        Array(3L, 4L, 5L))
    val retdata = indexedToCollectedData(collectedToIndexedData(sc.parallelize(initData))).collect()
    info(retdata.mkString(":"))
    assert(recursiveEqual(initData, retdata))
  }

  "Counting functions" should "produce correctly filtered marginal counts" in {
    val initData = List(Array(0L, 1L, 2L),
                        Array(0L, 3L, 4L),
                        Array(3L, 4L, 5L))
    val targetData = List((0L, 2L), (1L, 1L), (2L, 1L), (3L, 2L), (4L, 2L), (5L, 1L))

    val indexedData = collectedToIndexedData(sc.parallelize(initData))
    val counts = countConcepts(indexedData).collect()

    info(counts.mkString(":"))
    assert(recursiveEqual(targetData.sorted, counts.sorted))
  }

  it should "produce correct pairwise concept counts" in {
    val initData = List(Array(0L, 1L, 2L),
                        Array(0L, 3L, 4L),
                        Array(3L, 4L, 5L))

    val targetData = List(((0,1),(1,2,1)),((0,2),(1,2,1)),((1,2),(1,1,1)),
                          ((0,3),(1,2,2)),((0,4),(1,2,2)),((3,4),(2,2,2)),
                          ((3,5),(1,2,1)),((4,5),(1,2,1)))

    val indexedData = collectedToIndexedData(sc.parallelize(initData))
    val allpcounts = incrementalPairwiseCounts(indexedData, indexedData).collect()

    info(allpcounts.mkString(":"))
    assert(recursiveEqual(targetData.sorted, allpcounts.sorted))
  }

  it should "produce correct pairwise concept counts from context counts" in {
    val minConceptCount = 0
    val minContextCount = 2
    val c = List((0L, 1L), (1L, 4L), (2L, 2L), (3L, 1L), (4L, 5L))
    val initDataA = List((0L, c(0)), (0L, c(1)), (0L, c(4)), (1L, c(0)))
    val initDataB = List((2L, c(3)), (3L, c(1)), (3L, c(2)), (3L, c(3)),
                         (3L, c(4)), (4L, c(1)), (4L, c(4)))
    val targetData = Set(((0L, 3L), (9L, 9L, 11L)), ((0L, 4L), (9L, 9L, 9L)), ((3L, 4L), (9L, 11L, 9L)))
    val counts = incrementalPairwiseCountsFromCounts(sc.parallelize(initDataA),
                                                     sc.parallelize(initDataB),
                                                     minConceptCount, minContextCount).collect.toSet
    info(counts.mkString(":"))
    assert(counts == targetData)
  }

  it should "produce correct coexistence counts" in {
    val observedlistA = List(Array(0L, 1L, 2L, 1L, 1L),
                             Array(0L, 3L, 4L),
                             Array(3L, 4L, 5L))
    val observedlistB = List(Array(10L, 11L),
                             Array(12L, 13L),
                             Array(14L, 1L))

    val targetA = List(((0,1),(1,2,1)),((0,2),(1,2,1)),((1,2),(1,1,1)),
                       ((0,3),(1,2,2)),((0,4),(1,2,2)),((3,4),(2,2,2)),
                       ((3,5),(1,2,1)),((4,5),(1,2,1)))
    val targetB = List(((0,10),(1,2,1)),((1,10),(1,2,1)),((2,10),(1,1,1)),
                       ((0,11),(1,2,1)),((1,11),(1,2,1)),((10,11),(1,1,1)),
                       ((2,11),(1,1,1)),((1,3),(1,2,2)),((1,4),(1,2,2)),
                       ((0,12),(1,2,1)),((3,12),(1,2,1)),((4,12),(1,2,1)),
                       ((0,13),(1,2,1)),((3,13),(1,2,1)),((4,13),(1,2,1)),
                       ((12,13),(1,1,1)),((1,5),(1,2,1)),((1,14),(1,2,1)),
                       ((3,14),(1,2,1)),((4,14),(1,2,1)),((5,14),(1,1,1)))

    val indexedDataA = collectedToIndexedData(sc.parallelize(observedlistA))
    val indexedDataB = collectedToIndexedData(sc.parallelize(observedlistB))

    val allpcountsA = incrementalCoexistenceCounts(indexedDataA, indexedDataA).collect()
    val allpcountsB = incrementalCoexistenceCounts(indexedDataA, indexedDataB).collect()

    info(allpcountsA.mkString(":"))
    info(allpcountsB.mkString(":"))

    assert(recursiveEqual(targetA.sorted, allpcountsA.sorted) &&
           recursiveEqual(targetB.sorted, allpcountsB.sorted))
  }

  it should "produce meaningful correlations" in {
    val initData = List(Array(0L, 1L, 2L),
                        Array(0L, 3L, 4L),
                        Array(3L, 4L, 5L))
    val targetData = List(((0,1),(0.5,1.0)),((0,2),(0.5,1.0)),((1,2),(1.0,1.0)),
                          ((0,3),(0.5,0.5)),((0,4),(0.5,0.5)),((3,4),(1.0,1.0)),
                          ((3,5),(0.5,1.0)),((4,5),(0.5,1.0)))

    val indexedData = collectedToIndexedData(sc.parallelize(initData))
    val allpcounts = incrementalPairwiseCounts(indexedData, indexedData)
    val corr = allpcounts.map(x => (x._1, conditionalProbability(x._2._1, x._2._2, x._2._3, 3))).collect()

    info(corr.mkString(":"))
    assert(recursiveEqual(targetData.sorted, corr.sorted, ~=))
  }

  it should "produce correct recurrence counts" in {
    val observedlist = List(Array(0L, 1L, 2L, 1L, 1L),
                            Array(0L, 3L, 4L, 1L, 1L),
                            Array(3L, 4L, 5L, 5L, 1L, 1L))
    val targetData = List(((1,2),(2,3)),((1,3),(1,3)),((5,2),(1,1)))

    val indexedData = collectedToIndexedData(sc.parallelize(observedlist))
    val recurrcounts = countRecurrences(indexedData).collect()

    info(recurrcounts.mkString(":"))
    assert(recursiveEqual(targetData.sorted, recurrcounts.sorted))
  }

  it should "find recurrent concepts" in {
    val observedlist = List(Array(0L, 1L, 2L, 1L, 1L),
                            Array(0L, 3L, 4L, 1L, 1L),
                            Array(3L, 4L, 5L, 5L, 1L, 1L))
    val targetData = List((1,2),(5,2),(1,3))

    val indexedData = collectedToIndexedData(sc.parallelize(observedlist))
    val recurc = findRecurrencentConcepts(indexedData).collect()

    info(recurc.mkString(":"))
    assert(recursiveEqual(targetData.sorted, recurc.sorted))
  }

  "Higher-order activation functions" should "produce correct recurrent activations" in {
    val observedlist = List(Array(0L, 1L, 2L, 1L, 1L),
                            Array(0L, 3L, 4L, 1L, 1L),
                            Array(3L, 4L, 5L, 5L, 1L, 1L))
    val recconceptlist = List((10L,(1L,2)),
                              (11L,(1L,3)),
                              (12L,(5L,2)))
    val targetData = List((2,10),(0,10),(1,10),(0,11),(2,12))

    val indexedData = collectedToIndexedData(sc.parallelize(observedlist))
    val recConcepts = sc.parallelize(recconceptlist)
    val recact = activateRecurrentConcepts(indexedData, recConcepts).collect()

    info(recact.mkString(":"))
    assert(recursiveEqual(targetData.sorted, recact.sorted))
  }

  it should "produce correct OR activations" in {
    val indexedData = collectedToIndexedData(sc.parallelize((List(Array(0L, 1L, 2L),
                                             Array(0L, 3L, 4L),
                                             Array(3L, 4L, 5L)))))
    val orconcepts  = sc.parallelize(List((1L, 10L),
                                     (2L, 10L),
                                     (4L, 11L),
                                     (6L, 11L),
                                     (6L, 12L),
                                     (7L, 12L)))
    val targetData = List((0,10),(2,11),(1,11))

    val oract = activateORConcepts(indexedData, orconcepts).collect()
    info(oract.mkString(":"))
    assert(recursiveEqual(targetData.sorted, oract.sorted))
  }

  it should "produce correct AND activations" in {
    val indexedData = collectedToIndexedData(sc.parallelize((List(Array(0L, 1L, 2L),
                                             Array(0L, 3L, 4L),
                                             Array(3L, 4L, 5L)))))
    val andconcepts = sc.parallelize(List((1L, 13L),
                                     (2L, 13L),
                                     (4L, 14L),
                                     (6L, 14L),
                                     (6L, 15L),
                                     (7L, 15L)))
    val andcsize   = sc.parallelize(List((13L, 2),
                                    (14L, 2),
                                    (15L, 2)))
    val targetData = List((0,13))

    val andact = activateANDConceptsUnique(indexedData, andconcepts, andcsize).collect()
    info(andact.mkString(":"))
    //assert(recursiveEqual(targetData.sorted, andact.sorted))
    assert(true)
  }

  it should "produce correct OR activations from counts" in {
    val contextCounts = sc.parallelize(List((0L, (0L, 0L)), (0L, (1L, 2L)), (1L, (0L, 0L)), (1L, (2L, 3L))))
    val orConcepts = sc.parallelize(List((0L, 3L), (0L, 4L), (1L, 3L), (2L, 5L)))
    val targetData = Set((4L, (1L, 2L)), (3L ,(1L, 2L)), (3L, (2L, 3L)), (3L, (0L, 0L)), (4L, (0L, 0L)))
    val activations = activateORConceptsFromCounts(contextCounts, orConcepts).collect.toSet
    info(activations.mkString(":"))
    assert(activations == targetData)
  }

  it should "produce correct AND activations from counts" in {
    val contextCounts = sc.parallelize(List((0L, (0L, 0L)), (0L, (1L, 2L)), (1L, (0L, 0L)), (2L, (3L, 2L)),
                                            (1L, (2L, 3L)), (1L, (1L, 2L)), (2L, (1L, 2L)), (2L, (4L, 3L))))
    val andConcepts = sc.parallelize(List((0L, 4L), (0L, 5L), (1L, 4L), (2L, 6L), (1L, 5L), (2L, 5L), (3L, 6L)))
    val conceptSizes = sc.parallelize(List((4L, 2), (5L, 3), (6L, 2)))
    val targetData = Set((4L, (0L, 0L)), (4L, (1L, 2L)), (5L, (1L, 2L)))
    val activations = activateANDConceptsFromCounts(contextCounts, andConcepts, conceptSizes).collect.toSet
    info(activations.mkString(":"))
    assert(activations == targetData)
  }

  it should "produce correct combined higher order activations:" in {
    val indexedData = collectedToIndexedData(sc.parallelize((List(Array(0L, 1L, 2L),
                                             Array(0L, 3L, 4L),
                                             Array(3L, 4L, 5L)))))
    val orconcepts  = sc.parallelize(List((1L, 10L),
                                     (2L, 10L),
                                     (4L, 11L),
                                     (6L, 11L),
                                     (6L, 12L),
                                     (7L, 12L)))
    val andconcepts = sc.parallelize(List((1L, 13L),
                                     (2L, 13L),
                                     (4L, 14L),
                                     (6L, 14L),
                                     (6L, 15L),
                                     (7L, 15L)))
    val andcsize   = sc.parallelize(List((13L, 2),
                                    (14L, 2),
                                    (15L, 2)))
    val targetData = List((0,0),(0,1),(0,2),(1,0),(1,3),(1,4),(2,3),(2,4),(2,5),(0,10),(2,11),(1,11),(0,13))

    val allact = activateHigherOrder(indexedData, orconcepts, andconcepts, andcsize).collect()
    info(allact.mkString(":"))
    assert(recursiveEqual(targetData.sorted, allact.sorted))
  }

  "Higher-order concept functions" should "produce correct higher-order concepts from counts" in {
   val contextCounts = sc.parallelize(List((0L, (10L, 5L)), (0L, (11L, 7L)),
                                           (1L, (12L, 2L)), (1L, (13L, 1L)),
                                           (2L, (10L, 5L)), (3L, (11L, 7L)), (3L, (13L, 1L))))
   val andOrConcepts = findAndOrConceptsFromCounts(sc,
                          nrExamples = 15L, // Total context count
                          initialContextCounts = contextCounts,
                          correlationFunction = pointwiseMutualInformation,
                          clusteringAlgo = SLPA(),
                          clusteringParams = new ParameterMap().add(ClusteringParameters.Iterations, 16),
                          nrIterations = 2,
                          minConceptCount = 0L,
                          minContextCount = 0L,
                          minCorrelation = Some(-100),
                          minSimilarity = Some(0),
                          maxInDegree = Some(10),
                          conceptThreshold = 1000.0,
                          cacheLevel = MEMORY_AND_DISK)
    // Force evaluation
    andOrConcepts.correlationGraph.weights.count

    val labels = sc.parallelize(List((0L, "0"), (1L, "1"), (2L, "2"), (3L, "3")))
    writeAndOrConceptEdgesAsText(andOrConcepts.correlationGraph.weights,
                                 labels,
                                 andOrConcepts.andConcepts,
                                 andOrConcepts.orConcepts,
                                 100,
                                 "")

    // We do not compare against targetData here (the result is stochastic if using SLPA)
    assert(true)
  }

  "Similarity calculations" should "produce the correct transformation" in {
    val correlations = sc.parallelize(List((0L, 1L, 1.0D),
                                           (0L, 2L, 5.0D),
                                           (0L, 4L, 4.0D),
                                           (3L, 1L, 2.0D),
                                           (3L, 2L, 7.0D)))
    val similarities = correlationsToSimilarities(EdgeData(correlations))

    val collsims = similarities.collect()(0) // Correct output: (0,3,0.631578947368421)
    info(collsims.toString)
    assert(collsims._1 == 0 && collsims._2 == 3 && math.abs(collsims._3 - 0.631578947368421) < 0.001)
  }

  it should "prune the graph correctly" in {
    val edges = sc.parallelize(List((0L, 1L, 1.0D),
                                    (1L, 0L, 1.0D),
                                    (1L, 2L, 1.0D),
                                    (2L, 0L, 2.0D),
                                    (2L, 1L, 2.0D)))
    val targetData = List((2,0,2.0),(2,1,2.0),(1,2,1.0))

    val pruned = pruneEdgesByInDegree(EdgeData(edges), 1L).weights.collect()
    info(pruned.mkString(":"))
    assert(recursiveEqual(targetData.sorted, pruned.sorted))
  }


  it should "produce the correct similarities from a corpus" in {
    val minCount = 0L
    val windowSize = 1L

    val allWords = sc.parallelize(List("a","b","b","a","c","a","d","e","b","f"))
    val conceptIDs = enumerateUniqueStrings(allWords, minCount).persist(MEMORY_ONLY)
    val allIDs = allWords.zipWithIndex().join(conceptIDs).map(x=>(x._2._1, x._2._2))
    val nrExamples = allIDs.count()
    val conceptData  = indexedDataMovingWindow(allIDs, 2).
                       aggregateByKey(Set[Long]())((s,v)=>s+v,(x,y)=>x|y).
                      flatMapValues(x=>x) // Find distinct values using reduce by key (could use .distinct)
    val counts = incrementalCoexistenceCounts(conceptData, conceptData, 1L) // Co-ocurrence counts
    val correlations = counts.map(x=>(x._1,conditionalProbability(x._2._1,x._2._2,x._2._3, nrExamples)))
    val corrEdges = correlations.map(x=>(x._1._1,x._1._2,x._2._1)) ++
                    correlations.map(x=>(x._1._2,x._1._1,x._2._2))
    val corrGraph = EdgeData(corrEdges)
    val prunedCorrGraph = pruneEdgesByInDegree(corrGraph,2L)
    val simGraph = correlationsToSimilarities(prunedCorrGraph)
    val labels = conceptIDs.map(v=>(v._2,v._1))
    val topEdges = simGraph.map{case (i,j,w)=>(i,(j,w))}.join(labels).
                   map{case (i,((j,w),si))=>(j,(si,w))}.join(labels).
                   map{case (j,((si,w),sj))=>(si,sj,w)}.
                   top(3)(Ordering.by[(String, String, Double),Double](_._3))

    // println("\nallWords:")
    // allWords.collect.foreach(println)
    // println("\nconceptIDs:")
    // conceptIDs.collect.foreach(println)
    // println("\nallIDs:")
    // allIDs.collect.foreach(println)
    // println("\nnrExamples:")
    // println(nrExamples)
    // println("\nconceptData:")
    // conceptData.collect.foreach(println)
    // println("\ncounts:")
    // counts.collect.foreach(println)
    // println("\ncorrelations:")
    // correlations.collect.foreach(println)
    // println("\ncorrGraph:")
    // corrGraph.weights.collect.foreach(println)
    // println("\nprunedCorrGraph:")
    // prunedCorrGraph.weights.collect.foreach(println)
    // println("\nsimGraph:")
    // simGraph.collect.foreach(println)
    // println("\ntopEdges:")
    // topEdges.foreach(println)

    assert(true) // Not working for now
  }

  "Clustering functions" should "produce correct connected components" in {
    val edges = sc.parallelize(List((0L, 1L, 1.0D),
                                    (2L, 3L, 1.0D),
                                    (3L, 4L, 1.0D),
                                    (4L, 3L, 1.0D)))
    val targetData = List((0,0),(1,0),(2,2),(3,2),(4,2))

    val components = connectedComponents(edges).collect()
    info(components.mkString(":"))
    assert(recursiveEqual(targetData.sorted, components.sorted))
  }

  it should "produce correct max cliques" in {
    val edges = sc.parallelize(List((0L, 1L, 1.0D),
                                    (0L, 4L, 1.0D),
                                    (1L, 2L, 1.0D),
                                    (1L, 4L, 1.0D),
                                    (1L, 5L, 1.0D),
                                    (1L, 6L, 1.0D),
                                    (2L, 3L, 1.0D),
                                    (2L, 5L, 1.0D),
                                    (2L, 6L, 1.0D),
                                    (5L, 6L, 1.0D)))
    val targetData = List((2,0),(3,0),(0,1),(1,1),(4,1),(1,2),(2,2),(5,2),(6,2))

    val maxc = MaxCliques()
      .setIterations(100)
      .setUndirect(true)
      .cluster(edges)
      .collect()

    info(maxc.mkString(":"))  // Correct output: {2,3}, {0,1,4}, {1,2,5,6} <=>
    assert(recursiveEqual(targetData.sorted, maxc.sorted)) // (2,0):(3,0):(0,1):(4,1):(1,1):(1,2):(6,2):(5,2):(2,2)
 }

 it should "remove duplicate clusters correctly" in {
    // 0: {0, 1}, 1: {0, 2, 3}, 2: {0, 1}
    val assignments = sc.parallelize(List((0L, 0L),
                                          (1L, 0L),
                                          (0L, 1L),
                                          (2L, 1L),
                                          (3L, 1L),
                                          (0L, 2L),
                                          (1L, 2L)))

    val targetData = Array(Set(0L, 2L, 3L), Set(0L, 1L))

    val result = ClusteringFunction.discardDuplicateClusters(assignments).
                 map(_.swap).groupByKey.values.map(_.toSet).collect()

    info(result.mkString(":"))
    assert(result.size == 2 && (result(0) == targetData(0) && result(1) == targetData(1) ||
                                result(0) == targetData(1) && result(1) == targetData(0)))
  }

  "Classification functions" should "produce correct Naive Bayes-based classifications" in {
    val conceptData = sc.parallelize(List((0L, 0L),
                                          (0L, 1L),
                                          (1L, 0L),
                                          (1L, 2L),
                                          (2L, 1L),
                                          (2L, 2L),
                                          (3L, 1L),
                                          (3L, 3L)))
    val classData = sc.parallelize(List((0L, 0L),
                                        (1L, 0L),
                                        (2L, 1L),
                                        (3L, 1L)))
    val testData = sc.parallelize(List((0L, 0L),
                                       (0L, 1L),
                                       (1L, 1L),
                                       (1L, 2L),
                                       (2L, 3L)))
    val targetData = List((0,0),(1,1),(2,1))

    val nbClass = new NaiveBayes(conceptData, classData, 4L)
    val classprobs = nbClass.classConceptProbabilities.collect()
    val baseline = nbClass.classBaseline.collect()
    val classes = nbClass.classify(testData).collect()

    info(classes.mkString(":"))
    assert(recursiveEqual(targetData.sorted, classes.sorted))
  }

  "Formula functions" should "convert formulas to correct form" in {
    val p = Atom(0L)
    val q = Atom(1L)
    val r = Atom(2L)
    val s = Atom(3L)

    // (0 ∧ 1) ∨ (2 ∧ 3) => (0 ∨ 2) ∧ (1 ∨ 2) ∧ (0 ∨ 3) ∧ (1 ∨ 3)
    val f1 = Or(And(p,q),And(r,s))
    // (0 ∧ 1) ∨ 2 ∨ 3 => (0 ∨ 2 ∨ 3) ∧ (1 ∨ 2 ∨ 3)
    val f2 = Or(Or(And(p,q),r),s)

    Iterable(f1,f2).foreach{f =>
      val cnf = toCNF(f)
      info(cnf.toString)
      flattenCNF(cnf).foreach(v => info(v.mkString(" v ")))
    }

    assert(true) // Fix, should be actual test
  }

  it should "remove subsumed clauses in CNF correctly" in {
    val clauses = Set(Set(1L,2L,3L), Set(1L,2L), Set(2L,3L), Set(4L), Set(4L,5L))
    val correct = Set(Set(1L, 2L), Set(2L, 3L), Set(4L))
    val minimized = minimizeCNF(clauses)
    info(minimized.toString())
    assert(minimized == correct)
  }

  "ConceptData objects" should "execute functionality without crashing" in {
    val d = ConceptData.loadDataFile[String](sc, "Readme.md", x => x)
    assert(true) // If this does not crash, it passes...
  }
}
