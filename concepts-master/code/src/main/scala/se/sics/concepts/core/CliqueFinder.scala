package se.sics.concepts.core

import java.io._

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.util.Random
import scala.math.Ordering.Implicits._

/**
 * Below is a stab at implementing a version of the maximal clique algorithm described by:
 * Svendsen, Michael Steven. Mining maximal cliques from large graphs using MapReduce. Diss. Iowa State University, 2012.
 *
 * To run the algorithm, instantiate a CliqueFinder object with the edges of the graph to enumerate maximal cliques
 * over. Then run the method findCliques which returns an RDD of all the maximal cliques in the graph. This procedure
 * is exponential in the worst case but is efficient for graphs of low degree.
 *
 * @param edges: The edges of the graph. The current implementation assumes the graph is undirected. Only one of the
 *               edges between nodes needs to be present.
 * @param sc: spark context
 */
class CliqueFinder(edges: RDD[(Long, Long)], @transient sc: SparkContext) extends Serializable{
  // The rankList isn't used. Wee need to add the rank of nodes to a composite node object instead
  val rankList: RDD[(Long, (Int, Long))] =
    edges
      // Each edge will be counted one. This will skew the count if both directions are present for single node pairs.
      .flatMap{case (u,v) => Iterable((u,1), (v,1))}
      // Just sum up the degree
      .reduceByKey((a,b) => a+b)
      // This is just so that the second element in the list can be used as a unique ordering rank.
      .map{case (node, degree) => (node, (degree, node))}.cache()
  /**
   * Produce a ranking of each of the nodes. The rank should be unique, such that the nodes can always be totally ordered.
   * @param nodes: The nodes to calculate the rank for.
   * @param subGraph: The graph to use for deciding the rank.
   * @return A Map with node as keys and rank as values. The rank is a tuple with the degree of the node and the id of the node to break ties.
   */
  def calculateDegreeRank(nodes: Set[Long], subGraph: Iterable[(Long, Long)]): Map[Long, (Int, Long)] = {
    // Order the nodes of the subgraph according to degree

    nodes.map { case node => (node, (subGraph.count{case (u,v) => node == u || node == v}, node))}.toMap
  }

  /**
   * Produce a ranking of each of the nodes. The rank should be unique, such that the nodes can always be totally
   * ordered. This is a simple naive ranking which only returns the id of the node as the rank.
   * @param nodes: The nodes to calculate the rank for.
   * @param subGraph: The graph to use for deciding the rank.
   * @return A Map with node as keys and rank as values. The rank is just the id of the node.
   */
  def calculateIdRank(nodes: Set[Long], subGraph: Iterable[(Long, Long)]): Map[Long, Long] = {
    // Order the nodes of the subgraph according to degree
    nodes.map{ node => (node, node)}.toMap
  }

  def neighbours(node: Long, subgraph: Iterable[(Long, Long)]): Set[Long] = {
    val neighbours = subgraph.filter({case (u,v) => u == node || v == node }).map(edge => pickNeighbour(node, edge)).toSet
    neighbours
  }

  /**
   * Select a pivot node from the union of cand and fini which maximizes the size of the
   * intersection of cand and neighbourhood(pivot).
   * @param cand: The candidate set.
   * @param fini: The set of finnished nodes.
   * @param subGraph: The subgraph in question.
   * @return The node which has the maximum number of neighbours in the intersection with cand.
   */
  def selectPivot(cand: Set[Long], fini: Set[Long], subGraph: Iterable[(Long, Long)]): Long = {
    // We create a tuple for each node, where the first element is the size of the nodes neighbourhood
    // intersecting with cand. The second element is the node itself.
    val neighbourCandIntersections = cand.union(fini)
      .map(u => ((neighbours(u, subGraph) & cand).size, u))
    // We pick the node with the highest intersection
    val maxPair = neighbourCandIntersections.max(Ordering[(Int,Long)])
    //Return the node
    maxPair._2
  }


  def tomita(subGraph: Iterable[(Long, Long)], k: Set[Long], cand: Set[Long], fini: Set[Long]): ArrayBuffer[Iterable[Long]] = {
    // This method is pretty much an atrocity in conversions to and from mutable objects.
    var cliques = scala.collection.mutable.ArrayBuffer.empty[Iterable[Long]]

    if (cand.isEmpty && fini.isEmpty) {
      //If both cand and fini is empty we have extended the clique k as much as possible
      cliques.append(k)
    }
    else {
      //Is this how you do it? We need the sets to be mutable in this function. TODO: This is an ugly thing. Fix it.
      var kMut: scala.collection.mutable.Set[Long] = k.to[scala.collection.mutable.Set]
      var candMut:scala.collection.mutable.Set[Long] = cand.to[scala.collection.mutable.Set]
      var finiMut: scala.collection.mutable.Set[Long] = fini.to[scala.collection.mutable.Set]

      // The pivot is used to speed up the search according to Tomitas algorithm
      val pivot = selectPivot(cand.to[scala.collection.immutable.Set], fini.to[scala.collection.immutable.Set], subGraph)

      val ext = cand -- neighbours(pivot, subGraph)
      ext.foreach(q => {
        val k_q = kMut + q
        val neighbours_q = neighbours(q, subGraph)
        val cand_q = candMut & neighbours_q
        val fini_q = finiMut & neighbours_q
        val sub_cliques = tomita(subGraph,
          k_q.to[scala.collection.immutable.Set],
          cand_q.to[scala.collection.immutable.Set],
          fini_q.to[scala.collection.immutable.Set])
        if (sub_cliques.nonEmpty) cliques ++= sub_cliques
        candMut -= q
        finiMut += q
        kMut -= q
      })
    }
    cliques
  }

  /**
   * Tries to find all cliques in the given local graph.
   * @param localGraph A tuple with the root node for the local graph as first element and the edges of the graph as
   *                   the second.
   * @return Returns an iterable of the found cliques.
   */
  def findLocalCliques(localGraph: (Long, Set[(Long, Long)])): Iterable[Iterable[Long]] = localGraph match {
    // We'll do the reduce step of PECO as described by Svendsen
    case (node: Long, subGraph: Set[(Long, Long)]) =>
      // We need the nodes as a set for building the rank map
      val nodes: Set[Long] = subGraph.flatMap({case (u,v) => Iterator(u,v)})
      // The rank map is used to lookup the rank of a node. The rank is used to determine which node is responsible for
      // finding a certain clique. It ensures that each clique is only enumerated once, by the node with the
      // lowest rank.
      // The ranks should be the same regardless of which local graph we look at, therefore we can't use the local
      // subgraph to calculate degrees since it will vary depending on which node we are rooted at, and indeed often
      // causes the the rank of most nodes to be less than the rooted node, which in turns means that they are
      // uncorrectly added to the finnished set.
      val rankMap = calculateIdRank(nodes, subGraph)

      // getOrElse is pretty much a hack. rankMap will contain all nodes, so will never fail.
      val nodeRank = rankMap.getOrElse(node, 0L)
      // Fini should contain all nodes whose rank is less than ours. We won't be looking at those, the search with
      // those nodes as the root node will find the cliques with them.
      val fini: Set[Long] = nodes.filter(neighbour => rankMap.getOrElse(neighbour, 0L) < nodeRank)
      val cand: Set[Long] = (nodes -- fini) - node
      val k = Set(node)
      tomita(subGraph, k, cand, fini)
  }


  def pickNeighbour(node: Long, edge: (Long,Long)): Long = edge match {
    case (u,v) => if (node == u) v else u
  }

  def induceSubGraph(nodes: Set[Long], graph: Set[(Long, Long)]): Set[(Long, Long)] = {
    graph.filter{ case (u, v) => nodes.contains(u) && nodes.contains(v) }
  }

  /**
   * Given the pair of (node, neighbourhood(node)) we create pairs with
   * (neighbour \in neighbourhood(node), neighbourhood(node)).
   * These second tuples can then be used for a node to collect the
   * neighbourhoods where the clique it belongs to must exist.
   * @param neighbourhoods: A pair with the node as the first element and the neighbourhood of the node as the second.
   * @return An iterable of pairs with the neighbours of the node as the first element and the neighbourhood as the
   *         second.
   */
  def collectNeighboursNeighbourhoods(neighbourhoods: RDD[(Long, Set[(Long, Long)])]): RDD[(Long, Set[(Long, Long)])] = {
    // For each neighbour we create a pair with the neighbour as key and the neighbourhood as the value. Later these
    // neighbourhoods will be collected and Bron-Kerbosch will be applied to the intersection of the nodes own
    // neighbourhood and that of it's neighbours. Any cliques the node beLongs to must exist in that neighbourhood.
    val neighboursNeighbours = neighbourhoods.flatMap { case (node, neighbours) => neighbours.map{case edge => (pickNeighbour(node, edge), neighbours)}}
    val reducedNeighboursNeighbours = neighboursNeighbours.reduceByKey((s1, s2) => s1 ++ s2)
    reducedNeighboursNeighbours
  }

  /**
   * Thakes the iterables of the nodes neighbourhood and its neighbours neigbhourhoods and keeps the edges which belong
   * to the local subgraph. What we're looking for is basically the subgraph induced by the node and it's imediate
   * neighbours.
   */
  def makeSubgraphs(neighbours: RDD[(Long, Set[Long])], neighboursNeighbours: RDD[(Long, Set[(Long, Long)])]): RDD[(Long, Set[(Long, Long)])] = {
    //localNeighbourhoods: (Long, (Set[(Long, Long)], Iterable[Set[(Long, Long)]]))): (Long, Iterable[(Long, Long)]) = localNeighbourhoods match {
    neighbours.join(neighboursNeighbours).mapValues{case (localNeighbours, potentialSubgraph) => induceSubGraph(localNeighbours, potentialSubgraph) }
  }

  def findCliques(): RDD[Iterable[Long]] = {
    //The neighbourhood is all the edges containing a certain node, therefore we map each edge to both of it's nodes.
    val neighbourhoods: RDD[(Long, Set[(Long, Long)])] =
      edges.flatMap{case (u, v) => Iterable((u, Set((u,v))), (v, Set((u, v))))}.reduceByKey({case (s1, s2) => s1 ++ s2})

    // Each of the pairs have the node as the first component and their neighbourhoods as the second component.
    // We'd like to tell each node in our neighbourhood about our neigbourhood. Then every node will know about their
    // neighbours neighbours and this can be used to induce the subgraph where the clique that node beLongs to must exist.
    val neighboursNeighbourhoods: RDD[(Long, Set[(Long, Long)])] = collectNeighboursNeighbourhoods(neighbourhoods)
    val neighbourSets: RDD[(Long, Set[Long])] = neighbourhoods.mapValues(s => s.flatMap(edge => Set(edge._1, edge._2)))
    val subgraphs: RDD[(Long, Set[(Long, Long)])] = makeSubgraphs(neighbourSets, neighboursNeighbourhoods)

    // localNeighbourhoods now contains an iterable of iterables with edges for each node, those are the neighbourhoods of it's
    // neighbours. Now we wan't to produce the local subgraphs by taking the union of the intersections of these
    // neighbourhoods and our own neighbourhood.


    val localCliques = subgraphs.flatMap(findLocalCliques)
    localCliques.filter(_.nonEmpty)
  }
}

object CliqueFinder {
  def main(args: Array[String]) {
    //val inputPath = "data/benchmarks/MTURK-771/MTURK-771.csv"

    val noCores = 6

    // Configure and initialize Spark
    val conf = new SparkConf()
      .setMaster("local["+noCores.toString+"]")
      .setAppName("CNCPT BillionBigrams")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.compress", "true")
      .set("spark.default.parallelism", (noCores*3).toString)
      .set("spark.shuffle.memoryFraction", 0.4.toString)
      .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig
    // .set("spark.storage.memoryFraction", (0.4).toString)


    val sc = new SparkContext(conf)
    val edges1 = Array(
      //Three overlapping cliqes of different size
      //First clique
      (1, 2),
      (1, 3),
      (2, 3),
      //Second clique
      (2, 4),
      (2, 5),
      (3, 4),
      (3, 5),
      (4, 5),
      // Third clique
      (3, 6),
      (3, 7),
      (4, 6),
      (4, 7),
      (5, 6),
      (5, 7),
      (6, 7)
    )
    val testGraph1 = sc.parallelize(edges1.map{case (u,v) => (u.toLong, v.toLong)})

    val edges2 = Array(
    //Two 4-cliques connected at node 3 and 4
    //First clique
      (1,2),
      (1,3),
      (1,4),
      (2,3),
      (2,4),
      (3,4),
    //Second clique
      (3,5),
      (3,6),
      (4,5),
      (4,6),
      (5,6)
    )
    val testGraph2 = sc.parallelize(edges2.map{case (u,v) => (u.toLong, v.toLong)})

    val edges3 = Array(
      // Two almost fully overlapping cliques, only one node differs between them
      (1,2),
      (1,3),
      (1,4),
      (2,3),
      (2,4),
      (3,4),
      (1,5),
      (2,5),
      (3,5),
      (4,5),
      (1,6),
      (2,6),
      (3,6),
      (4,6)
    )
    val testGraph3 = sc.parallelize(edges3.map{case (u,v) => (u.toLong, v.toLong)})

    // Bloody awful conversion from words to Long ids
//    val wordPairs = sc.textFile(inputPath).map(_.split(",").take(2)).map(arr => (arr(0), arr(1)))
//    val wordsIds = wordPairs.flatMap({case (u, v) => Iterable(u,v)}).distinct().zipWithUniqueId().collectAsMap()
//    val edges = wordPairs.map({case (u,v) => (wordsIds.getOrElse(u, 0L), wordsIds.getOrElse(v, 0L))})

    val cliqueFinder1 = new CliqueFinder(testGraph1, sc)
    val graphs1 = cliqueFinder1.findCliques().collect()
    print(graphs1)

    val cliqueFinder2 = new CliqueFinder(testGraph2, sc)
    val graphs2 = cliqueFinder2.findCliques().collect()
    print(graphs2)

    val cliqueFinder3 = new CliqueFinder(testGraph3, sc)
    val graphs3 = cliqueFinder3.findCliques().collect()
    print(graphs3)

    sc.stop()
  }
}
