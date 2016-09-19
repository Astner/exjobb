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

package se.sics.concepts.util

import org.apache.spark.rdd.RDD
import play.api.libs.json._
import se.sics.concepts.graphs.ConceptIDs._
import org.apache.spark.graphx._

object Statistics {

  /** (bin position, count) pairs for uniform bins between min and max values */
  def binCounts(data: RDD[Double], numBins: Int): Array[(Double, Long)] = {
    val h = data.histogram(numBins)
    h._1.zip(h._2)
  }

  /** (bin position, count) pairs in two-element arrays (JSON does not support tuples) */
  def JsHistogram(data: RDD[Double], numBins: Int): JsArray = {
    JsArray(binCounts(data, numBins).map{case (ctr, cnt) => JsArray(Seq(JsNumber(ctr), JsNumber(cnt)))})
  }

  /** Mean, standard deviation and histogram */
  def JsStatistics(data: RDD[Double], numBins: Int): JsObject = {
    val statistics = data.stats()
    JsObject(Seq(
          "mean" -> JsNumber(statistics.mean),
          "standard deviation" -> JsNumber(statistics.stdev),
          "histogram" -> JsHistogram(data, numBins)))
  }

  /** Statistics of edge weights, indegree and outdegree of graph given by edges */
  def graphStatistics(edges: RDD[(Long, Long, Double)], numBins: Int): JsObject = {
    JsObject(Seq(
          "edge count" -> JsNumber(edges.count()),
          "vertex count" -> JsNumber(edges.flatMap { case (i, j, w) => Iterable(i, j) }.distinct().count()),
          "edge weight" -> JsStatistics(edges.map(_._3), numBins),
          "indegree" -> JsStatistics(edges.map{case (i,j,w)=>(j,1D)}.reduceByKey(_+_).values, numBins),
          "outdegree" -> JsStatistics(edges.map{case (i,j,w)=>(i,1D)}.reduceByKey(_+_).values, numBins))
    )
  }

  /** Various graph measures. Note that we store the full vertex measure vectors to facilitate postprocessing. */
  def graphMeasures(edges: RDD[(ConceptID, ConceptID, Weight)]) = {
    // Sort and discard duplicate vertex pairs
    val undirectedEdges = edges.map{case (i, j, w) => if(i < j) (i, j) else (j, i)}.distinct
    // Init GraphX graph
    val undirectedGraph = Graph.fromEdgeTuples(undirectedEdges, defaultValue = 1L)
    // Count triangles passing through each vertex
    val triangleCounts = undirectedGraph.triangleCount.vertices
    // Unweighted degrees
    val degrees = undirectedGraph.degrees
    // Count number of triads per vertex
    val triadCounts = degrees.filter{case (i, d) => d > 1}.mapValues(d => d*(d-1)/2)
    // Clustering coefficients per vertex
    val localClusteringCoeffs = triangleCounts.join(triadCounts).map{case (i, (t, tt)) => t.toDouble/tt.toDouble}

    JsObject(Seq(
          "directed edge count" -> JsNumber(edges.count()),
          "undirected edge count" -> JsNumber(undirectedEdges.count()),
          "vertex count" -> JsNumber(undirectedEdges.flatMap{case (i, j) => Iterable(i, j)}.distinct.count),
          "component count" -> JsNumber(undirectedGraph.connectedComponents().vertices.values.distinct.count),
          "degrees" -> JsArray(degrees.map{case (i, d) => JsNumber(d)}.collect),
          "indegrees" -> JsArray(edges.map{case (i, j, w) => (j, 1L)}.reduceByKey(_+_).map{case (j, d) => JsNumber(d)}.collect),
          "outdegrees" -> JsArray(edges.map{case (i, j, w) => (i, 1L)}.reduceByKey(_+_).map{case (i, d) => JsNumber(d)}.collect),
          "weighted indegrees" -> JsArray(edges.map{case (i, j, w) => (j, w)}.reduceByKey(_+_).map{case (j, d) => JsNumber(d)}.collect),
          "weighted outdegrees" -> JsArray(edges.map{case (i, j, w) => (i, w)}.reduceByKey(_+_).map{case (i, d) => JsNumber(d)}.collect),
          "local clustering coefficients" -> JsArray(localClusteringCoeffs.map(JsNumber(_)).collect))
    )
  }
}
