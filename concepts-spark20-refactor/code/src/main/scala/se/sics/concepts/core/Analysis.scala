package se.sics.concepts.core

import java.io._

import org.apache.spark.SparkContext._
import org.apache.spark._

class Analysis(@transient sc: SparkContext) extends Serializable {
  /**
    * Writes vertex weights, out degrees, sums of remaining edges and sums of discarded
    * edges to a comma separated file.
    */
  def exportVertexStatistics(graph: CGraph, fileName: String): Unit = {
    val file = new PrintWriter(new File(fileName))
    file.write(s"Vertex weight, out degree, sum of remaining edges, sum of discarded edges\n")
    graph.vertexIndexWeights
      .join(graph.outDegrees)
      .leftOuterJoin(graph.remainingEdgeWeightSums)
      .leftOuterJoin(graph.discardedEdgeWeightSums)
      .collect.foreach{case (i, (((w, od), rews), dews)) =>
        file.write(s"${w},${od},${rews.getOrElse(0.0)},${dews.getOrElse(0.0)}\n")}
    file.close()
  }
}

object Analysis {
  def apply(@transient sc: SparkContext) = new Analysis(sc)
}