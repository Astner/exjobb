package se.sics.concepts.core

import java.io._

import org.apache.spark.SparkContext._
import org.apache.spark._

class Experiments(@transient sc: SparkContext) extends Serializable {

  // Set experiment id as current time stamp in seconds
  val id: Long = System.currentTimeMillis/1000

  /** Writes aggregate sizes for different saturation exponents to CSV file
    * for further post-processing (e.g. using R or Python) to determine how
    * robust the clustering algorithm is with regard to the saturation parameter.
    */
  def clusterSizesPerSaturation(
    graph: CGraph,
    numberOfIterations: Int,
    minSaturation: Int,
    maxSaturation: Int,
    outPath: String): Unit = {

    val saturationExponents = List.range(minSaturation, maxSaturation + 1).map(_.toDouble)

    def aggregateSizes(s: Double) =
      graph.clustersBySimilarity(numberOfIterations, useWeights = true, s)
      .map{case (k, v) => (v, k)}.groupByKey
      .map{case (k, iter) => (s, iter.size)}
      .collect

    val file = new PrintWriter(new File(outPath + "aggregate-sizes-" + id.toString + ".txt"))
    saturationExponents.foreach(s => aggregateSizes(s).foreach{case (s, size) => file.write(s"${s},${size}\n")})
    file.close()
  }
}

object Experiments {
  def apply(@transient sc: SparkContext) = new Experiments(sc)
}