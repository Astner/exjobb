package com.sics.cgraph

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import play.api.libs.json._
import scala.math._
import java.io._

object Statistics {

  /** (bin position, count) pairs for uniform bins between min and max values */
  def binCounts(data: RDD[Double], numberOfBins: Int): Array[(Double, Long)] = {
    val h = data.histogram(numberOfBins)
    h._1.zip(h._2)
  }

  /** (bin position, count) pairs in two-element arrays (JSON does not support tuples) */
  def JSONHistogram(data: RDD[Double], numberOfBins: Int): JsArray = {
    JsArray(binCounts(data, numberOfBins).map{case (ctr, cnt) => JsArray(Seq(JsNumber(ctr), JsNumber(cnt)))})
  }

  /** Histogram and various statistics */
  def JSONStatistics(data: RDD[Double], numberOfBins: Int): JsObject = {
    val statistics = data.stats()
    JsObject(Seq(
          "mean" -> JsNumber(statistics.mean),
          "standard deviation" -> JsNumber(statistics.stdev),
          "sum" -> JsNumber(statistics.sum),
          "histogram" -> JSONHistogram(data, numberOfBins)))
  }
}