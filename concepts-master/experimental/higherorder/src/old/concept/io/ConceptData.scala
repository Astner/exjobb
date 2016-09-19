/*
 Copyright (C) 2015 Daniel Gillblad, Olof Görnerup , Theodoros Vasiloudis (dgi@sics.se,
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

package se.sics.concepts.io

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import se.sics.concepts.graphs.ConceptIDs._
import se.sics.concepts.io.ConceptsIO._
import se.sics.concepts.io.ProductIO._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Storage and utility class for data used and produced in concept calculations.
  *
  * Stores data sets used around concept calculations and provides standardized
  * methods for loading and writing these data sets to standardized file names on
  * a set path. All data sets are represented with Option values, where None
  * represents not available / not specified.
  *
  * @param concepts The mapping between basic concept IDs and their String representation
  * @param contexts Context data stored as (DataIndex, ConceptID) pairs
  * @param count The number of examples in the context data
  * @param correlations Stores the correlation between concepts
  * @param similarities Stores the calculated similarity between concepts
  * @param recurrentConcepts All used recurrent concepts stored as pairs of the ID of the
  * recurrent concept and its constituent concept along with the number of recurrences
  * @param andConcepts And concepts
  * @param orConcepts Or concepts
  * @param path If specified, the path files were loaded from / will be written to.
  */
class ConceptData(val concepts          : Option[RDD[(ConceptID, String)]],
                  val contexts          : Option[RDD[(DataIndex, ConceptID)]],
                  val count             : Option[ConceptCount],
                  val contextCounts     : Option[RDD[(ConceptID, (ContextID, ContextCount))]],
                  val correlations      : Option[RDD[(ConceptID, ConceptID, Weight)]],
                  val similarities      : Option[RDD[(ConceptID, ConceptID, Weight)]],
                  val corrClusters      : Option[RDD[(ConceptID, ClusterID)]],
                  val simClusters       : Option[RDD[(ConceptID, ClusterID)]],
                  val recurrentConcepts : Option[RDD[(ConceptID,(ConceptID, ConceptCount))]],
                  val andConcepts       : Option[RDD[(ConceptID, ConceptID)]],
                  val andConceptLengths : Option[RDD[(ConceptID, ConceptCount)]],
                  val orConcepts        : Option[RDD[(ConceptID, ConceptID)]],
                  val path              : Option[String]) {
  /** Write all specified data sets.
    *
    * Returns new ConceptData object containing the same datasets as that of this
    * object, with the data sets specified as arguments replaced with new data sets.
    * These new data sets will be written to their standard locations. If the files
    * already exist, they are overwritten.
    */
  def write(sc                   : SparkContext,
            newConcepts          : Option[RDD[(ConceptID, String)]]       = None,
            newContexts          : Option[RDD[(DataIndex, ConceptID)]]         = None,
            newCount             : Option[ConceptCount]                      = None,
            newContextCounts     : Option[RDD[(ConceptID, (ContextID, ContextCount))]] = None,
            newCorrelations      : Option[RDD[(ConceptID, ConceptID, Weight)]] = None,
            newSimilarities      : Option[RDD[(ConceptID, ConceptID, Weight)]] = None,
            newCorrClusters      : Option[RDD[(ConceptID, ClusterID)]]         = None,
            newSimClusters       : Option[RDD[(ConceptID, ClusterID)]]         = None,
            newRecurrentConcepts : Option[RDD[(ConceptID, (ConceptID, ConceptCount))]]  = None,
            newAndConcepts       : Option[RDD[(ConceptID, ConceptID)]]         = None,
            newAndConceptLengths : Option[RDD[(ConceptID, ConceptCount)]]          = None,
            newOrConcepts        : Option[RDD[(ConceptID, ConceptID)]]         = None,
            newPath              : Option[String]                    = None) : ConceptData = {
    val lpath = if (newPath.isEmpty) path.get else ConceptData.cleanPath(newPath.get)

    val retConcepts = if (newConcepts.isEmpty) concepts else
      Some(ConceptData.writeDataFile(lpath + ConceptData.conceptsFileName, newConcepts.get))
    val retContexts = if (newContexts.isEmpty) contexts else
      Some(ConceptData.writeDataFile(lpath + ConceptData.contextsFileName, newContexts.get))
    val retCorr = if (newCorrelations.isEmpty) correlations else
      Some(ConceptData.writeDataFile(lpath + ConceptData.correlationsFileName, newCorrelations.get))
    val retSim = if (newSimilarities.isEmpty) similarities else
      Some(ConceptData.writeDataFile(lpath + ConceptData.similaritiesFileName, newSimilarities.get))
    val retCorrClu = if (newCorrClusters.isEmpty) corrClusters else
      Some(ConceptData.writeDataFile(lpath + ConceptData.corrClustersFileName, newCorrClusters.get))
    val retSimClu = if (newSimClusters.isEmpty) simClusters else
      Some(ConceptData.writeDataFile(lpath + ConceptData.simClustersFileName, newSimClusters.get))
    val retRecurrent = if (newRecurrentConcepts.isEmpty) recurrentConcepts else
      Some(ConceptData.writeDataFile(lpath + ConceptData.recurrentFileName, newRecurrentConcepts.get))
    val retAnd = if (newAndConcepts.isEmpty) andConcepts else
      Some(ConceptData.writeDataFile(lpath + ConceptData.andFileName, newAndConcepts.get))
    val retAndLen = if (newAndConceptLengths.isEmpty) andConceptLengths else
      Some(ConceptData.writeDataFile(lpath + ConceptData.andLengthFileName, newAndConceptLengths.get))
    val retOr = if (newOrConcepts.isEmpty) orConcepts else
      Some(ConceptData.writeDataFile(lpath + ConceptData.orFileName, newOrConcepts.get))
    val retCount = if (newCount.isEmpty) count else
      Some(ConceptData.writeValueFile(sc, lpath + ConceptData.countFileName, newCount.get))
    val retContextCounts = if (newContextCounts.isEmpty) contextCounts else
      Some(ConceptData.writeDataFile(lpath + ConceptData.contextCountsFileName, newContextCounts.get))

    ConceptData(retConcepts, retContexts, retCount, retContextCounts, retCorr, retSim, retCorrClu, retSimClu, 
      retRecurrent, retAnd, retAndLen, retOr, Some(lpath))
  }

  /** Inspect available concept data.
    *
    * Prints information on the available concept data on an easy to read format.
    */
  def inspect() : Unit = {
    println("# Concept data information")
    if (path.isEmpty) println(" (no path available)")
    else println(", " + path.get)
    if (count.isEmpty) println(" - No count information") else println(" - Count: " + count.get)
    if (concepts.isDefined) {
      print(" - Concept samples: ")
      println(concepts.get.take(5).mkString(":"))
    }
    if (contexts.isDefined) {
      print(" - Context samples: ")
      println(contexts.get.take(5).mkString(":"))
    }
    if (contextCounts.isDefined) println(" - ContextCounts available")    
    if (correlations.isDefined) println(" - Correlations available")
    if (similarities.isDefined) println(" - Similarities available")
    if (corrClusters.isDefined) println(" - Correlation clusters available")
    if (simClusters.isDefined) println(" - Similarity clusters available")
    if (recurrentConcepts.isDefined) println(" - Recurrent concepts available")
    if (andConcepts.isDefined) println(" - And concepts available")
    if (orConcepts.isDefined) println(" - Or concepts available")
  }
}

/** Companion object for class [[ConceptData]].
  *
  * Contains associated functionality and utility functions.
  */
object ConceptData {
  /** File name for concept data. */
  val conceptsFileName     : String = "concepts"
  /** File name for context data. */
  val contextsFileName     : String = "contexts"
  /** File name for example count information. */
  val countFileName        : String = "count"
  /** File name for context count information. */
  val contextCountsFileName : String = "context-counts"
  /** File name for correlation data. */
  val correlationsFileName : String = "correlations"
  /** File name for similarity data. */
  val similaritiesFileName : String = "similarities"
  /** File name for correlation cluster data. */
  val corrClustersFileName : String = "correlation-clusters"
  /** File name for similarity cluster data. */
  val simClustersFileName : String = "similarity-clusters"
  /** File name for recurrent concepts data. */
  val recurrentFileName    : String = "recurrent-concepts"
  /** File name for and concepts data. */
  val andFileName          : String = "and-concept-lengths"
  /** File name for and concept lengths. */
  val andLengthFileName    : String = "and-concepts"
  /** File name for or concepts data. */
  val orFileName           : String = "or-concepts"

  /** Create a new ConceptData object. */
  def apply(concepts          : Option[RDD[(ConceptID, String)]] = None,
            contexts          : Option[RDD[(DataIndex, ConceptID)]] = None,
            count             : Option[ConceptCount] = None,
            contextCounts     : Option[RDD[(ConceptID, (ContextID, ContextCount))]] = None,
            correlations      : Option[RDD[(ConceptID, ConceptID, Weight)]] = None,
            similarities      : Option[RDD[(ConceptID, ConceptID, Weight)]] = None,
            corrClusters      : Option[RDD[(ConceptID, ClusterID)]] = None,
            simClusters       : Option[RDD[(ConceptID, ClusterID)]] = None,
            recurrentConcepts : Option[RDD[(ConceptID, (ConceptID, ConceptCount))]] = None,
            andConcepts       : Option[RDD[(ConceptID, ConceptID)]] = None,
            andConceptLengths : Option[RDD[(ConceptID, ConceptCount)]] = None,
            orConcepts        : Option[RDD[(ConceptID, ConceptID)]] = None,
            path              : Option[String] = None) : ConceptData = {
    new ConceptData(concepts, contexts, count, contextCounts, correlations, similarities, corrClusters, simClusters,
                    recurrentConcepts, andConcepts, andConceptLengths, orConcepts, path)
  }

  /** Clean up specified path string.
    *
    * Returns a possibly modified and expanded path string suitable for use with
    * the standard file names.
    */
  def cleanPath(path : String) : String =
    if (path(path.length - 1) == '/') path else path + '/' // Make sure string ends with / (win: no, won't work)

  /** Load all available concept data at specified path.
    *
    * Returns a [[ConceptData]] object containing all data sets available by their standardized file
    * names at the specified path.
    */
  def load(sc: SparkContext, path : String) : ConceptData = {
    if (Option(path).getOrElse("").isEmpty) ConceptData() // If string is null or empty, just return None:s
    val lpath = cleanPath(path)
    ConceptData(
      // Load concepts
      loadDataFile[(ConceptID, String)](sc, lpath + conceptsFileName, fromSimpleString[ConceptID, String]),
      // Load contexts
      loadDataFile[(DataIndex, ConceptID)](sc, lpath + contextsFileName, fromSimpleString[DataIndex, ConceptID]),
      // Load counts
      loadValueFile[ConceptCount](sc, lpath + countFileName, s => s.toLong),
      // Load context counts
      loadDataFile[(ConceptID, (ContextID, ContextCount))](sc, lpath + contextCountsFileName,
                                      s=> fromSimpleString[ConceptID, ContextID, ContextCount](s) match
                                      { case (a,b,c) => (a,(b,c)) }),
      // Load correlations
      loadDataFile[(ConceptID, ConceptID, Weight)](sc, lpath + correlationsFileName,
                                       fromSimpleString[ConceptID, ConceptID, Weight]),
      // Load similarities
      loadDataFile[(ConceptID, ConceptID, Weight)](sc, lpath + similaritiesFileName,
                                       fromSimpleString[ConceptID, ConceptID, Weight]),
      // Load correlation clusters
      loadDataFile[(ConceptID, ClusterID)](sc, lpath + corrClustersFileName,
                                       fromSimpleString[ConceptID, ClusterID]),
      // Load similarity clusters
      loadDataFile[(ConceptID, ClusterID)](sc, lpath + simClustersFileName,
                                       fromSimpleString[ConceptID, ClusterID]),
      // Load recurrences
      loadDataFile[(ConceptID, (ConceptID, ConceptCount))](sc, lpath + recurrentFileName,
                                      s=> fromSimpleString[ConceptID, ConceptID, ConceptCount](s) match
                                      { case (a,b,c) => (a,(b,c)) }),
      // Load AND concepts
      loadDataFile[(ConceptID, ConceptID)](sc, lpath + andFileName, fromSimpleString[ConceptID,ConceptID]),
      // Load AND concept lengths
      loadDataFile[(ConceptID, ConceptCount)](sc, lpath + andLengthFileName, fromSimpleString[ConceptID, ConceptCount]),
      // Load OR concepts
      loadDataFile[(ConceptID, ConceptID)](sc, lpath + orFileName, fromSimpleString[ConceptID,ConceptID]),
      Some(lpath))
  }

  /** Write all specified data sets.
    *
    * Returns a new [[ConceptData]] object containing the specified data sets.
    * These data sets are be written to their standard locations. If the files
    * already exist, they are overwritten.
    */
  def write(sc: SparkContext,
            concepts          : Option[RDD[(ConceptID, String)]]       = None,
            contexts          : Option[RDD[(DataIndex, ConceptID)]]         = None,
            count             : Option[ConceptID]                      = None,
            contextCounts      : Option[RDD[(ConceptID, (ContextID, ContextCount))]] = None,
            correlations      : Option[RDD[(ConceptID, ConceptID, Weight)]] = None,
            similarities      : Option[RDD[(ConceptID, ConceptID, Weight)]] = None,
            corrClusters      : Option[RDD[(ConceptID, ClusterID)]]         = None,
            simClusters       : Option[RDD[(ConceptID, ClusterID)]]         = None,
            recurrentConcepts : Option[RDD[(ConceptID, (ConceptID, ConceptCount))]]  = None,
            andConcepts       : Option[RDD[(ConceptID, ConceptID)]]         = None,
            andConceptLengths : Option[RDD[(ConceptID, ConceptCount)]]          = None,
            orConcepts        : Option[RDD[(ConceptID, ConceptID)]]         = None,
            path              : Option[String]                    = None) : ConceptData = {
    ConceptData().write(sc, concepts, contexts, count, contextCounts, correlations, similarities, corrClusters, 
                        simClusters, recurrentConcepts, andConcepts, andConceptLengths, orConcepts, path)
  }

  /** Load a data file.
    *
    * Load a data file to an RDD using the specified conversion function from a string to
    * the RDD data type.
    */
  def loadDataFile[T : ClassTag](
      sc: SparkContext, file : String,
      toType : (String) => T) : Option[RDD[T]] = {
    if (fileExists(sc, file)) Some(sc.textFile(file).map(toType))
    else None
  }

  /** Load a value file.
    *
    * Load file storing a single value using the specified conversion function from a string
    * to the specific value type.
    */
  def loadValueFile[T : ClassTag](sc: SparkContext, file : String,
                                  toType : (String) => T) : Option[T] = {
    if (fileExists(sc, file)) Some(toType(sc.textFile(file).take(1)(0)))
    else None
  }

  /** Write a data file.
    *
    * Write data as compressed text using the specified conversion function between the data type
    * and a string. Return the data set given as argument.
    */
  def writeDataFile[T <: Product : ClassTag](file: String, data: RDD[T], compressed: Boolean = false) : RDD[T] = {
    if (fileExists(data.context, file)) deleteFile(data.context, file)
    if (compressed) {
      writeToCompressedFile(file, data.map(v=>toSimpleString(v)))
    } else {
      writeToFile(file, data.map(v=>toSimpleString(v)))
    }
    data
  }

  /** Write a value file.
    *
    * Write a single value to the specifed file, using the specifed conversion function between the
    * value type and a string. Return the value given as argument.
    */
  def writeValueFile[T : ClassTag](sc: SparkContext, file: String, value: T) : T = {
    if (fileExists(sc, file)) deleteFile(sc, file)
    writeToFile(file, sc.parallelize(Array(value.toString)))
    value
  }

  /** Load a data file on parquet format.
    *
    * Load a data file to an RDD using the specifed conversion function from a Row to
    * the RDD data type.
    */
  def loadParquetData[T : ClassTag](sc: SparkContext, file : String,
                                    toType : (Row) => T) : Option[RDD[T]] = {
   if (fileExists(sc, file)) {
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     Some(sqlContext.read.parquet(file).rdd.map(toType))
   }
   else None
  }

  /** Write a parquet data file.
    *
    * Write data as parquet file the specified conversion function between the data type
    * and a string. Return the data set given as argument.
    */
  def writeParquetData[T <: Product](file: String, data: RDD[T])
  (implicit tt: TypeTag[T], ct: ClassTag[T]) : RDD[T] = {
    if (fileExists(data.context, file)) deleteFile(data.context, file)
    val sqlContext = new org.apache.spark.sql.SQLContext(data.context)
    //import sqlContext.implicits._
    val df = sqlContext.createDataFrame(data)
    df.write.parquet(file)
    data
  }
}
