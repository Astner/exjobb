package com.sics.concepts

/*
 Copyright 2015 Daniel Gillblad and Olof Görnerup (dgi@sics.se,
 olofg@sics.se).

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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import com.sics.concepts.ConceptsIO._

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
class ConceptData(val concepts          : Option[RDD[(Long, String)]],
                  val contexts          : Option[RDD[(Long, Long)]],
                  val count             : Option[Long],
                  val correlations      : Option[RDD[(Long, Long, Double)]],
                  val similarities      : Option[RDD[(Long, Long, Double)]],
                  val recurrentConcepts : Option[RDD[(Long,(Long, Int))]],
                  val andConcepts       : Option[RDD[(Long, Long)]],
                  val orConcepts        : Option[RDD[(Long, Long)]],
                  val path              : Option[String]) {
  /** Write all specified data sets.
    *
    * Returns new ConceptData object containing the same datasets as that of this
    * object, with the data sets specified as arguments replaced with new data sets.
    * These new data sets will be written to their standard locations. If the files
    * already exist, they are overwritten.
    */
  def write(sc                   : SparkContext,
            newConcepts          : Option[RDD[(Long, String)]] = None,
            newContexts          : Option[RDD[(Long, Long)]] = None,
            newCount             : Option[Long] = None,
            newCorrelations      : Option[RDD[(Long, Long, Double)]] = None,
            newSimilarities      : Option[RDD[(Long, Long, Double)]] = None,
            newRecurrentConcepts : Option[RDD[(Long, (Long, Int))]] = None,
            newAndConcepts       : Option[RDD[(Long, Long)]] = None,
            newOrConcepts        : Option[RDD[(Long, Long)]] = None,
            newPath              : Option[String] = None) : ConceptData = {
    val lpath = if (newPath.isEmpty) path.get else ConceptData.cleanPath(newPath.get)
    val retConcepts = if (newConcepts.isEmpty) concepts else {
      Some(ConceptData.writeDataFile(lpath + ConceptData.conceptsFileName, newConcepts.get,
                                     (v:(Long,String)) => v._1 + " " + v._2))
    }
    val retContexts = if (newContexts.isEmpty) contexts else {
      Some(ConceptData.writeDataFile(lpath + ConceptData.contextsFileName, newContexts.get,
                                     (v:(Long,Long)) => v._1 + " " + v._2))
    }
    val retCorr = if (newCorrelations.isEmpty) correlations else {
      Some(ConceptData.writeDataFile(lpath + ConceptData.correlationsFileName, newCorrelations.get,
                                     (v:(Long,Long,Double)) => v._1 + " " + v._2 + " " + v._3))
    }
    val retSim = if (newSimilarities.isEmpty) similarities else {
      Some(ConceptData.writeDataFile(lpath + ConceptData.similaritiesFileName, newSimilarities.get,
                                     (v:(Long,Long,Double)) => v._1 + " " + v._2 + " " + v._3))
    }
    val retRecurrent = if (newRecurrentConcepts.isEmpty) recurrentConcepts else {
      Some(ConceptData.writeDataFile(lpath + ConceptData.recurrentFileName, newRecurrentConcepts.get,
                                     (v:(Long,(Long,Int))) => v._1 + " " + v._2._1 + " " + v._2._2))
    }
    val retAnd = if (newAndConcepts.isEmpty) andConcepts else {
      Some(ConceptData.writeDataFile(lpath + ConceptData.andFileName, newAndConcepts.get,
                                     (v:(Long,Long)) => v._1 + " " + v._2))
    }
    val retOr = if (newOrConcepts.isEmpty) orConcepts else {
      Some(ConceptData.writeDataFile(lpath + ConceptData.orFileName, newOrConcepts.get,
                                     (v:(Long,Long)) => v._1 + " " + v._2))
    }
    val retCount = if (newCount.isEmpty) count else {
      Some(ConceptData.writeValueFile(sc, lpath + ConceptData.countFileName, newCount.get))
    }

    ConceptData(retConcepts, retContexts, retCount, retCorr, retSim, retRecurrent, retAnd, retOr, Some(lpath))
  }

  /** Inspect available concept data.
    *
    * Prints information on the available concept data on an easy to read format.
    */
  def inspect() : Unit = {
    print("# Concept data information")
    if (path.isEmpty) println(" (no path avaliable)")
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
    if (correlations.isDefined) println(" - Correlations available")
    if (similarities.isDefined) println(" - Similarities available")
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
  /** File name for correlation data. */
  val correlationsFileName : String = "correlations"
  /** File name for similarity data. */
  val similaritiesFileName : String = "similarities"
  /** File name for recurrent concepts data. */
  val recurrentFileName    : String = "recurrent-concepts"
  /** File name for and concepts data. */
  val andFileName          : String = "and-concepts"
  /** File name for or concepts data. */
  val orFileName           : String = "or-concepts"

  /** Create a new ConceptData object. */
  def apply(concepts          : Option[RDD[(Long, String)]] = None,
            contexts          : Option[RDD[(Long, Long)]] = None,
            count             : Option[Long] = None,
            correlations      : Option[RDD[(Long, Long, Double)]] = None,
            similarities      : Option[RDD[(Long, Long, Double)]] = None,
            recurrentConcepts : Option[RDD[(Long, (Long, Int))]] = None,
            andConcepts       : Option[RDD[(Long, Long)]] = None,
            orConcepts        : Option[RDD[(Long, Long)]] = None,
            path              : Option[String] = None) : ConceptData = {
    new ConceptData(concepts, contexts, count, correlations, similarities,
                    recurrentConcepts, andConcepts, orConcepts, path)
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
    ConceptData(loadDataFile[(Long,String)](sc, lpath + conceptsFileName,
                                            s => {val f=s.split(" ");(f(0).toLong,f(1))}),
                loadDataFile[(Long,Long)](sc, lpath + contextsFileName,
                                          s => {val f=s.split(" ");(f(0).toLong,f(1).toLong)}),
                loadValueFile[Long](sc, lpath + countFileName, s=> s.toLong),
                loadDataFile[(Long,Long,Double)](sc, lpath + correlationsFileName,
                                          s => {val f=s.split(" ");(f(0).toLong,f(1).toLong,f(2).toDouble)}),
                loadDataFile[(Long,Long,Double)](sc, lpath + similaritiesFileName,
                                          s => {val f=s.split(" ");(f(0).toLong,f(1).toLong,f(2).toDouble)}),
                loadDataFile[(Long,(Long,Int))](sc, lpath + recurrentFileName,
                                                s => {val f=s.split(" ");(f(0).toLong,(f(1).toLong,f(2).toInt))}),
                loadDataFile[(Long,Long)](sc, lpath + andFileName,
                                          s => {val f=s.split(" ");(f(0).toLong,f(1).toLong)}),
                loadDataFile[(Long,Long)](sc, lpath + orFileName,
                                          s => {val f=s.split(" ");(f(0).toLong,f(1).toLong)}),
                Some(lpath))
  }

  /** Write all specified data sets.
    *
    * Returns a new [[ConceptData]] object containing the specified data sets.
    * These data sets are be written to their standard locations. If the files
    * already exist, they are overwritten.
    */
  def write(sc: SparkContext,
            concepts          : Option[RDD[(Long, String)]] = None,
            contexts          : Option[RDD[(Long, Long)]] = None,
            count             : Option[Long] = None,
            correlations      : Option[RDD[(Long, Long, Double)]] = None,
            similarities      : Option[RDD[(Long, Long, Double)]] = None,
            recurrentConcepts : Option[RDD[(Long, (Long, Int))]] = None,
            andConcepts       : Option[RDD[(Long, Long)]] = None,
            orConcepts        : Option[RDD[(Long, Long)]] = None,
            path              : Option[String] = None) : ConceptData = {
    ConceptData().write(sc, concepts, contexts, count, correlations, similarities,
                        recurrentConcepts, andConcepts, orConcepts, path)
  }

  /** Load a data file.
    *
    * Load a data file to an RDD using the specifed conversion function from a string to
    * the RDD data type.
    */
  def loadDataFile[T : ClassTag](sc: SparkContext, file : String,
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
  def writeDataFile[T : ClassTag](file: String, data: RDD[T],
                                  toStrFunc : (T) => String) : RDD[T] = {
    if (fileExists(data.context, file)) deleteFile(data.context, file)
    writeToCompressedFile(file, data.map(toStrFunc))
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
     import sqlContext.implicits._
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
