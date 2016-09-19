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

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import play.api.libs.json._
import se.sics.concepts.util.ConceptsUtil
import se.sics.concepts.graphs.ConceptIDs
import ConceptIDs._
import ConceptsUtil._
import scala.collection.JavaConverters._

import scala.collection.mutable

// TODO: Fix the code, where does the HDFS assumption come from?
/** IO and related utility functions for the concepts library. */
object ConceptsIO {
  /** Write string to the specified file. */
  def writeToFile(path: String, txt: String): Unit = {
    Files.write(Paths.get(path), txt.getBytes(StandardCharsets.UTF_8))
  }

  /** Write strings as lines to the specified file. */
  def writeToFile(path: String, txt: Iterable[String]): Unit = {
    Files.write(Paths.get(path), txt.asJavaCollection, StandardCharsets.UTF_8)
  }
  /** Write string RDD to file. */
  def writeToFile(path: String, strings: RDD[String]): Unit ={
    strings.saveAsTextFile(path)
  }

  /** Write string RDD to compressed file. */
  def writeToCompressedFile(path: String, strings: RDD[String]): Unit =
    strings.saveAsTextFile(path,classOf[org.apache.hadoop.io.compress.GzipCodec])

  /** Read file as one single string. */
  def readFromFile(path: String): String = {
    scala.io.Source.fromFile(path).getLines().mkString
  }

  /** Check if (hdfs) file exists. */
  def fileExists(sc: SparkContext, file: String) : Boolean = {
    val hconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hconf)
    fs.exists(new org.apache.hadoop.fs.Path(file))
  }

  /** Delete (hdfs) file (all partitions included). */
  def deleteFile(sc: SparkContext, file: String) : Boolean = {
    val hconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hconf)
    try { fs.delete(new org.apache.hadoop.fs.Path(file), true); true }
    catch { case _ : Throwable => { false } }
  }

  /** Merge partioned file to one single file somewhat efficiently.
    *
    * Based on a comment at http://www.markhneedham.com/blog/2014/11/30/spark-write-to-csv-file/
    */
  def mergeFile(sc: SparkContext, srcPath: String, destPath: String, deleteOriginal: Boolean = false) :
  Unit = {
    val hadoopConfig = sc.hadoopConfiguration
    val srcFs  = FileSystem.get(URI.create(srcPath), hadoopConfig)
    val destFs = FileSystem.get(URI.create(destPath), hadoopConfig)
    FileUtil.copyMerge(srcFs, new Path(srcPath),
                       destFs, new Path(destPath),
                       deleteOriginal, hadoopConfig, null)
  }

  /**
   * Exports graph to JSON for D3 visualization.
   *
   * Vertices without edges are discarded.
    *
    * @param edges (concept index, concept index, weight) tuples
   * @param clusters (i,c) tuples, where concepts i belongs to clusters c
   * @param concepts (concept index, concept label) tuples
   * @param weightThreshold Min allowed edge weight
   * @param numEdges Number of edges, with largest weights
   * @param fileName JSON file name of output
   */
  def exportGraphToJSON(edges : RDD[(ConceptID, ConceptID, Double)],
                        clusters : Option[RDD[(ConceptID, ConceptID)]] = None,
                        concepts : RDD[(ConceptID, String)],
                        weightThreshold : Double,
                        numEdges : Long,
                        fileName : String): Unit = {
    val prunedEdges = topNEdges(edges.filter(_._3 >= weightThreshold),numEdges)
    val withLabels = prunedEdges.flatMap { case (i, j, w) => Iterable(i, j) }.distinct().
                     // Merges with none to label intersection (intersect + join possibly faster)
                     map((_,None)).join(concepts).mapValues{case (n,s) => s}
    val withClusters = if(clusters.isEmpty) withLabels.mapValues{s => (s,0L)}
                       else withLabels.join(clusters.get)
    val prunedVertices = withClusters.zipWithIndex().map{case ((io,(s,c)),in)=>(in,(io,s,c))}.sortBy(_._1). // Re-index and sort
                         persist(MEMORY_ONLY)
    val newIndices = prunedVertices.map{case (in,(io,s,c))=>(io,in)}.persist(MEMORY_ONLY)
    val indexEdges = prunedEdges.map{case (i,j,w)=>(i,(j,w))}.join(newIndices). // Re-map edge indices
                     map{case (io,((jo,w),in))=>(jo, (in,w))}.join(newIndices).
                     map{case (jo,((in,w),jn))=>(in,jn,w)}
    val jsonVertices = prunedVertices.map{case (in,(io,s,c))=>
        JsObject(Seq("name"->JsString(s),"group"->JsNumber(c.toDouble)))}
    val jsonEdges = indexEdges.map{case (i,j,w)=>
        JsObject(Seq("source"->JsNumber(i),"target"->JsNumber(j),"value"->JsNumber(w)))}
    val jsonGraph = JsObject(Seq("nodes"->JsArray(jsonVertices.collect()),"links"->JsArray(jsonEdges.collect())))
    writeToFile(fileName,Json.prettyPrint(jsonGraph))
    newIndices.unpersist()
    prunedVertices.unpersist()
  }

  /** Writes labeled edges with strongest weights to file.
   *
   * @param edges Index edges
   * @param labels Index-label pairs
   * @param num Number of edges to return
   * @param fileName Path and name of output file
   */
  def writeTopLabeledEdges(edges : RDD[(Long, Long, Double)],
                           labels : RDD[(Long, String)],
                           num : Int,
                           fileName : String) : Unit = {
    // Map labels to edges and find edges with strongest weights
    val topL = edges.map{case (i,j,w)=>(i,(j,w))}.join(labels).
               map{case (i,((j,w),si))=>(j,(si,w))}.join(labels).
               map{case (j,((si,w),sj))=>(si,sj,w)}.
               top(num)(Ordering.by[(String, String, Double),Double](_._3))
    val writeable = topL.view.map{case (si,sj,w)=>w.toString+" "+si+" "+sj}
    writeToFile(fileName, writeable) // Will throw NotImplemented
  }

  /** Writes concepts to readable text file.
   *
   * Concepts are written using their text representation if available, otherwise
   * the numerical concept ID is used. Data is forced to one partition before writing.
    *
    * @param concepts (i,c) tuples, where concept i is a constituent of concept c
   * @param conceptNames Index-label pairs
   * @param fileName Output file name
   */
  def writeConceptsAsText(concepts : RDD[(ConceptID, ConceptID)],
                          conceptNames : RDD[(ConceptID, String)],
                          fileName : String) : Unit = {
    val collected = concepts.leftOuterJoin(conceptNames).
      map { case (i, (c, s)) => (c, s.getOrElse(i.toString)) }.groupByKey()
    val writable = collected.map{case (c,s)=>c.toString+": "+s.mkString(" ")}
    toOnePartition(writable).saveAsTextFile(fileName)
  }

  /** Writes concept counts to readable text file.
   *
   * Recurrent concepts are written using their text representation if available, otherwise
   * the numerical concept ID is used. Data is forced to one partition before writing.
    *
    * @param counts (c,n) tuples, representing concept id c occuring n times
   * @param conceptNames Index-label pairs
   * @param fileName Output file name
   */
  def writeConceptCountsAsText(counts : RDD[(ConceptID, ConceptCount)],
                               conceptNames : RDD[(ConceptID, String)],
                               fileName : String) : Unit = {
    val withstrings = counts.leftOuterJoin(conceptNames).
                      map{case (c,(n,s))=>(c,n,s.getOrElse(c.toString))}
    val writable = withstrings.map{case (c,n,s)=>s+" "+n.toString}
    toOnePartition(writable).saveAsTextFile(fileName)
  }

  /** Writes recurrent concepts to readable text file.
   *
   * Recurrent concepts are written using their text representation if available, otherwise
   * the numerical concept ID is used. Data is forced to one partition before writing.
    *
    * @param concepts (c,n) tuples, representing concept id co-occurring n times
   * @param conceptNames Index-label pairs
   * @param fileName Output file name
   */
  def writeRecurrentConceptsAsText(concepts : RDD[(ConceptID, Int)],
                                   conceptNames : RDD[(ConceptID, String)],
                                   fileName : String) : Unit = {
    val withstrings = concepts.leftOuterJoin(conceptNames).
                      map{case (c,(n,s))=>(c,n,s.getOrElse(c.toString))}
    val writable = withstrings.map{case (c,n,s)=>s+"^"+n.toString}
    toOnePartition(writable).saveAsTextFile(fileName)
  }

  /** Writes higher-order concepts to readable text file.
   *
   * Higher-order concepts are written as nested lists, where [] encloses AND concepts
   * and () encloses OR concepts. AND and OR concepts are assumed to not share any ids.
   * Note that RDDs are collected and used as Maps.
    *
    * @param andConcepts (i,c) tuples, where concept i is a constituent of AND concept c
   * @param orConcepts (i,c) tuples, where concept i is a constituent of OR concept c
   * @param conceptNames (i,s) tuples, where s is the label of concept i
   * @param fileName Output file name
   */
  def writeHigherOrderConceptsAsText(recurrentConcepts : RDD[(ConceptID, (ConceptID, ConceptCount))],
                                     andConcepts : RDD[(ConceptID, ConceptID)],
                                     orConcepts : RDD[(ConceptID, ConceptID)],
                                     conceptNames : RDD[(ConceptID, String)],
                                     fileName : String) : Unit = {
    val andMap = andConcepts.map { case (c, cc) => (cc, c) }.groupByKey().collectAsMap()
    val orMap = orConcepts.map { case (c, cc) => (cc, c) }.groupByKey().collectAsMap()
    val labelMap = conceptNames.collectAsMap()

    val recLabels = recurrentConcepts.collectAsMap().
                    mapValues{case (c, n) => labelMap.getOrElse(c, c.toString)+"^"+n.toString}
    val fullLabels = labelMap ++ recLabels

    def conceptToString(concept : ConceptID) : String =
      if(fullLabels.contains(concept)) fullLabels(concept)
      else if(andMap.contains(concept)) "["+andMap(concept).map(conceptToString).mkString(",")+"]"
      else if(orMap.contains(concept)) "("+orMap(concept).map(conceptToString).mkString(",")+")"
      else "<"+concept.toString+">" // Concept missing

    val allConcepts = andConcepts.map { case (c, cc) => cc }.union(orConcepts.map { case (c, cc) => cc }).distinct()
    val parsedConcepts = allConcepts.map(cc => cc.toString + ": " + conceptToString(cc)).collect().mkString("\n")

    writeToFile(fileName,parsedConcepts)
  }

  // TODO: Test execution with both and and or concepts, target for HDFS?
  def writeHigherOrderConceptsAsTextScalable(
      recurrentConcepts : RDD[(ConceptID, (ConceptID, ConceptCount))],
      andConcepts : RDD[(ConceptID, ConceptID)],
      orConcepts : RDD[(ConceptID, ConceptID)],
      conceptNames : RDD[(ConceptID, String)],
      fileName : String)
    : Unit = {

    val stringConcepts = andConcepts
      .union(orConcepts)
      .map{case (conceptID, itemID) => (itemID, conceptID)}
      .join(conceptNames)
      .map{case (itemID, (conceptID, string)) => (conceptID, string)}

    // The following could also be done without the filter, directly on stringConcepts, but having a conditional
    // in the map might degrade performance. Would need to test both versions to see which is faster
    val aggregatedAnd = stringConcepts.filter{case (conceptID, _) => isANDConcept(conceptID)}
      .aggregateByKey(mutable.MutableList[String]())((list, element) => {list += element}, _ ++ _)
      .map{case (conceptID, itemList) => "["+itemList.mkString(",")+"]"}

    val aggregatedOr = stringConcepts.filter{case (conceptID, _) => isORConcept(conceptID)}
      .aggregateByKey(mutable.MutableList[String]())((list, element) => {list += element}, _ ++ _)
      .map{case (conceptID, itemList) => "("+itemList.mkString(",")+")"}

//    aggregatedAnd.saveAsTextFile(fileName + "and-text")
    aggregatedOr.saveAsTextFile(fileName + "or-text")

  }

  /** Writes correlations between labeled higher-order concepts to file.
   *
   * NOTE: Concepts are collected as maps.
   *
   * @param edges Index edges
   * @param labels (i, s) tuples, where s is the label of concept i
   * @param andConcepts (i, c) tuples, where concept i is a constituent of AND concept c
   * @param orConcepts (i, c) tuples, where concept i is a constituent of OR concept c
   * @param numberOfEdges number of strongest correlations to write to file
   * @param fileName Path and name of output file
   */
  def writeAndOrConceptEdgesAsText(edges : RDD[(ConceptID, ConceptID, Weight)],
                                   labels : RDD[(ConceptID, String)],
                                   andConcepts : RDD[(ConceptID, ConceptID)],
                                   orConcepts : RDD[(ConceptID, ConceptID)],                                   
                                   numberOfEdges : Int,
                                   fileName : String) : Unit = {

    val labelMap = labels.collectAsMap()
    val andMap = andConcepts.map(_.swap).groupByKey().collectAsMap()
    val orMap = orConcepts.map(_.swap).groupByKey().collectAsMap()

    def conceptToString(concept : ConceptID) : String =
      if(labelMap.contains(concept)) labelMap(concept)
      else if(andMap.contains(concept)) "["+andMap(concept).map(conceptToString).mkString(",")+"]"
      else if(orMap.contains(concept)) "("+orMap(concept).map(conceptToString).mkString(",")+")"
      else "<"+concept.toString+">" // Concept missing

    val topEdges = edges.top(numberOfEdges)(Ordering.by[(ConceptID, ConceptID, Weight), Weight](_._3))
    val concepts = topEdges.flatMap{case (i, j, w) => Iterable(i, j)}.distinct
    val conceptStringMap = concepts.map(i => (i, conceptToString(i))).toMap
    val toWrite = topEdges.map{case (i, j, w) => w.toString + "\t" + conceptStringMap(i) + "\t" + conceptStringMap(j)}.mkString("\n")

    if(fileName == "") println(toWrite) else writeToFile(fileName, toWrite)
  }

  /** Exports OR concept hierarchy to file in Newik tree format 
    * 
    * Exports binary OR hierarchy to Newik tree (https://en.wikipedia.org/wiki/Newick_format). 
    * Branch lengths are given by 1 - s, for similarity s.
    * Note that RDDs are collected as maps.
    */
  def exportORConceptHierarchyToNewick(similarities : RDD[(ConceptID, ConceptID, Weight)],
                                       labels : RDD[(ConceptID, String)],
                                       orConcepts : RDD[(ConceptID, ConceptID)],
                                       fileName : String) : Unit = {
    // Map from leaf concepts to labels
    val labelMap = labels.collectAsMap()
    // Map from concept index to (child 1, child 2, similarity) tuple
    val orMap = orConcepts.map{case (c, cc) => (cc, List(c))}.
                           reduceByKey((a, b) => if(a.size == 1 && b.size == 1) a ++ b else a).mapValues(v => (v(0), v(1))).
                           map{case (cc, (i, j)) => if(i < j) ((i, j), cc) else ((j, i), cc)}.
                           join(similarities.map{case (i, j, s) => if(i < j) ((i, j), s) else ((j, i), s)}).
                           map{case ((i, j), (cc, sij)) => (cc, (i, j, sij))}.collectAsMap()
  
    def conceptToString(concept : ConceptID) : String =
      if(labelMap.contains(concept)) labelMap(concept)  // Leaf
      else if(orMap.contains(concept)) {  // Parent
        val orTuple = orMap(concept) // (child 1, child 2, similarity)
        val d = {1.0 - orTuple._3}.toString
        "(" + conceptToString(orTuple._1) + ":" + d + "," + conceptToString(orTuple._2) + ":" + d + ")"
      }
      else "<"+concept.toString+">" // Concept missing

    val newik = orConcepts.values.distinct.map(cc => conceptToString(cc) + ";").collect().mkString("\n")
    writeToFile(fileName, newik)
  }
}

