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

package se.sics.concepts.util

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import se.sics.concepts.clustering.{ClusteringAlgorithm, EdgesToClusters, MaxCliques, SLPA, MutualMax}
import se.sics.concepts.graphs.ConceptIDs._
import se.sics.concepts.measures.CorrelationMeasures._

import scala.reflect._

/** Commonly used utility functions for transforming data etc. */
object ConceptsUtil {

  /**
    * Creates the Spark configuration used by the applications
    *
    * @return A SparkCong object with preset arguments
    */
  def createSparkContext(): SparkConf = {
    new SparkConf()
      //      .setMaster("local[8]") // Set this either through --master option when launching or at spark-defaults.conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "1")
      .set("spark.kryoserializer.buffer.max.mb", "1024")
      // Turn registration requirement on sometimes to check what we are missing, of by default
      //      .set("spark.kryo.registrationRequired","true")
      //.set("spark.kryo.referenceTracking", "false") // Must be set to true if we have circular references
      .set("spark.shuffle.consolidateFiles", "true")
      //      .set("spark.local.dir", "/lhome/tvas/tmp")
      //      .set("spark.eventLog.enabled", "true")
      //      .set("spark.eventLog.compress", "true")
      //      .set("spark.default.parallelism", (8).toString)
      //.set("spark.storage.memoryFraction", "0.5")
      //      .set("spark.io.compression.codec", "snappy")
      .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig
  }

  /** Writes a file with experiment parameter settings to disk.
    * TODO: Move to ConceptsIO?
    *
    * @param configString Experiment configuration in a multiline String
    * @param experimentPath Path to experiment directory
    * @param useHadoop When set to true will use Hadoop to write the file to HDFS
    */
  def writeParameterFile(configString: String, experimentPath: String, sc: SparkContext, useHadoop: Boolean = false): Unit = {
    if(experimentPath != "") {

      val date = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
        .format(new java.util.Date(System.currentTimeMillis))
      val pathString = new File(experimentPath, "parameters-" + date).toString

      val settingsFile = if (useHadoop) {
        val hadoopConf = sc.hadoopConfiguration
        val fsPath = hadoopConf.get("fs.default.name")
        // Append the hdfsURL to the path
        val hdfsPath = new org.apache.hadoop.fs.Path(fsPath + pathString)
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new Configuration())
        new BufferedWriter(new OutputStreamWriter(hdfs.create(hdfsPath)))
      }
      else {
        new FileWriter(new File(pathString))
      }
      try {
        settingsFile.write(configString)
      } catch {
        case ioe: IOException => println("WARNING: Could not write parameters file")
        // case e: Exception => ...
      }
      finally {
        settingsFile.close()
      }
    }
  }

  /** Make sure an RDD is in one single partition
   *
   * Useful e.g. for writing an RDD to disk when a single partition is desired.
   */
  def toOnePartition[T](data : RDD[T]) : RDD[T] = data.coalesce(1, shuffle = true)

  /** Enumerate all unique strings in data set.
  *
  * Return an RDD of unique (String, ID) pairs, where each string occurs at least minCount times
  * in the input data set.
  */
  def enumerateUniqueStrings(allStrings : RDD[String],
                             minCount : Long = 1) : RDD[(String, Long)] =
    allStrings.map(x=>(x,1L)).reduceByKey(_+_).filter(x=>x._2>=minCount).keys.zipWithIndex()

  /** Expand data set of collections to key-value pairs of unique instance id and values.
  *
  * Expands a data set consisting of collections to pairs of example index and values.
  */
  def collectedToIndexedData[U, T <% TraversableOnce[U] : ClassTag](conceptData : RDD[T]) : RDD[(Long, U)] =
    conceptData.zipWithIndex().map(x=>(x._2,x._1)).flatMapValues(x=>x)

  /** Expand data set of collections to key-value pairs of unique instance id offset by constant and values.
  *
  * Expands a data set consisting of collections to pairs of example index and values.
  */
  def collectedToIndexedData[U, T <% TraversableOnce[U] : ClassTag](conceptData : RDD[T],
                                                                    offset : Long) : RDD[(Long, U)] =
    conceptData.zipWithIndex().map(x=>(x._2,x._1)).flatMapValues(x=>x).map(x=>(x._1 + offset,x._2))

  /** Convert an indexed data set to an RDD of collections in order. */
  def indexedToCollectedData[U : ClassTag](indexedData : RDD[(Long, U)]) : RDD[Iterable[U]] =
    indexedData.groupByKey().sortBy(x=>x._1).values

  /** Convert a sequence of values to an indexed sequence while applying a moving window.
  *
  * Return indexed data set where each observation is repeated n times, i.e. each (index,id) pair is expanded to
  * n instances (index,id), (index+1,id), ..., (index+n-1,id), where n is the specified window size.
  */
  def sequenceToIndexDataWithMovingWindow[U : ClassTag](seq : RDD[U], winSize : Long) : RDD[(Long, U)] =
    seq.zipWithIndex().mapValues(x=> x until (x + winSize)).flatMapValues(x=>x).map(x=>(x._2,x._1))

  /** Apply a moving window to an indexed data set.
  *
  * Return data set where each observation is repeated n times, i.e. each (index,id) pair is expanded to
  * n instances (index,id), (index+1,id), ..., (index+n-1,id), where n is the specified window size.
  */
  def indexedDataMovingWindow[U : ClassTag](seq : RDD[(Long, U)], winSize : Long) : RDD[(Long, U)] = {
    //    seq.flatMap{case (id, item) => {
    //      val idSeq = id until (id + winSize)
    //      val itemSeq = List.fill(idSeq.size)(item)
    //      idSeq.zip(itemSeq)
    //      }
    //    }
    seq.map(x => (x._2, (x._1 until (x._1 + winSize)))).flatMapValues(x => x).map(x => (x._2, x._1))
  }

  /** Return n edges with largest weights.
  *
  * NOTE: Does zipWithIndex always maintain order?
  */
  def topNEdges(edges : RDD[(ConceptID, ConceptID, Double)], n : Long) : RDD[(ConceptID, ConceptID, Double)] =
    edges.sortBy(_._3, ascending=false).zipWithIndex().filter{case ((i,j,w),k) => k < n}.keys

  /** Return true iff two doubles are equal to a certain precision. */
  def approximatelyEqualFP(x : Double, y : Double, precision : Double = 0.001) : Boolean =
    Math.abs(x - y) <= precision * Math.min(x, y)

  /** Return true iff values are approximately equal.
  *
  * Compares floating point values within certain precision, all othe
  * values use '=='.
  */
  def ~=(x : Any, y : Any) : Boolean =
    approximatelyEqual(x, y, 0.001)

  /** Return true iff values are approximately equal.
  *
  * Compares floating point values within certain precision, all othe
  * values use '=='.
  */
  def approximatelyEqual(x : Any, y : Any, precision : Double = 0.001) : Boolean = {
    x match {
      case dx : Double => {
        y match {
          case dy : Double => approximatelyEqualFP(dx, dy, precision)
          case fy : Float => approximatelyEqualFP(dx, fy.toDouble, precision)
          case _ => x == y
        }
      }
      case fx : Float => {
        y match {
          case dy : Double => approximatelyEqualFP(fx.toDouble, dy, precision)
          case fy : Float => approximatelyEqualFP(fx.toDouble, fy.toDouble, precision)
          case _ => x == y
        }
      }
      case _ => x == y
    }
  }

  /** Map strings in commands to correlation functions */
  def findCorrFunc(cfName : String, bayesAlpha : Double = 5.0, confidence : Double = -1.0) :
  (Long, Long, Long, Long) => (Double, Double) = {
    val optConfidence : Option[Double] = if (confidence < 0.0) None else Some(confidence)


    val bmi = bayesianMI(_ : Long, _ : Long, _ : Long, _ : Long, alpha = bayesAlpha,
      confidence = optConfidence)
    val bpwmi = bayesianPWMI(_ : Long, _ : Long, _ : Long, _ : Long, alpha = bayesAlpha,
      confidence = optConfidence)
    val nbpwmi = normalizedBayesianPWMI(_ : Long, _ : Long, _ : Long, _ : Long,
      alpha = bayesAlpha, confidence = optConfidence)

    val cfmap : Map[String, (Long, Long, Long, Long) => (Double, Double)] =
      Map("existence" -> existence, "pairwiseCount" -> pairwiseCount, "weightedCount" -> weightedCount,
        "overlap" -> overlap, "sorensenDice" -> sorensenDice, "jaccard" -> jaccard,
        "condprob" -> conditionalProbability, "pwmi" -> pointwiseMutualInformation,
        "bpwmi" -> bpwmi, "normpwmi" -> normalizedPWMI, "normbpwmi" -> nbpwmi,
        "mutualinfo" -> mutualInformation, "bmi" -> bmi)
    cfmap(cfName)
  }

  def selectClusteringAlgo(algorithmName: String): ClusteringAlgorithm = {
    algorithmName match {
      case "slpa" => new SLPA()
      case "maxcliques" => new MaxCliques()
      case "edges" => new EdgesToClusters()
      case "mutualmax" => new MutualMax()
      case _ => new SLPA()
    }
  }

}
