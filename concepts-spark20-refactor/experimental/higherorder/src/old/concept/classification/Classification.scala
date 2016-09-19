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

package se.sics.concepts.classification

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import se.sics.concepts.graphs.ConceptIDs
import ConceptIDs._

/** Concept classification base trait. */
trait ConceptClassifier extends Serializable {
  /** Classify indexed data set. */
  def classify(conceptData : RDD[(DataIndex, ConceptID)]) : RDD[(DataIndex, ClassID)]
}

/** Naive bayes concept based classification.
*
* @constructor Create a new Naive Bayes classifier trained on specified concept data and class
*              label data sets.
*/
class NaiveBayes(conceptData : RDD[(DataIndex, ConceptID)], classData : RDD[(DataIndex, ClassID)],
                 nrExamples: Long)
extends ConceptClassifier {
  /** Concept occurrence probabilities conditional on class. */
  val classConceptProbabilities : RDD[(ConceptID, (ClassID, Double))] = {
    val conceptCount = conceptData.map(x => (x._2, 1L)).reduceByKey(_ + _)
    conceptData.join(classData).map(x => ((x._2._2, x._2._1), 1L)).reduceByKey(_ + _).
    map(x => (x._1._2, (x._1._1, x._2))).join(conceptCount).
    mapValues(x => (x._1._1, x._1._2.toDouble / x._2.toDouble)).
    persist(MEMORY_AND_DISK)
  }

  /** Prior probabilities per class. */
  val classPrior : RDD[(ClassID, Double)] =
    classData.map(x => (x._2, 1L)).reduceByKey(_ + _).mapValues(_.toDouble / nrExamples.toDouble)

  /** Log-likelihood of class given that no concepts are present.
  *
  * Primarily used for fast classification of sparse data, i.e. data sets where only a few
  * concepts are present per example.
  */
  val classBaseline : RDD[(ClassID, Double)] = {
    classConceptProbabilities.values.
    aggregateByKey(0.0)((a, v) => v match {
      case 1.0 => a - 100.0
      case _ => a + math.log(1.0 - v)
      },
      _ + _).join(classPrior).mapValues(x => x._1 + math.log(x._2)).
    persist(MEMORY_AND_DISK)
  }

  override def classify(conceptData : RDD[(DataIndex, ConceptID)]) : RDD[(DataIndex, ClassID)] = {
    conceptData.map(x => (x._2, x._1)).join(classConceptProbabilities).
    map(x => ((x._2._1, x._2._2._1), x._2._2._2)).
    aggregateByKey(0.0)((a, v) => v match {
      case 1.0 => a + 100.0
      case 0.0 => a - 100.0
      case _ => a + math.log(v) - math.log(1.0 - v)
      },
      _ + _).
    map(x => (x._1._2, (x._1._1, x._2))).join(classBaseline).
    map(x => (x._2._1._1, (x._1, x._2._1._2 + x._2._2))).
    reduceByKey((x, y) => if (x._2 < y._2) y else x).
    mapValues(x => x._1)
  }
}
