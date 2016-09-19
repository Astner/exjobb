/*
 Copyright (C) 2016 Daniel Gillblad, Olof Görnerup, Theodoros Vasiloudis (dgi@sics.se,
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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import se.sics.concepts.graphs.ConceptIDs
import ConceptIDs._
import play.api.libs.json._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object ClassificationMLlib {

  /** Loads classification examples
   *
   * Loads and parses classification examples into (class label, tokens) pairs.
   * Assumes that each line starts with a class label, followed by a tab, followed by 
   * space-separated tokens. Note that duplicate tokens are discarded.
   */
  def loadExamples(sc : SparkContext, fileName: String): RDD[(String, Iterable[String])] = {
    sc.textFile(fileName).map{line =>
      val parts = line.split('\t')
      (parts(0), parts(1).split(' ').distinct)
    }
  }

  /** Translates activations and example-class pairs to MLlib LabelPoint vectors. */
  def activationsToExampleVectors(examplesToObjects: RDD[(DataIndex, ConceptID)], // Activations
                                  examplesToClasses: RDD[(DataIndex, Long)],
                                  cacheLevel : StorageLevel = MEMORY_AND_DISK): RDD[LabeledPoint] = {
    val newIndices = examplesToObjects.values.distinct.zipWithIndex.persist(cacheLevel) // Re-index objects
    val reIndexedActivations = examplesToObjects.map(_.swap).join(newIndices).values
    val vectorSize = newIndices.count.toInt

    reIndexedActivations.groupByKey.join(examplesToClasses).
      map{case (exampleIndex, (objectIndices, classIndex)) =>        
        val sparseVector = objectIndices.map(_.toInt).toArray.sorted.map((_, 1.0))  // Indices must be increasing
        LabeledPoint(classIndex.toDouble, Vectors.sparse(vectorSize, sparseVector))
      }
  }

  /** Performs naive Bayes classification of examples with activated concepts. */
  def naiveBayes(naiveBayesType: String, 
                 testRatio: Double, 
                 activations: RDD[(DataIndex, ConceptID)],
                 examplesToClasses: RDD[(DataIndex, Long)],
                 cacheLevel : StorageLevel = MEMORY_AND_DISK): JsObject = {
    val exampleVectors = activationsToExampleVectors(activations, examplesToClasses, cacheLevel)
    val splits = exampleVectors.randomSplit(Array(1.0 - testRatio, testRatio), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = naiveBayesType)
    val predictionsAndLabels = test.map(p => (model.predict(p.features), p.label))
    val metrics = new MulticlassMetrics(predictionsAndLabels)

    JsObject(Seq(
      "accuracy" -> JsNumber(predictionsAndLabels.filter(x => x._1 == x._2).count.toDouble / test.count.toDouble),
      "precision" -> JsNumber(metrics.precision),
      "recall" -> JsNumber(metrics.recall),
      "f1 score" -> JsNumber(metrics.fMeasure),
      "weighted precision" -> JsNumber(metrics.weightedPrecision),
      "weighted recall" -> JsNumber(metrics.weightedRecall),
      "weighted f1 score" -> JsNumber(metrics.weightedFMeasure),
      "weighted true positive rate" -> JsNumber(metrics.weightedTruePositiveRate),
      "weighted false positive rate" -> JsNumber(metrics.weightedFalsePositiveRate)))
  }
}
