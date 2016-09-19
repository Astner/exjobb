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

package se.sics.concepts.main

import org.apache.spark.SparkContext
import se.sics.concepts.clustering.ClusteringAlgorithm
import se.sics.concepts.clustering.ClusteringParameters.{Iterations, UseWeights}
import se.sics.concepts.util.ParameterMap
import se.sics.concepts.io.ConceptData
import se.sics.concepts.main.Actions._
import se.sics.concepts.util.ConceptsUtil._


//noinspection ScalaUnnecessaryParentheses
object HigherOrderMain {
  case class HigherOrderConfig(
    local:                  Boolean = true,
    inputpath:              String = "",
    corrfunc:               String = "pwmi",
    clusteringAlgo:         String = "slpa",
    minCount:               Long = 0,
    minContextCount:        Long = 0,
    maxInDegree:            Long = 100,
    maxClusterIterations:   Int  = 10,
    maxLevel:               Long = 2,
    minCorrelation:         Double = 0.0,
    minSimilarity:          Double = 0.0,
    conceptThreshold:       Double = 0.0,
    bayesAlpha:             Double = 0.0,
    confidence:             Double = -1.0,
    maxIterations:          Int = 10,
    useWeights:             Boolean = false)

  /** Main test/evaluation function for library. */
  def main(args: Array[String]) {
    // Define application name and version
    val scoptAppName = "concepts"
    val appVersion = "0.4"
    // Setup Spark environment
    val conf = createSparkContext()

    // Setup parser
    val parser = new scopt.OptionParser[HigherOrderConfig](scoptAppName) {
      head(scoptAppName, appVersion)
      help("help") text ("prints this usage text")
      version("version") text ("prints version")

      opt[Unit]("local") abbr ("lcl") action { (_, c) =>
        c.copy(local = true)
      } text ("indicate that we are running locally, default false")

      opt[String]("path") abbr ("p") required() action { (x, c) =>
        c.copy(inputpath = x)
      } text ("path to concept data files")

      opt[String]("corrfunc") abbr ("cf") action { (x, c) =>
        c.copy(corrfunc = x)
      } text ("correlation function")

      opt[Long]("mincount") abbr ("mc") action { (x, c) =>
        c.copy(minCount = x)
      } text ("minimum concept count, default 2") // Reuses variable here

      opt[Long]("mincontextcount") abbr ("mcc") action { (x, c) =>
        c.copy(minContextCount = x)
      } text ("minimum context count, default 0")

      opt[String]("clusteringalgo") abbr ("cl") action { (x, c) =>
        c.copy(clusteringAlgo = x)
      } text ("Select clustering algorithm (slpa (default), maxcliques, edges or mutualmax)")

      opt[Int]("maxclustiter") abbr ("mci") action { (x, c) =>
        c.copy(maxClusterIterations = x)
      } text ("maximum clustering iterations, default 10")

      opt[Long]("maxlevel") abbr ("ml") action { (x, c) =>
        c.copy(maxLevel = x)
      } text ("maximum depth of higher order concepts, default 2")

      opt[Double]("bayesalpha") abbr ("ba") action { (x, c) =>
        c.copy(bayesAlpha = x)
      } text ("Bayesian alpha parameter (used in some correlation measures)")

      opt[Double]("confidence") abbr ("co") action { (x, c) =>
        c.copy(confidence = x)
      } text ("Desired confidence level (used in some correlation measures)")

      opt[Long]("maxdegree") abbr ("md") action { (x, c) =>
        c.copy(maxInDegree = x)
      } text ("indegree threshold")

      opt[Double]("mincorr") abbr ("mcr") action { (x, c) =>
        c.copy(minCorrelation = x)
      } text ("minimum correlation")

      opt[Double]("minsim") abbr ("msm") action { (x, c) =>
        c.copy(minSimilarity = x)
      } text ("minimum similarity")

      opt[Double]("conthreshold") abbr ("cth") action { (x, c) =>
        c.copy(conceptThreshold = x)
      } text ("concept threshold")

      opt[Boolean]("useweights") abbr ("uw") action { (x, c) =>
        c.copy(useWeights = x)
      } text ("use edge weights when clustering")
    }


    // Give relevant app name
    val appName = "concepts-higherOrder"
    val experimentPath = parser.parse(args, HigherOrderConfig()) match {
      case Some(config) => {
        config.inputpath
      }
      case _ => throw new IllegalArgumentException("Invalid configuration")
    }

    // Find out if we want to run locally
    val runLocal: Boolean = parser.parse(args, HigherOrderConfig()) match {
      case Some(config) => config.local
      case _ => false
    }

    // Create SparkContext
    val sc = if (runLocal) new SparkContext(conf.setAppName(appName).setMaster("local[8]"))
    else new SparkContext(conf.setAppName(appName))

    // Parse arguments
    parser.parse(args, HigherOrderConfig()) match {
      case Some(config) => {
        val cd = ConceptData.load(sc, config.inputpath)

        val clusteringAlgo: ClusteringAlgorithm = selectClusteringAlgo(config.clusteringAlgo)

        val clusteringParameters: ParameterMap = new ParameterMap()
          .add(Iterations, config.maxClusterIterations)
          .add(UseWeights, config.useWeights)

        contextsToHigherOrderConcepts(
          sc, cd, config.minCount, config.minContextCount,
          findCorrFunc(config.corrfunc, config.bayesAlpha, config.confidence),
          config.minCorrelation, config.minSimilarity, config.maxInDegree, config.conceptThreshold,
          clusteringAlgo, clusteringParameters, config.maxLevel)
      }
      case _ => throw new IllegalArgumentException("Invalid configuration")
    }

    // Write parameters for experiment to file
    parser.parse(args, HigherOrderConfig()) match {
      case Some(config) => {
        import se.sics.concepts.util.Implicits._
        writeParameterFile(config.toStringWithFields, experimentPath, sc, useHadoop = !runLocal)
      }
      case _ => throw new IllegalArgumentException("Invalid configuration")
    }

    sc.stop()
  }
}
