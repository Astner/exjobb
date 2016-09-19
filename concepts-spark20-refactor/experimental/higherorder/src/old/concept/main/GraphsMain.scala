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
import se.sics.concepts.io.ConceptData
import se.sics.concepts.main.Actions._
import se.sics.concepts.util.ConceptsUtil._

//noinspection ScalaUnnecessaryParentheses
object GraphsMain {
  /** Storage class for configuration options. */
  case class Config(local:                  Boolean = false,
                    command:                String = "",
                    inputpath:              String = "",
                    corrfunc:               String = "pwmi",
                    calculate:              String = "all",
                    minCount:               Long = 0,
                    minContextCount:        Long = 0,
                    maxInDegree:            Long = 100,
                    minCorrelation:         Double = 0.0,
                    minSimilarity:          Double = 0.0,
                    conceptThreshold:       Double = 0.0,
                    bayesAlpha:             Double = 0.0,
                    confidence:             Double = -1.0,
                    numBins:                Int = 1000,
                    maxIterations:          Int = 10)

  /** Main test/evaluation function for library. */
  def main(args: Array[String]) {
    // Define application name and version
    val scoptAppName = "concepts"
    val appVersion = "0.4"
    // Setup Spark environment
    val conf = createSparkContext()

    // Setup parser
    val parser = new scopt.OptionParser[Config](scoptAppName) {
      head(scoptAppName, appVersion)
      help("help") text ("prints this usage text")
      version("version") text ("prints version")

      opt[Unit]("local") abbr ("lcl") action { (_, c) =>
        c.copy(local = false)
      } text ("indicate that we are running locally, default false")


      cmd("graphs") action { (_, c) =>
        c.copy(command = "graphs")
      } text ("Calculate correlation and similarity graphs from context data.") children(
        opt[String]("path") abbr ("p") required() action { (x, c) =>
          c.copy(inputpath = x)
        } text ("path to concept data files"),
        opt[String]("corrfunc") abbr ("cf") action { (x, c) =>
          c.copy(corrfunc = x)
        } text ("correlation function"),
        opt[String]("calculate") abbr ("ca") action { (x, c) =>
          c.copy(calculate = x)
        } text ("select calculation (correlations, similarities, all (default))"),
        opt[Long]("mincount") abbr ("mc") action { (x, c) =>
          c.copy(minCount = x)
        } text ("minimum example count, default 0"),
        opt[Long]("mincontextcount") abbr ("mcc") action { (x, c) =>
          c.copy(minContextCount = x)
        } text ("minimum context count, default 0"),
        opt[Double]("bayesalpha") abbr ("ba") action { (x, c) =>
          c.copy(bayesAlpha = x)
        } text ("Bayesian alpha parameter (used in some correlation measures)"),
        opt[Double]("confidence") abbr ("co") action { (x, c) =>
          c.copy(confidence = x)
        } text ("Desired confidence level (used in some correlation measures)"),
        opt[Long]("maxdegree") abbr ("md") action { (x, c) =>
          c.copy(maxInDegree = x)
        } text ("indegree threshold"),
        opt[Double]("mincorr") abbr ("mcr") action { (x, c) =>
          c.copy(minCorrelation = x)
        } text ("minimum correlation"),
        opt[Double]("minsim") abbr ("msm") action { (x, c) =>
          c.copy(minSimilarity = x)
        } text ("minimum similarity")
        )

    }

    // Give relevant app name
    val appName = "concepts" + parser.parse(args, Config()).get.command
    val experimentPath = parser.parse(args, Config()) match {
      case Some(config) => {
        config.command match {
          case _ => config.inputpath
        }
      }
      case None => ""
    }

    // Find out if we want to run locally
    val runLocal : Boolean = parser.parse(args, Config()) match {
      case Some(config) => config.local
      case _ => false
    }

    // Create SparkContext
    val sc = if(runLocal) new SparkContext(conf.setAppName(appName).setMaster("local[8]"))
    else new SparkContext(conf.setAppName(appName))

    // Parse arguments
    parser.parse(args, Config()) match {
      case Some(config) => {
            val cd = ConceptData.load(sc, config.inputpath)
            config.calculate match {
              case "correlations" => contextsToGraphs(sc, cd, config.minCount, config.minContextCount,
                findCorrFunc(config.corrfunc, config.bayesAlpha, config.confidence),
                config.minCorrelation, config.minSimilarity, config.maxInDegree,
                calculateSimilarities = false)
              case "similarities" => contextsToGraphs(sc, cd, config.minCount, config.minContextCount,
                findCorrFunc(config.corrfunc, config.bayesAlpha, config.confidence),
                config.minCorrelation, config.minSimilarity, config.maxInDegree,
                calculateCorrelations = false)
              case _ => contextsToGraphs(sc, cd, config.minCount, config.minContextCount,
                findCorrFunc(config.corrfunc, config.bayesAlpha, config.confidence),
                config.minCorrelation, config.minSimilarity, config.maxInDegree)
            }
          }
      case None => throw new IllegalArgumentException("Invalid configuration")
    }

    // Write parameters for experiment to file
    parser.parse(args, Config()) match {
      case Some(config) => {
        import se.sics.concepts.util.Implicits._
        writeParameterFile(config.toStringWithFields, experimentPath, sc, useHadoop = !runLocal)
      }
      case None => {
        //If something went wrong with the config do nothing
      }
    }

    sc.stop()
  }


}
