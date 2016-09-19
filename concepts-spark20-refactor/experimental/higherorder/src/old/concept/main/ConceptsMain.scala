/*
 Copyright (C) 2015 Daniel Gillblad, Olof Görnerup, Theodoros Vasiloudis (dgi@sics.se,
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

import org.apache.spark._
import se.sics.concepts.clustering.ClusteringAlgorithm
import se.sics.concepts.clustering.ClusteringParameters._
import se.sics.concepts.io.ConceptData
import se.sics.concepts.main.Actions._
import se.sics.concepts.util.ConceptsUtil._
import se.sics.concepts.util.ParameterMap
import se.sics.concepts.experiments.Experiments

/** Object containing main calculation/evaluation functions for library. */
//noinspection ScalaUnnecessaryParentheses
object ConceptsMain {
  /** Storage class for configuration options. */
  case class Config(local:                  Boolean = true,
                    command:                String = "",
                    inputpath:              String = "",
                    outputpath:             String = "",
                    inputFile:              String = "",
                    corrfunc:               String = "pwmi",
                    data:                   String = "",
                    calculate:              String = "all",
                    clusteringAlgo:         String = "slpa",
                    format:                 String = "text",
                    minCount:               Long = 0,
                    minConceptCount:        Long = 0,
                    minContextCount:        Long = 0,
                    maxDegree:              Long = 0,
                    maxDegreeMin:           Long = 0,
                    maxDegreeMax:           Long = 0,
                    maxDegreeDelta:         Long = 0,
                    numberOfExportedItems:  Int = 1000000,
                    minCorrelation:         Double = 0.0,
                    minSimilarity:          Double = 0.0,
                    minSimilarityMin:       Double = 0.0,
                    minSimilarityMax:       Double = 0.0,
                    minSimilarityDelta:     Double = 0.0,
                    maxNumEdges:            Long = 1000,
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
      help("help") text("prints this usage text")
      version("version") text("prints version")

      opt[Unit]("local") abbr("lcl") action { (_, c) =>
        c.copy(local = true) } text("indicate that we are running locally, default true")

      cmd("cluster") action { (_, c) =>
        c.copy(command = "cluster") } text("Cluster vertices in correlation and similarity graphs.") children(
          opt[String]("path") abbr("p") required() action { (x, c) =>
            c.copy(inputpath = x) } text("path to graph files"),
          opt[String]("calculate") abbr("ca") action { (x, c) =>
            c.copy(calculate = x) } text("select calculation (correlations, similarities, all (default))"),
          opt[String]("clusteringAlgo") abbr("cl") action { (x, c) =>
            c.copy(clusteringAlgo = x) } text("Select clustering algorithm (SLPA (default), MaxCliques or Edges)"),
          opt[Double]("mincorr") abbr("mc") action { (x, c) =>
            c.copy(minCorrelation = x) } text("minimum correlation"),
          opt[Double]("minsim") abbr("ms") action { (x, c) =>
            c.copy(minSimilarity = x) } text("minimum similarity"),
          opt[Int]("maxiterations") abbr("mi") action { (x, c) =>
            c.copy(maxIterations = x) } text("maximum number of iterations")
        )

      cmd("inspect") action { (_, c) =>
        c.copy(command = "inspect") } text("Inspect available concept data.") children(
          opt[String]("path") abbr("p") required() action { (x, c) =>
            c.copy(inputpath = x) } text("path to concept data files")
        )

      cmd("export") action { (_, c) =>
        c.copy(command = "export") } text("Export concept data.") children(
          opt[String]("path") abbr("p") required() action { (x, c) =>
            c.copy(inputpath = x) } text("path to concept data files"),
          opt[String]("data") abbr("d") required() action { (x, c) =>
            c.copy(data = x) } text("data to export ('contexts', 'concepts', or 'graphs')"),
          opt[String]("format") abbr("f") action { (x, c) =>
            c.copy(format = x) } text("export format"),
          opt[Int]("num") abbr("nm") action { (x, c) =>
            c.copy(numberOfExportedItems = x) } text("number of items to export"),
          opt[Double]("mincorr") abbr("mc") action { (x, c) =>
            c.copy(minCorrelation = x) } text("minimum (absolute value) correlation (for graph export)"),
          opt[Double]("minsim") abbr("ms") action { (x, c) =>
            c.copy(minSimilarity = x) } text("minimum similarity (for graph export)"),
          opt[Long]("maxedges") abbr("me") action { (x, c) =>
            c.copy(maxNumEdges = x) } text("maximum number of edges (for graph export)")
        )

      cmd("statistics") action { (_, c) =>
        c.copy(command = "statistics") } text("Calculate graph statistics.") children(
          opt[String]("path") abbr("p") required() action { (x, c) =>
            c.copy(inputpath = x) } text("path to graph files"),
          opt[String]("calculate") abbr("ca") action { (x, c) =>
            c.copy(calculate = x) } text("select calculation (correlations, similarities, all (default))"),
          opt[Int]("numbins") abbr("nb") action { (x, c) =>
            c.copy(numBins = x) } text("number of histogram bins")
        )

      cmd("experiment") action { (_, c) =>
        c.copy(command = "experiment") } text("Perform experiment.") children(
          opt[String]("calculate") abbr("ca") action { (x, c) =>
            c.copy(calculate = x) } text("select experiment (simerrors, classification)"),
          opt[String]("path") abbr("p") required() action { (x, c) =>
            c.copy(inputpath = x) } text("path to concept data files"),
          opt[String]("inputfile") abbr("if") action { (x, c) =>
            c.copy(inputFile = x) } text("path to input file"),
          opt[String]("corrfunc") abbr("cf") action { (x, c) =>
            c.copy(corrfunc = x) } text("correlation function"),
          opt[Long]("mincount") abbr("mc") action { (x, c) =>
            c.copy(minCount = x) } text("minimum example count, default 0"),
          opt[Long]("minconceptcount") abbr("mcp") action { (x, c) =>
            c.copy(minConceptCount = x) } text("minimum concept count, default 0"),
          opt[Long]("mincontextcount") abbr("mcx") action { (x, c) =>
            c.copy(minContextCount = x) } text("minimum context count, default 0"),
          opt[Double]("mincorr") abbr("mc") action { (x, c) =>
            c.copy(minCorrelation = x) } text("minimum (absolute value) correlation"),
          opt[Double]("minsim") abbr("ms") action { (x, c) =>
            c.copy(minSimilarity = x) } text("minimum similarity"),
          opt[Double]("minsimmin") abbr("msn") action { (x, c) =>
            c.copy(minSimilarityMin = x) } text("min similarity min"),
          opt[Double]("minsimmax") abbr("msx") action { (x, c) =>
            c.copy(minSimilarityMax = x) } text("min similarity max"),
          opt[Double]("minsimdelta") abbr("ms") action { (x, c) =>
            c.copy(minSimilarityDelta = x) } text("min similarity delta"),
          opt[Long]("maxdegree") abbr("md") action { (x, c) =>
            c.copy(maxDegree = x) } text("max indegree"),
          opt[Long]("maxdegreemin") abbr("mdn") action { (x, c) =>
            c.copy(maxDegreeMin = x) } text("min max indegree"),
          opt[Long]("maxdegreemax") abbr("mdx") action { (x, c) =>
            c.copy(maxDegreeMax = x) } text("max max indegree"),
          opt[Long]("maxdegreedelta") abbr("mdd") action { (x, c) =>
            c.copy(maxDegreeDelta = x) } text("delta max indegree")  
        )

      cmd("classify") action { (_, c) =>
        c.copy(command = "classify") } text("Train and test classifier.") children(
          opt[String]("path") abbr("p") required() action { (x, c) =>
            c.copy(inputpath = x) } text("path to concept data files")
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
        config.command match {

          case "cluster" => {
            val cd = ConceptData.load(sc, config.inputpath)

            val clusteringAlgo: ClusteringAlgorithm = selectClusteringAlgo(config.clusteringAlgo)

            val clusteringParameters: ParameterMap = new ParameterMap()
              .add(Iterations, config.maxIterations)

            clusterVertices(sc, cd, config.minCorrelation, config.minSimilarity, clusteringAlgo, clusteringParameters,
                            calculateCorrelations = config.calculate == "correlations" || config.calculate == "all",
                            calculateSimilarities = config.calculate == "similarities" || config.calculate == "all")
          }

          case "inspect" => {
            val cd = ConceptData.load(sc, config.inputpath)
            cd.inspect()
          }
          case "export" => {
            val cd = ConceptData.load(sc, config.inputpath)
            config.data match {
              case "contexts" => exportContextsAsText(sc, cd)
              case "concepts" => exportHigherOrderConceptsAsText(sc, cd) //exportHigherOrderConceptsAsTextScalable(sc, cd)
              case "graphs"   => {
                config.format match {
                  case "json" => exportGraphsAsJSON(sc, cd, config.minCorrelation, config.maxNumEdges, config.minSimilarity)
                  case "text" => exportGraphsAsText(sc, cd, config.numberOfExportedItems)
                  case _ => println("No valid format given")
                }
              }
              case _ => println("No valid data type given")
            }
          }
          case "statistics" => {
            val cd = ConceptData.load(sc, config.inputpath)
            config.calculate match {
              case "correlations" => exportGraphStatistics(sc, cd, config.numBins, calculateSimStats = false)
              case "similarities" => exportGraphStatistics(sc, cd, config.numBins, calculateCorrStats = false)
              case "all" => exportGraphStatistics(sc, cd, config.numBins)
              case _ => println("Graph option invalid")
            }
          }
          case "experiment" => {
            val cd = ConceptData.load(sc, config.inputpath)
            config.calculate match {
              case "simerrors" => Experiments.indegreeThresholdVsSimilarityError(sc, cd, config.minCount, findCorrFunc(config.corrfunc))
              case "classification" => Experiments.firstOrderORClassification(sc, cd, config.inputFile, config.inputpath)
              case "maxdegreegraphprops" => Experiments.indegreeThresholdVsGraphProperties(cd, config.inputpath,
                                                                                  findCorrFunc(config.corrfunc), config.corrfunc,
                                                                                  config.minConceptCount, config.minContextCount,
                                                                                  config.minCorrelation, config.minSimilarity,
                                                                                  config.maxDegreeMin, config.maxDegreeMax, config.maxDegreeDelta)
              case "minsimgraphprops" => Experiments.similarityThresholdVsGraphProperties(cd, config.inputpath, findCorrFunc(config.corrfunc),
                                                                                          config.corrfunc, config.minConceptCount, config.minContextCount, 
                                                                                          config.minCorrelation, config.maxDegree, config.minSimilarityMin,
                                                                                          config.minSimilarityMax, config.minSimilarityDelta)
              case "wordsimilarity" => Experiments.wordSimilarityBenchmark(sc, cd, config.inputpath, config.inputFile, findCorrFunc(config.corrfunc), config.corrfunc, 
                                                                           config.minConceptCount, config.minContextCount, config.minCorrelation, 
                                                                           config.maxDegreeMin, config.maxDegreeMax, config.maxDegreeDelta)
              case _ => println("Invalid option")
            }
          }
          case "classify" => {
            val cd = ConceptData.load(sc, config.inputpath)
          }
          case _ => {
            println("No valid command given: Run '" + appName + " --help' to see available commands.")
          }
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
