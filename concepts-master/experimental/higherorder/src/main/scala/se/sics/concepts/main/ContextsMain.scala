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
import se.sics.concepts.main.Actions._
import se.sics.concepts.util.ConceptsUtil.{createSparkContext, writeParameterFile}

import org.apache.log4j.Logger
import org.apache.log4j.Level

//noinspection ScalaUnnecessaryParentheses
object ContextsMain {
  case class Config(local:                  Boolean = true,
                    inputfile:              String = "",
                    inputpath:              String = "",
                    outputpath:             String = "",
                    format:                 String = "",
                    dictionary:             String = "",
                    minCount:               Long = 0,
                    minContextCount:        Long = 0,
                    minRecCount:            Long = 0,
                    windowSize:             Long = 1,
                    recurrent:              Boolean = true,
                    unique:                 Boolean = false,
                    indexedinput:           Boolean = false,
                    numBins:                Int = 1000,
                    maxIterations:          Int = 10,
                    countContexts:          Boolean = false)

  def main(args: Array[String]) {
    // Reduce amount of logging
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

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
        c.copy(local = true)
      } text ("indicate that we are running locally, default false")

      opt[String]("inputfile") abbr ("if") required() action { (x, c) =>
        c.copy(inputfile = x)
      } text ("input text file")

      opt[String]("outputpath") abbr ("op") required() action { (x, c) =>
        c.copy(outputpath = x)
      } text ("output path")

      opt[String]("dictionary") abbr ("dc") action { (x, c) =>
        c.copy(dictionary = x)
      } text ("dictionary file (optional, otherwise derived from data)")

      opt[String]("format") abbr ("f") action { (x, c) =>
        c.copy(format = x)
      } text ("input format: text or ngrams")

      opt[Long]("mincount") abbr ("mc") action { (x, c) =>
        c.copy(minCount = x)
      } text ("minimum example count, default 0")

      opt[Long]("mincontextcount") abbr ("mcc") action { (x, c) =>
        c.copy(minContextCount = x)
      } text ("minimum context count, default 0")

      opt[Long]("minreccount") abbr ("mr") action { (x, c) =>
        c.copy(minRecCount = x)
      } text ("minimum recurrence count, default 0")

      opt[Long]("winsize") abbr ("ws") action { (x, c) =>
        c.copy(windowSize = x)
      } text ("window size, default 1")

      opt[Boolean]("unique") abbr ("uq") action { (x, c) =>
        c.copy(unique = x)
      } text ("indicate that concept occurrences are unique in examples, default false")

      opt[Boolean]("indexedinput") abbr ("ii") action { (x, c) =>
        c.copy(indexedinput = x)
      } text ("indicate that input data is already indexed, default false")

      opt[Boolean]("recurrent") abbr ("rc") action { (x, c) =>
        c.copy(recurrent = x)
      } text ("derive and append recurrent concepts to data, default true")

      opt[Boolean]("countcontexts") abbr ("cc") action { (x, c) =>
        c.copy(countContexts = x)
      } text ("count context occurrences")

    }

    // Give relevant app name
    val appName = "concepts-context"
    val experimentPath = parser.parse(args, Config()) match {
      case Some(config) => config.outputpath
      case _ => throw new IllegalArgumentException("Invalid configuration.")
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
            config.format match {
              case "text" => {
                textFileToContextCounts(
                  sc,
                  config.inputfile,
                  config.outputpath,
                  config.windowSize.toInt,
                  config.minCount)
              }
              case "ngrams" => {
                ngramsFileToContextCounts(sc, ngramFile = config.inputfile,
                  outputPath = config.outputpath,
                  minContextCount = config.minContextCount)
              }
              case _ => throw new IllegalArgumentException("Invalid configuration")
            }
        }

      case _ => throw new IllegalArgumentException("Invalid configuration")
    }

    // Write parameters for experiment to file
    parser.parse(args, Config()) match {
      case Some(config) => {
        import se.sics.concepts.util.Implicits._
        writeParameterFile(config.toStringWithFields, experimentPath, sc, useHadoop = !runLocal)
      }
      case None => {
        //If something went wrong with the config do nothing
        Logger.getLogger("org").error("scopt configuration was wrong")
      }
    }

    sc.stop()
  }



}
