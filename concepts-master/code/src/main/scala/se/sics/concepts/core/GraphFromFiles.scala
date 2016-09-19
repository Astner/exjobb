package se.sics.concepts.core

// # Copy first 10 files from . to ../tweets_sample
// find . -maxdepth 1 -type f |head -10|xargs cp -t ../tweets_sample

import java.io._
import java.util.Calendar

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD


object GraphFromFiles {

  case class Config(inputPath: String = "", experimentName: String = "",
                    localDir: String = "", noCores: Int = 4, maxDegree: Long = Long.MaxValue,
                    vtxRange: (Double, Double) = (1.0E-4, 1.0E-3),
                    edgeRange: (Double, Double) = (0.01, 1.0))

  val argParser = new scopt.OptionParser[Config]("CNCPT") {
    head("Concepts GraphFromFiles", "")

    opt[String]('p', "input-path") valueName("<path-to-graph-files>") required() action { (x, c) =>
      c.copy(inputPath = x)
    } text ("input-path should point to the directory with the input files")

    opt[String]('e', "experiment-name") optional() action { (x, c) =>
      c.copy(experimentName = x)
    } text ("Provide a name for the experiment (optional)")

    opt[String]('l', "local-dir") valueName("<path-to-tmp>")optional() action { (x, c) =>
      c.copy(localDir = x)
    } text ("Provide a path for where Spark should generate temp files (optional)")

    opt[Int]('m', "max-degree") optional() action { (x, c) =>
      c.copy(maxDegree = x)
    } text ("Indicate the maximum number of incoming edges for vertices in the graph (optional)")

    opt[(Double, Double)]("vtx-limits") optional() keyValueName("<min>", "<max>") action {case ((low, high), c) =>
      c.copy(vtxRange = (low, high))
    } text ("Indicate the upper and lower limits for the number of word appearances. Format: \"low=high\" (optional)")

    opt[(Double, Double)]("edge-limits") optional() keyValueName("<min>", "<max>") action {case ((low, high), c) =>
      c.copy(edgeRange = (low, high))
    } text ("Indicate the upper and lower limits for the edge weights. Format: \"low=high\" (optional)")

    // TODO: Indicate the master here
    opt[Int]('c', "no-cores") optional() action { (x, c) =>
      c.copy(noCores = x)
    } text ("Indicate the number of cores to use in the experiment (optional)")

    help("help")
  }


  def main(args: Array[String]) {

    val arguments =  argParser.parse(args, Config())

    // Path is now just the "run" name, i.e. the initial experiment name given
    // when creating the edges, vertices and bigrams files.
    // Example structure: output/tweets-full/tweets-full-edges/[spark-part-files]
    // Here, when creating the correlation edges etc. files with TweetBigramsTest we gave "tweets-full" as
    // experiment name.
    // The experiment name we provide now will create a new folder and put all the new graph files
    // under that path.
    // Example: When running GraphFromFiles with --experiment-name "new-exp" and --input-path "tweets-full"
    // a new folder new-exp will be created under output/tweets-full and all the relevant output will be placed
    // under that folder.
    val path = arguments.get.inputPath
    val experimentNameOpt = arguments.get.experimentName
    val localDir = arguments.get.localDir
    val noCores = arguments.get.noCores
    val maxDegree = arguments.get.maxDegree
    val vtxRange = arguments.get.vtxRange
    val edgeRange = arguments.get.edgeRange

    // (Randomized) name for the experiment, to avoid overwriting older output files
    val experimentName: String = {
      if(experimentNameOpt == "") (System.currentTimeMillis / 1000).toString
      else experimentNameOpt
    }

    // Assuming execution from <repo-root>/code, and output placed in code/output
    // dirPath is "output/<execution-name>"
    val dirPath = new File("output", path)
    // inputPath is "output/<execution-name>/<execution-name>"
    val inputPath = new File(dirPath, path)
    // experimentPath is "output/<execution-name>/<experiment-name>"
    val experimentPath = new File(dirPath, experimentName)

    val conf = new SparkConf()
//          .setMaster("local["+noCores.toString+"]")
      .setAppName("CNCPT GraphFromFiles")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
      .set("spark.kryoserializer.buffer.mb", "128")
      .set("spark.shuffle.consolidateFiles", "true")
//          .set("spark.eventLog.enabled", "true")
//          .set("spark.eventLog.compress", "true")
      //.set("spark.local.dir", localDir)
      //.set("spark.eventLog.dir", localDir + "/spark-events")
      .set("spark.default.parallelism", 300.toString)
      //.set("spark.shuffle.memoryFraction", (0.4).toString)
      .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig

    var start = System.currentTimeMillis / 1000

    val sc = new SparkContext(conf)
    println("Creating graph from files")
    val similarityGraph: CGraph = {
          CGraph(inputPath + "-vertices", inputPath + "-edges", maxDegree, vtxRange, edgeRange, sc)
    }

    // val simEdgesCount = similarityGraph.simEdges.count // TODO: Remove for long-running experiments!
    // println(s"Number of similarity edges: ${simEdgesCount}")
    // var simTime = System.currentTimeMillis / 1000 - start


    // (("vertex i", "vertex j"), "max relative L1 norm", "max error")
//    val relL1WithErrorBound: RDD[((Long, Long), Double, Double)] =  similarityGraph.relativeL1NormWithErrorBound
//    relL1WithErrorBound.saveAsTextFile(new File(experimentPath, "relL1WithErrorBound").toString)
//
//    // (vtx_id, edge_error_sum)
//    var then = System.currentTimeMillis / 1000
//    println(s"Writing discardedEdgeWeightSums to file ${Calendar.getInstance.getTime.toString}")
//    val discardedEdgeWeightSums : RDD[(Long, Double)] = similarityGraph.discardedEdgeWeightSums
//    discardedEdgeWeightSums.saveAsTextFile(new File(experimentPath, "discardedEdgeWeightSums").toString)
//
//    val dewsDuration = (System.currentTimeMillis / 1000) - then
//    println(s"Time to calculate discardedEdgeWeightSums: ${dewsDuration}s")
//
//    // (i, j, sij, ri, rj, di, dj)
//    then = System.currentTimeMillis / 1000
//    println(s"Writing sharedTermsWithSums to file ${Calendar.getInstance.getTime.toString}")
//    val sharedTermsWithSums = similarityGraph.sharedTermsWithSums
//    sharedTermsWithSums.saveAsTextFile(new File(experimentPath, "sharedTermsWithSums").toString)
//    try {
//      sharedTermsHistograms(sharedTermsWithSums, new File(experimentPath, "hist").toString)
//    } catch {
//      case e: Exception => println("Failed while exporting histograms")
//    }


//    val stewsDuration = (System.currentTimeMillis / 1000) - then
//    println(s"Time to calculate sharedTermsWithSums: ${stewsDuration}s")

//    val simEdges = similarityGraph.similarityEdgesWithLabels
//    simEdges.saveAsTextFile(new File(experimentPath, "string-sim-edges").toString)

    val simEdges = similarityGraph.simEdges
    simEdges.saveAsTextFile(new File(experimentPath, "id-sim-edges").toString)

    // val reversedPairs = simEdges.map(pair => (pair._2, pair._1))
    // val simsRanked = reversedPairs.sortByKey(false)
    // val topSims = simsRanked.take(10000)

    // val filename = path + "-simsRanked-" + experimentName

    // TweetBigrams.printToFile(new File(filename))(p => {
    //   topSims.foreach(p.println)
    // })

    var totalTime = System.currentTimeMillis / 1000 - start
    val settingsFile = new PrintWriter(new File(experimentPath, "parameters.txt"))
    // settingsFile.write(s"uniqueWords: ${similarityGraph.vertices.count}\n")
    settingsFile.write(s"vtxWRange: $vtxRange, edgWRange: $edgeRange\n")
    settingsFile.write(s"max-degree: $maxDegree\n")
    // settingsFile.write(s"sim-time: ${simTime}, total-time: ${totalTime}\n")
//    settingsFile.write(s"dewsDuration: ${dewsDuration}, stewsDuration: ${stewsDuration}\n")
    //settingsFile.write(s"similarity-edges: ${simEdgesCount}\n")
    settingsFile.close()

    sc.stop()
  }

  def sharedTermsHistograms(
    sharedTerms: RDD[(Long, Long, Double, Double, Double, Double, Double)],
    filePath: String) {

    // Create dirs fpr output
    // TODO: Use modified version of createOutputDir to better handle failures to create dirs
    val baseDirOK = new File(filePath).mkdirs
    // sijPlot <- histogramAndDensity(ggplot(df, aes(x=Sij)))
    // Sij shouldn't throw, as it does not contain +/- inf or NaN
    var res = sharedTerms.map{case (i, j, sij, ri, rj, di, dj) => sij}.histogram(30)
    histogramToFile(res, new File(filePath, "Sij"))

    // TODO: If this ends up taking too long, I could do all the calculations in one
    // map, but then I would still need one map for each value as the histogram
    // function only works on RDD[Double]. Not sure about the speed gains.

    // keptDiscardedRatioPlot <- histogramAndDensity(ggplot(df, aes(x=(ri+rj)/(di+dj))))
    try {
      res = sharedTerms.map{case (i, j, sij, ri, rj, di, dj) => (ri+rj)/(di+dj)}.histogram(30)
      histogramToFile(res, new File(filePath, "keptDiscardedRatio"))
    } catch {
      case e: Exception => println("Failed at keptDiscardedRatio")
    }

    // totalErrorPlot <- histogramAndDensity(ggplot(df, aes(x=ri+rj+di+dj)))
    try {
      res = sharedTerms.map{case (i, j, sij, ri, rj, di, dj) => ri+rj+di+dj}.histogram(30)
      histogramToFile(res, new File(filePath, "totalErrorPlot"))
    } catch {
      case e: Exception => println("Failed at totalErrorPlot")
    }

    // maxErrorPerSimilarityPlot <- histogramAndDensity(ggplot(df, aes(x=(di+dj)/(ri+rj+di+dj))))
    try {
      res = sharedTerms.map{case (i, j, sij, ri, rj, di, dj) => (di+dj)/(ri+rj+di+dj)}.histogram(30)
      histogramToFile(res, new File(filePath, "maxErrorPerSimilarity"))
    } catch {
      case e: Exception => println("Failed at maxErrorPerSimilarity")
    }

    // relErrorPerSimilarityPlot <- histogramAndDensity(ggplot(df, aes(x=(di+dj)/(Sij+ri+rj+di+dj))))
    try {
      res = sharedTerms.map{case (i, j, sij, ri, rj, di, dj) => (di+dj)/(sij+ri+rj+di+dj)}.histogram(30)
      histogramToFile(res, new File(filePath, "relErrorPerSimilarity"))
    } catch {
      case e: Exception => println("Failed at relErrorPerSimilarity")
    }

    // relL1NormPlot <- histogramAndDensity(ggplot(df, aes(x=(Sij+ri+rj+di+dj)/(ri+rj+di+dj))))
    try {
      res = sharedTerms.map{case (i, j, sij, ri, rj, di, dj) => (sij+ri+rj+di+dj)/(ri+rj+di+dj)}.histogram(30)
      histogramToFile(res, new File(filePath, "relL1Norm"))
    } catch {
      case e: Exception => println("Failed at relL1Norm")
    }

    // minRelL1NormPlot <- histogramAndDensity(ggplot(df, aes(x=(Sij+ri+rj)/(ri+rj+di+dj))))
    try {
      res = sharedTerms.map{case (i, j, sij, ri, rj, di, dj) => (sij+ri+rj)/(ri+rj+di+dj)}.histogram(30)
      histogramToFile(res, new File(filePath, "minRelL1NormPlot"))
    } catch {
      case e: Exception => println("Failed at minRelL1NormPlot")
    }

  }

  def histogramToFile(hist: (Array[Double], Array[Long]), filename: File) = {
    val histFile = new PrintWriter(filename)
    histFile.write(s"${hist._1.mkString(" ")}\n")
    histFile.write(s"${hist._2.mkString(" ")}\n")
    histFile.close()
  }

  // def createOutputDir(experimentName: String) = {
  //   val path: String = "output/" + experimentName
  //   val fpath = new File(path)

  //   if (!fpath.exists()) {
  //     val ok = fpath.mkdirs
  //     if(ok) path + "/" + experimentName
  //     else {
  //       println("Could not create directory, saving to output/")
  //       path
  //     }
  //   }
  //   else path + "/" + experimentName
  // }

}

