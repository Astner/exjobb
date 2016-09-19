package se.sics.concepts.core


import java.io._
import java.util.Calendar

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import se.sics.concepts.core.ConceptIDs._


object Cluster {

  case class Config(inputPath: String = "", experimentName: String = "", minWeight: Double = 1e-4,
                    iterations: Int = 20, capacity: Int = 20, numOverlaps: Int = 20,
                    threshold: Double = 1e-4)

  val argParser = new scopt.OptionParser[Config]("CNCPT") {
    head("Concepts Cluster", "")

    opt[String]('p', "input-path") valueName ("<path-to-graph-files>") required() action { (x, c) =>
      c.copy(inputPath = x)
    } text ("input-path should point to the directory with the input files")

    opt[String]('e', "experiment-name") optional() action { (x, c) =>
      c.copy(experimentName = x)
    } text ("Provide a name for the experiment (optional)")

    opt[Int]('i', "iterations") optional() action { (x, c) =>
      c.copy(iterations = x)
    } text ("Indicate the number of iterations to use in the clustering (optional)")

    opt[Int]('c', "capacity") optional() action { (x, c) =>
      c.copy(capacity = x)
    } text ("Indicate the capacity to use in the clustering (optional)")

    opt[Int]('o', "overlaps") optional() action { (x, c) =>
      c.copy(numOverlaps = x)
    } text ("Indicate the number of overlaps for the clustering (optional)")

    opt[Double]('w', "min-weight") optional() action { (x, c) =>
      c.copy(minWeight = x)
    } text ("Indicate the minimum edge weight in the graph (optional)")

    opt[Double]('t', "threshold") optional() action { (x, c) =>
      c.copy(threshold = x)
    } text ("Indicate the probability threshold after which we assign a vertex to a cluster (optional)")

    help("help")
  }


  def main(args: Array[String]) {

    val arguments = argParser.parse(args, Config())

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
    val iterations = arguments.get.iterations
    val capacity = arguments.get.capacity
    val numOverlaps = arguments.get.numOverlaps
    val minWeight = arguments.get.minWeight
    val threshold = arguments.get.threshold


    // (Randomized) name for the experiment, to avoid overwriting older output files
    val experimentName: String = {
      if (experimentNameOpt == "") (System.currentTimeMillis / 1000).toString
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
          .setAppName("CNCPT Cluster")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", classOf[CKryoRegistrator].getName)
          .set("spark.kryoserializer.buffer.mb", "128")
          .set("spark.shuffle.consolidateFiles", "true")
//          .set("spark.eventLog.enabled", "true")
//          .set("spark.eventLog.compress", "true")
          //.set("spark.local.dir", localDir)
          //.set("spark.eventLog.dir", localDir + "/spark-events")
//          .set("spark.default.parallelism", 300.toString)
          //.set("spark.shuffle.memoryFraction", (0.4).toString)
    .set("spark.io.compression.codec", "lz4") // snappy has problems in zinkbig

    val start = System.currentTimeMillis / 1000

    val sc = new SparkContext(conf)

    // Prepare SLPA
    val slpa = SLPA()
      .setIterations(iterations)
      .setCapacity(capacity)
      .setNumOverlaps(numOverlaps)
      .setUndirect(false)
      .setThreshold(threshold)

    val timestamp = (System.currentTimeMillis() / 1000).toString
    // Read similarity graph from files, filter out edges below threshold
    val idSimEdgesFiltered: RDD[(Long, Long, Double)] = sc.textFile(new File(experimentPath, "id-sim-edges").toString)
      .map(s => s.replaceAll("\\(|\\)", "").split(","))
      .map(s => (s(0).trim.toLong, s(1).trim.toLong, s(2).trim.toDouble)) // (str1, str2, weight)
      .filter{case (s1, s2, w) => w > minWeight}
      .cache().setName("simEdgesFiltered")

    val vertices: RDD[(Long, String)] = CGraph.readVtxFile(sc, new File(inputPath + "-vertices").toString)
      .map(x => (x._1, x._2)) // Get rid of the weights
      .cache().setName("vertices")

    val verticesFiltered = idSimEdgesFiltered
      .map{case (i,j,w)=>(i,j)}
      .flatMap { case (i, j) => Iterable(i, j) }.distinct()
      .cache().setName("verticesFiltered")

    // Save filtered edges and vertices for future experiments
    idSimEdgesFiltered.saveAsTextFile(
      new File(experimentPath, "simEdgesFiltered-" + minWeight + "-" + timestamp).toString)
    verticesFiltered.saveAsTextFile(
      new File(experimentPath, "verticesFiltered-" + minWeight + "-" + timestamp).toString)

    // Perform clusteringIDS
    val clusteringIDs: RDD[(ConceptID, ClusterID)] = slpa.cluster(idSimEdgesFiltered, Some(verticesFiltered))

    // Write clusteringIDS result to disk
    clusteringIDs
      .cache().setName("clusteringIDs")
      .saveAsTextFile(new File(experimentPath, "clusteringIDS-" + timestamp).toString)

    ClusteringAlgorithm.exportClusteredSimilarityGraph(
      idSimEdgesFiltered.map(ssw => ((ssw._1, ssw._2), ssw._3)),
      verticesFiltered,
      vertices,
      clusteringIDs,
      minWeight,
      experimentPath,
      timestamp
    )


    val totalTime = System.currentTimeMillis / 1000 - start
    val settingsFile = new PrintWriter(new File(experimentPath, "clustering-parameters-" + timestamp +  ".txt"))
    settingsFile.write(s"iterations: $iterations, capacity: $capacity\n")
    settingsFile.write(s"minWeight: $minWeight\n")
    settingsFile.write(s"numOverlaps: $numOverlaps\n")
    settingsFile.write(s"totalTime: $totalTime\n")
    //settingsFile.write(s"similarity-edges: ${simEdgesCount}\n")
    settingsFile.close()

    sc.stop
  }
}

